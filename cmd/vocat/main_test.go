package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
)

// fakeModemClient exercises card policy orchestration without hardware.
type fakeModemClient struct {
	steps []fakeStep
	index int
}

type fakeStep struct {
	command string
	lines   []string
}

func (client *fakeModemClient) Execute(_ context.Context, command string) (modem.Response, error) {
	if client.index >= len(client.steps) {
		return modem.Response{}, fmt.Errorf("unexpected command %q", command)
	}
	step := client.steps[client.index]
	client.index++
	if command != step.command {
		return modem.Response{}, fmt.Errorf("command %q, want %q", command, step.command)
	}
	return modem.Response{Command: command, Lines: step.lines, Final: "OK"}, nil
}

func (client *fakeModemClient) WaitURC(context.Context, func(string) bool) (string, error) {
	return "", errors.New("no URC scripted")
}

func (client *fakeModemClient) Close() error { return nil }

func (client *fakeModemClient) assertExhausted(t *testing.T) {
	t.Helper()
	if client.index != len(client.steps) {
		t.Fatalf("consumed %d of %d scripted commands", client.index, len(client.steps))
	}
}

type fakeDiscoverer struct{ candidates []modem.Candidate }

func (discoverer fakeDiscoverer) Discover(context.Context) ([]modem.Candidate, error) {
	return discoverer.candidates, nil
}

type fakeOpener struct{ client modem.Client }

func (opener fakeOpener) Open(context.Context, modem.Port) (modem.Client, error) {
	return opener.client, nil
}

const regionTestDeviceID = "quectel-region-test"

func newRegionTestManager(t *testing.T, client modem.Client) *device.Manager {
	t.Helper()
	manager, err := device.NewManager(device.Options{
		Discoverer: fakeDiscoverer{candidates: []modem.Candidate{{
			ID:      regionTestDeviceID,
			Product: "EC20",
			ATPort:  modem.Port{Path: "/dev/ttyUSB2", Role: modem.PortRoleAT},
		}}},
		Opener:         fakeOpener{client: client},
		CommandTimeout: time.Second,
		LongTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = manager.Stop(context.Background()) })
	return manager
}

func newRegionTestStore(t *testing.T) *store.Store {
	t.Helper()
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func regionTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDefaultCardPolicySupportsEverySIMRegion(t *testing.T) {
	for _, imsi := range []string{"460001234567890", "461001234567890", "310260123456789"} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/legacy=%t", imsi, legacy), func(t *testing.T) {
				ctx := context.Background()
				database := newRegionTestStore(t)
				iccid := "89860012345678901234"
				if legacy {
					if err := database.UpsertCardPolicy(ctx, store.CardPolicy{ICCID: iccid, AirplaneEnabled: true, IPVersion: "IPV4V6", APN: "ims", Source: "auto_region_block"}); err != nil {
						t.Fatal(err)
					}
				}
				client := &fakeModemClient{steps: []fakeStep{
					{command: "AT+CFUN?", lines: []string{"+CFUN: 1"}},
					{command: "AT+CFUN=4"},
					{command: "AT+CFUN?", lines: []string{"+CFUN: 4"}},
				}}
				manager := newRegionTestManager(t, client)
				snapshot := &device.Snapshot{DeviceID: regionTestDeviceID, SIMReady: true, IMSI: imsi, ICCID: iccid}
				if legacy {
					// An existing policy is never rewritten by the automatic
					// path, whatever its source: it must not flip
					// vowifi_enabled from 0 back to 1 (2026-09-14 rebound).
					client = &fakeModemClient{}
					manager = newRegionTestManager(t, client)
				}
				enforceDefaultSafeCardPolicy(ctx, regionTestLogger(), database, manager, regionTestDeviceID, snapshot)
				policy, err := database.CardPolicy(ctx, iccid)
				if err != nil {
					t.Fatal(err)
				}
				if legacy {
					if policy.VoWiFiEnabled || policy.Source != "auto_region_block" || policy.APN != "ims" {
						t.Fatalf("legacy policy rewritten: %+v", policy)
					}
				} else if !policy.VoWiFiEnabled || policy.NetworkEnabled || !policy.AirplaneEnabled || policy.Source != "default" {
					t.Fatalf("default policy = %+v, %v", policy, err)
				}
				client.assertExhausted(t)
			})
		}
	}
}

func TestDefaultCardPolicyPreservesManualPolicy(t *testing.T) {
	ctx := context.Background()
	database := newRegionTestStore(t)
	iccid := "89860012345678901234"
	original := store.CardPolicy{ICCID: iccid, NetworkEnabled: true, IPVersion: "IPV4V6", APN: "internet", Source: "manual"}
	if err := database.UpsertCardPolicy(ctx, original); err != nil {
		t.Fatal(err)
	}
	client := &fakeModemClient{}
	manager := newRegionTestManager(t, client)
	snapshot := &device.Snapshot{DeviceID: regionTestDeviceID, SIMReady: true, IMSI: "460001234567890", ICCID: iccid}
	enforceDefaultSafeCardPolicy(ctx, regionTestLogger(), database, manager, regionTestDeviceID, snapshot)
	policy, err := database.CardPolicy(ctx, iccid)
	if err != nil || policy.Source != "manual" || !policy.NetworkEnabled || policy.VoWiFiEnabled || policy.AirplaneEnabled || policy.APN != "internet" {
		t.Fatalf("manual policy changed: %+v, %v", policy, err)
	}
	client.assertExhausted(t)
}

func TestProvisionedDeviceTypeRecognizesNativeWWAN(t *testing.T) {
	native := modem.Candidate{
		HardwareKind: "wwan",
		USBPath:      "/sys/devices/pci0000:00/0000:00:00.0/wwan/wwan0",
		QMIControl:   "/dev/wwan0qmi0",
		ATPort:       modem.Port{Path: "/dev/wwan0at0"},
	}
	if got := provisionedDeviceType(native); got != store.DeviceTypeWiFi410 {
		t.Fatalf("native WWAN type = %q, want %q", got, store.DeviceTypeWiFi410)
	}

	usb := modem.Candidate{
		USBPath:    "/sys/bus/usb/devices/1-6",
		QMIControl: "/dev/cdc-wdm0",
		ATPort:     modem.Port{Path: "/dev/ttyUSB2"},
	}
	if got := provisionedDeviceType(usb); got != store.DeviceTypePCIeEC20EC25 {
		t.Fatalf("USB modem type = %q, want %q", got, store.DeviceTypePCIeEC20EC25)
	}
}

func TestDesiredDeviceVoWiFi(t *testing.T) {
	realICCID := "89860012345678901234"
	placeholder := "89111111111111111111"
	cases := []struct {
		name   string
		config store.Device
		policy store.CardPolicy
		iccid  string
		want   bool
		reason string
	}{
		{name: "policy on, untouched device", config: store.Device{ID: "a"}, policy: store.CardPolicy{VoWiFiEnabled: true}, iccid: realICCID, want: true},
		{name: "policy off", config: store.Device{ID: "a"}, policy: store.CardPolicy{VoWiFiEnabled: false}, iccid: realICCID, want: false, reason: "policy_disabled"},
		{name: "user switched this device off", config: store.Device{ID: "a", VoWiFiUserDisabled: true}, policy: store.CardPolicy{VoWiFiEnabled: true}, iccid: realICCID, want: false, reason: "user_disabled"},
		{name: "blank eUICC placeholder identity", config: store.Device{ID: "a"}, policy: store.CardPolicy{VoWiFiEnabled: true}, iccid: placeholder, want: false, reason: "placeholder_iccid"},
		{name: "placeholder wins over policy off", config: store.Device{ID: "a"}, policy: store.CardPolicy{VoWiFiEnabled: false}, iccid: placeholder, want: false, reason: "placeholder_iccid"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, reason := desiredDeviceVoWiFi(test.config, test.policy, test.iccid)
			if got != test.want || reason != test.reason {
				t.Fatalf("desiredDeviceVoWiFi() = %v, %q; want %v, %q", got, reason, test.want, test.reason)
			}
		})
	}
}

// The reconciler mirrors the card policy into the device row. It may switch a
// device off, but it must not switch a user-disabled device back on, and it
// must never ask the runtime to start a line on the blank-eUICC placeholder.
func TestApplyDesiredDeviceVoWiFiNeverReopensUserDisabledDevice(t *testing.T) {
	policy := store.CardPolicy{ICCID: "89860012345678901234", VoWiFiEnabled: true, AirplaneEnabled: true}
	config := store.Device{ID: "a", VoWiFiEnabled: false, VoWiFiUserDisabled: true}
	next, changed := applyDesiredDeviceVoWiFi(config, policy, false)
	if changed || next.VoWiFiEnabled {
		t.Fatalf("user-disabled device changed: %+v (changed=%v)", next, changed)
	}
	config = store.Device{ID: "a", VoWiFiEnabled: true, VoWiFiUserDisabled: true}
	next, changed = applyDesiredDeviceVoWiFi(config, policy, false)
	if !changed || next.VoWiFiEnabled {
		t.Fatalf("stale enabled row not pulled down: %+v (changed=%v)", next, changed)
	}
	config = store.Device{ID: "a", VoWiFiEnabled: false, NetworkEnabled: true}
	next, changed = applyDesiredDeviceVoWiFi(config, policy, true)
	if !changed || !next.VoWiFiEnabled || next.NetworkEnabled {
		t.Fatalf("policy-enabled device not applied: %+v (changed=%v)", next, changed)
	}
	config = store.Device{ID: "a", VoWiFiEnabled: true}
	next, changed = applyDesiredDeviceVoWiFi(config, policy, true)
	if changed || !next.VoWiFiEnabled {
		t.Fatalf("already-enabled device rewritten: %+v (changed=%v)", next, changed)
	}
}
