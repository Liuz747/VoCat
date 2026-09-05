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
				enforceDefaultSafeCardPolicy(ctx, regionTestLogger(), database, manager, regionTestDeviceID, snapshot)
				policy, err := database.CardPolicy(ctx, iccid)
				if err != nil || !policy.VoWiFiEnabled || policy.NetworkEnabled || !policy.AirplaneEnabled || policy.Source != "default" {
					t.Fatalf("default policy = %+v, %v", policy, err)
				}
				if legacy && policy.APN != "ims" {
					t.Fatalf("existing APN lost: %+v", policy)
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
