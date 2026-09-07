package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
	"vocat/internal/vowifi/integration"
	"vocat/internal/vowifi/multisim"
	vowifiruntime "vocat/internal/vowifi/runtime"

	"vocat/internal/vowifi"
	"vocat/internal/vowifi/ims"
)

func TestMultiSIMSMSKeepsSubscriptionAndPhysicalInbox(t *testing.T) {
	a := ims.ReceivedSMS{DeviceID: "session-a", ICCID: "iccid-a", IMSI: "imsi-a", Text: "a", MessageID: "message-a"}
	b := ims.ReceivedSMS{DeviceID: "session-b", ICCID: "iccid-b", IMSI: "imsi-b", Text: "b", MessageID: "message-b"}
	for _, original := range []ims.ReceivedSMS{a, b} {
		got := physicalIMSSMS(original, "reader")
		if got.DeviceID != "reader" || got.ICCID != original.ICCID || got.IMSI != original.IMSI || got.Text != original.Text || got.MessageID != original.MessageID {
			t.Fatalf("subscription corrupted during physical inbox projection: %+v", got)
		}
	}
}

func TestCommittedMultiSIMSwitchRequiresExactReadyIdentity(t *testing.T) {
	cause := errors.New("AT+CSIM=10,\"82C0000006\" failed: +CME ERROR: 0")
	for _, tc := range []struct {
		name, iccid string
		ready       bool
		wantError   bool
	}{
		{"committed", "target", true, false},
		{"wrong_profile", "other", true, true},
		{"not_ready", "target", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			calls := 0
			err := verifyMultiSIMSwitch(ctx, cause, "target", time.Millisecond, func(context.Context) (vowifi.SIMIdentity, bool, error) {
				calls++
				return vowifi.SIMIdentity{ICCID: tc.iccid, IMSI: "stable", IMEI: "modem"}, tc.ready, nil
			})
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v", err)
			}
			if err == nil && calls != 2 {
				t.Fatalf("want two stable probes, got %d", calls)
			}
		})
	}
}

func TestMultiSIMSwitchNeverTreatsArbitraryErrorAsCommit(t *testing.T) {
	cause := errors.New("permission denied")
	err := verifyMultiSIMSwitch(context.Background(), cause, "target", time.Millisecond, func(context.Context) (vowifi.SIMIdentity, bool, error) {
		t.Fatal("must not probe after an unrelated error")
		return vowifi.SIMIdentity{}, false, nil
	})
	if !errors.Is(err, cause) {
		t.Fatalf("got %v", err)
	}
}

func TestMultiSIMSwitchRejectsProbeCompletedAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cause := errors.New("AT+CSIM=10,\"82C0000006\" failed: +CME ERROR: 0")
	err := verifyMultiSIMSwitch(ctx, cause, "target", time.Millisecond, func(context.Context) (vowifi.SIMIdentity, bool, error) {
		cancel()
		return vowifi.SIMIdentity{ICCID: "target"}, true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestMultiSIMReservesOneOwnerPerPhysicalReader(t *testing.T) {
	bridge := &multiSIMIntegration{readers: make(map[string]*multiSIMReader)}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for _, id := range []string{"alias-a", "alias-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			reader := &multiSIMReader{backend: &multiSIMBackend{deviceID: id, physicalID: "usb-1-2"}}
			if bridge.reserveReader(id, reader) == nil {
				accepted.Add(1)
			}
		}(id)
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("physical reader acquired by %d groups", accepted.Load())
	}
	if err := bridge.reserveReader("different", &multiSIMReader{backend: &multiSIMBackend{deviceID: "different", physicalID: "usb-1-3"}}); err != nil {
		t.Fatalf("independent reader rejected: %v", err)
	}
}

func TestMultiSIMRejectsEvenIdleAliasesBeforeOwnership(t *testing.T) {
	ctx := context.Background()
	database := newRegionTestStore(t)
	devices := &multiSIMTestAT{entries: []device.Device{{ID: "physical", Discovered: true, Candidate: modem.Candidate{ATPort: modem.Port{Path: "/dev/a"}}}}}
	for _, id := range []string{"primary", "alias"} {
		if err := database.UpsertDevice(ctx, store.Device{ID: id, Name: id, ATPort: "/dev/a"}); err != nil {
			t.Fatal(err)
		}
	}
	bridge := &multiSIMIntegration{database: database, mapper: integration.ATMapper{Store: database, Devices: devices}}
	if err := bridge.rejectReaderAliases(ctx, "primary", "physical"); err == nil {
		t.Fatal("idle alias may access an owned reader")
	}
	if len(devices.calls) != 0 {
		t.Fatal("alias validation touched hardware")
	}
}

type multiSIMTestAT struct {
	entries []device.Device
	calls   []string
}

func (d *multiSIMTestAT) Get(id string) (device.Device, error) {
	for _, v := range d.entries {
		if v.ID == id {
			return v, nil
		}
	}
	return device.Device{}, device.ErrNotFound
}
func (d *multiSIMTestAT) List() []device.Device { return append([]device.Device(nil), d.entries...) }
func (d *multiSIMTestAT) ExecuteAT(_ context.Context, id, command string) (modem.Response, error) {
	d.calls = append(d.calls, id+":"+command)
	return modem.Response{Lines: []string{"+CFUN: 1", "+CGACT: 1,1"}, Final: "OK"}, nil
}
func (d *multiSIMTestAT) ExecuteSensitiveAT(ctx context.Context, id, command string) (modem.Response, error) {
	return d.ExecuteAT(ctx, id, command)
}
func (d *multiSIMTestAT) BeginUICCTransaction(ctx context.Context, _ string) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func TestMultiSIMPinnedExecutorRejectsRemappedReader(t *testing.T) {
	ctx := context.Background()
	database := newRegionTestStore(t)
	cfg := store.Device{ID: "configured", Name: "modem", ATPort: "/dev/a"}
	if err := database.UpsertDevice(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	devices := &multiSIMTestAT{entries: []device.Device{
		{ID: "physical-a", Discovered: true, Candidate: modem.Candidate{ATPort: modem.Port{Path: "/dev/a"}}},
		{ID: "physical-b", Discovered: true, Candidate: modem.Candidate{ATPort: modem.Port{Path: "/dev/b"}}},
	}}
	mapper := integration.ATMapper{Store: database, Devices: devices}
	executor := multiSIMPinnedAT{mapper: mapper, deviceID: cfg.ID, physicalID: "physical-a"}
	transaction, release, err := executor.BeginUICCTransaction(ctx, cfg.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := executor.ExecuteAT(transaction, cfg.ID, "AT"); err != nil {
		t.Fatal(err)
	}
	cfg.ATPort = "/dev/b"
	if err := database.UpsertDevice(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteAT(transaction, cfg.ID, "AT+CSIM"); err == nil {
		t.Fatal("ordinary APDU followed remapped reader")
	}
	if _, err := executor.ExecuteSensitiveAT(transaction, cfg.ID, "AT+AUTH"); err == nil {
		t.Fatal("AKA followed remapped reader")
	}
	if _, _, err := executor.BeginUICCTransaction(ctx, cfg.ID); err == nil {
		t.Fatal("transaction accepted a different physical reader")
	}
	backend := &multiSIMBackend{deviceID: cfg.ID, physicalID: "physical-a", mapper: mapper}
	if _, err := backend.ActiveICCID(ctx); err == nil {
		t.Fatal("active identity accepted remapped hardware")
	}
	if err := backend.SwitchProfile(ctx, multisim.Profile{ICCID: "123", AID: "A000"}); err == nil {
		t.Fatal("profile switch accepted remapped hardware")
	}
	if len(devices.calls) != 1 || devices.calls[0] != "physical-a:AT" {
		t.Fatalf("unexpected hardware operations: %v", devices.calls)
	}
}

func TestMultiSIMRestoreRetriesHandoffWithoutReusingRadioCheckpoint(t *testing.T) {
	ctx := context.Background()
	executor := &multiSIMTestAT{}
	adapter, err := vowifi.NewEC20Adapter(executor, vowifi.EC20AdapterOptions{RestoreCellularData: true})
	if err != nil {
		t.Fatal(err)
	}
	radio, err := adapter.Snapshot(ctx, "configured")
	if err != nil {
		t.Fatal(err)
	}
	singles := vowifiruntime.New(vowifiruntime.Options{})
	if err := singles.Close(ctx); err != nil {
		t.Fatal(err)
	}
	resumed := 0
	reader := &multiSIMReader{backend: &multiSIMBackend{EC20Adapter: adapter, deviceID: "configured", physicalID: "physical"}, radio: radio, radioSaved: true, resumeUSSD: func() { resumed++ }}
	bridge := &multiSIMIntegration{singles: singles, readers: map[string]*multiSIMReader{"configured": reader}}
	cfg := multisim.Config{DeviceID: "configured"}
	if err := bridge.restore(ctx, cfg); !errors.Is(err, vowifiruntime.ErrClosed) {
		t.Fatalf("expected post-radio handoff failure: %v", err)
	}
	if reader.radioSaved {
		t.Fatal("successful radio restore left consumed checkpoint marked reusable")
	}
	if resumed != 0 {
		t.Fatal("USSD resumed while ownership handoff was incomplete")
	}
	operations := len(executor.calls)
	bridge.closing.Store(true)
	if err := bridge.restore(ctx, cfg); err != nil {
		t.Fatalf("handoff retry failed: %v", err)
	}
	if len(executor.calls) != operations {
		t.Fatal("handoff retry repeated radio restore")
	}
	if err := bridge.restore(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if resumed != 1 {
		t.Fatalf("USSD suspension resumed %d times, want exactly once", resumed)
	}
}

func TestMultiSIMAPNUsesOnlySelectedProfileOrIMSDefault(t *testing.T) {
	ctx := context.Background()
	database := newRegionTestStore(t)
	cfg := store.Device{ID: "configured", Name: "modem", APN: "old-profile-private-apn"}
	if err := database.UpsertDevice(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	bridge := &multiSIMIntegration{database: database}
	profile := multisim.Profile{ICCID: "8910000000000000001", AID: "A000"}
	line, err := bridge.lineDeviceConfig(ctx, cfg.ID, profile, "session-id")
	if err != nil {
		t.Fatal(err)
	}
	if line.APN != "ims" || line.ID != "session-id" {
		t.Fatalf("inherited unrelated device identity/APN: %+v", line)
	}
	if err := database.UpsertCardPolicy(ctx, store.CardPolicy{ICCID: profile.ICCID, APN: "profile-ims"}); err != nil {
		t.Fatal(err)
	}
	line, err = bridge.lineDeviceConfig(ctx, cfg.ID, profile, "session-id")
	if err != nil {
		t.Fatal(err)
	}
	if line.APN != "profile-ims" {
		t.Fatalf("selected profile APN lost: %q", line.APN)
	}
}
