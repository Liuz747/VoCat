package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/multisim"
)

const testDownloadICCID = "8900000000000000002"

type nodeDownloadController struct {
	nodeESIMController
	t         *testing.T
	locked    *bool
	info      device.EsimInfo
	listErr   error
	downloads int
	reads     int
}

func (f *nodeDownloadController) ESIMDownloadProfile(_ context.Context, id string, _ device.EsimDownloadParams, _ func(device.EsimProgress)) (*device.EsimDownloadResult, error) {
	if !*f.locked || id != "physical" {
		f.t.Fatal("download did not use owned physical reader")
	}
	f.downloads++
	return &device.EsimDownloadResult{ICCID: testDownloadICCID + "F"}, nil
}
func (f *nodeDownloadController) ESIMListProfiles(_ context.Context, id string) (device.EsimInfo, error) {
	if !*f.locked || id != "physical" {
		f.t.Fatal("readback did not use owned physical reader")
	}
	f.reads++
	return f.info, f.listErr
}
func TestNodeDownloadRequiresReadbackBeforeAddingLine(t *testing.T) {
	for _, mode := range []string{"installed", "read-error", "missing", "unknown-state"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
				t.Fatal(err)
			}
			initial := store.MultiSIMConfig{DeviceID: "record", Enabled: true, Profiles: []store.MultiSIMProfile{{ICCID: "8900000000000000001", AID: "A000"}}}
			if _, err = db.SaveMultiSIMConfig(ctx, initial); err != nil {
				t.Fatal(err)
			}
			locked := false
			transactions := 0
			f := &nodeDownloadController{t: t, locked: &locked, info: device.EsimInfo{AID: "A000", Profiles: []device.EsimProfile{{ICCID: testDownloadICCID, AID: "A001", StateText: "disabled"}}}}
			if mode == "read-error" {
				f.listErr = errors.New("reader failed")
			}
			if mode == "missing" {
				f.info.Profiles = nil
			}
			if mode == "unknown-state" {
				f.info.Profiles[0].State = 7
				f.info.Profiles[0].StateText = ""
			}
			multi := multisim.New(multisim.Options{Factory: func(_ context.Context, _ multisim.Config, p multisim.Profile, id string) (*vowifi.Orchestrator, error) {
				return downloadTestOrchestrator(p, id, downloadTestTunnel{}, downloadTestIMS{})
			}})
			defer multi.Close(context.Background())
			service := &nodeActionService{database: db, multisim: multi, esim: f, readCard: func(ctx context.Context, _ string, op func(context.Context, string) error) error {
				transactions++
				locked = true
				defer func() { locked = false }()
				return op(ctx, "physical")
			}}
			params, _ := json.Marshal(map[string]string{"activation_code": "LPA:1$example.test$test"})
			result, actionErr := service.downloadProfile(ctx, nodemqtt.Command{Target: &nodemqtt.Target{Device: "record", Slot: "123456789012345", BindingVersion: 1}, Params: params}, func(string, *int) {})
			if mode == "read-error" || mode == "missing" {
				if actionErr == nil || actionErr.State != "uncertain" {
					t.Fatalf("unconfirmed write: result=%v error=%+v", result, actionErr)
				}
			} else {
				if actionErr != nil {
					t.Fatalf("download error: %+v", actionErr)
				}
				want := "disabled"
				if mode == "unknown-state" {
					want = "unknown"
				}
				if result.(map[string]any)["profile_state"] != want {
					t.Fatalf("state=%v", result)
				}
			}
			saved, err := db.MultiSIMConfig(ctx, "record")
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 2
			if mode == "read-error" || mode == "missing" {
				wantCount = 1
			}
			if len(saved.Profiles) != wantCount || saved.Profiles[0].ICCID != initial.Profiles[0].ICCID {
				t.Fatalf("download changed wrong group entries: %+v", saved)
			}
			if transactions != 1 || f.downloads != 1 || f.reads != 1 {
				t.Fatalf("transactions=%d downloads=%d reads=%d", transactions, f.downloads, f.reads)
			}
		})
	}
}

func TestNodeExplicitTargetFindsWrittenProfileOutsideGroup(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
		t.Fatal(err)
	}
	locked := false
	f := &nodeDownloadController{t: t, locked: &locked, info: device.EsimInfo{AID: "A000", Profiles: []device.EsimProfile{{ICCID: testDownloadICCID, AID: "A001"}}}}
	service := &nodeActionService{database: db, esim: f, phoneCache: phoneCollection{generatedAt: time.Now()}, readCard: func(ctx context.Context, _ string, op func(context.Context, string) error) error {
		locked = true
		defer func() { locked = false }()
		return op(ctx, "physical")
	}}
	record, _, failure := service.resolveCommandTarget(ctx, nodemqtt.Target{Device: "record", Slot: "123456789012345", ICCID: testDownloadICCID, BindingVersion: 1})
	if failure != nil || record.Target.ICCID != testDownloadICCID || record.AID != "A000" {
		t.Fatalf("record=%+v error=%+v", record, failure)
	}
	if _, err = db.MultiSIMConfig(ctx, "record"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("target lookup modified config: %v", err)
	}
}

func TestNodeEnsureRejectedConfigRollsBack(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
		t.Fatal(err)
	}
	initial := store.MultiSIMConfig{DeviceID: "record", Enabled: true, Profiles: []store.MultiSIMProfile{{ICCID: "8900000000000000001", AID: "A000"}}}
	if _, err = db.SaveMultiSIMConfig(ctx, initial); err != nil {
		t.Fatal(err)
	}
	multi := multisim.New(multisim.Options{}) // No factory: Apply must reject the change.
	defer multi.Close(ctx)
	service := &nodeActionService{database: db, multisim: multi}
	if err = service.ensureProfileConfigured(ctx, "record", device.EsimProfile{ICCID: testDownloadICCID, AID: "A000"}); err == nil {
		t.Fatal("Apply unexpectedly succeeded")
	}
	saved, err := db.MultiSIMConfig(ctx, "record")
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Profiles) != 1 || saved.Profiles[0] != initial.Profiles[0] {
		t.Fatalf("rejected change persisted: %+v", saved)
	}
}

type downloadTestSIM struct{ profile multisim.Profile }

func (s downloadTestSIM) ReadIdentity(context.Context, string) (vowifi.SIMIdentity, error) {
	return vowifi.SIMIdentity{ICCID: s.profile.ICCID, IMSI: "310260000000001", HomeMCC: "310", HomeMNC: "260", HomeCountryCode: "US"}, nil
}

type downloadTestAKA struct{}

func (downloadTestAKA) CheckReady(context.Context, vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	return vowifi.AKAEvidence{Ready: true, Application: "USIM"}, nil
}
func (downloadTestAKA) Authenticate(context.Context, vowifi.SIMIdentity, vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return vowifi.AKAResult{}, nil
}

type downloadTestRadio struct{}

func (downloadTestRadio) Snapshot(context.Context, string) (vowifi.RadioSnapshot, error) {
	return vowifi.RadioSnapshot{}, nil
}
func (downloadTestRadio) StopCellularData(context.Context, string) error              { return nil }
func (downloadTestRadio) EnterVoWiFiRFOff(context.Context, string) error              { return nil }
func (downloadTestRadio) Restore(context.Context, string, vowifi.RadioSnapshot) error { return nil }

type downloadTestProxy struct{}

func (downloadTestProxy) Resolve(context.Context, vowifi.ProxyRequest) (vowifi.ProxyRoute, error) {
	return vowifi.ProxyRoute{Mode: vowifi.ProxyModeDirect}, nil
}

type downloadTestTunnel struct{ fail *atomic.Bool }

func (p downloadTestTunnel) Start(context.Context, vowifi.TunnelRequest) (vowifi.TunnelSession, error) {
	if p.fail != nil && p.fail.Load() {
		return nil, errors.New("temporary failure")
	}
	return downloadTestTunnelSession{}, nil
}

type downloadTestTunnelSession struct{}

func (downloadTestTunnelSession) Evidence() vowifi.TunnelEvidence {
	return vowifi.TunnelEvidence{Established: true, Name: "userspace", ResponderAUTH: vowifi.ResponderAUTHVerified}
}
func (downloadTestTunnelSession) Close(context.Context) error { return nil }

type downloadTestIMS struct{ closeEntered, closeRelease chan struct{} }

func (p downloadTestIMS) Start(context.Context, vowifi.IMSRequest) (vowifi.IMSSession, error) {
	return &downloadTestIMSSession{p: p}, nil
}

type downloadTestIMSSession struct{ p downloadTestIMS }

func (*downloadTestIMSSession) Evidence() vowifi.IMSEvidence {
	return vowifi.IMSEvidence{Registered: true, RegistrationState: "registered"}
}
func (*downloadTestIMSSession) EnableSMS(context.Context) (vowifi.SMSEvidence, error) {
	return vowifi.SMSEvidence{Ready: true}, nil
}
func (s *downloadTestIMSSession) Close(context.Context) error {
	if s.p.closeEntered != nil {
		select {
		case s.p.closeEntered <- struct{}{}:
		default:
		}
	}
	if s.p.closeRelease != nil {
		<-s.p.closeRelease
	}
	return nil
}

type downloadTestPhones struct{}

func (downloadTestPhones) SaveAssociatedNumber(context.Context, vowifi.PhoneRecord) error { return nil }
func downloadTestOrchestrator(p multisim.Profile, id string, tunnel vowifi.TunnelProvider, ims vowifi.IMSProvider) (*vowifi.Orchestrator, error) {
	return vowifi.New(vowifi.Dependencies{SIM: downloadTestSIM{p}, AKA: downloadTestAKA{}, Radio: downloadTestRadio{}, Proxy: downloadTestProxy{}, Tunnel: tunnel, IMS: ims, Phones: downloadTestPhones{}}, vowifi.Options{DeviceID: id})
}

func TestNodeDownloadAutomaticallyStartsAndAddsLines(t *testing.T) {
	for _, mode := range []string{"blank", "running", "disabled", "unready", "retry"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
				t.Fatal(err)
			}
			old := store.MultiSIMProfile{ICCID: "8900000000000000001", AID: "A000"}
			var starts atomic.Int32
			var retried atomic.Bool
			multi := multisim.New(multisim.Options{RetryInitial: 5 * time.Millisecond, RetryMaximum: 10 * time.Millisecond, Factory: func(_ context.Context, _ multisim.Config, p multisim.Profile, id string) (*vowifi.Orchestrator, error) {
				if p.ICCID == old.ICCID {
					starts.Add(1)
				}
				if mode == "unready" || (mode == "retry" && retried.CompareAndSwap(false, true)) {
					return nil, errors.New("reader unavailable")
				}
				return downloadTestOrchestrator(p, id, downloadTestTunnel{}, downloadTestIMS{})
			}})
			defer multi.Close(context.Background())
			if mode == "running" || mode == "disabled" {
				cfg := store.MultiSIMConfig{DeviceID: "record", Enabled: mode == "running", Profiles: []store.MultiSIMProfile{old}}
				if _, err = db.SaveMultiSIMConfig(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				if cfg.Enabled {
					if err = multi.Apply(ctx, runtimeMultiSIMConfigForNode(cfg)); err != nil {
						t.Fatal(err)
					}
					for {
						g := multi.State("record")
						if g.Phase == "running" && len(g.Lines) == 1 && g.Lines[0].State.SMSReady {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("initial line not ready")
						case <-time.After(time.Millisecond):
						}
					}
				}
			}
			locked := false
			f := &nodeDownloadController{t: t, locked: &locked, info: device.EsimInfo{AID: "A000", Profiles: []device.EsimProfile{{ICCID: testDownloadICCID, AID: "A001", StateText: "disabled"}}}}
			service := &nodeActionService{database: db, multisim: multi, esim: f, readCard: func(ctx context.Context, _ string, op func(context.Context, string) error) error {
				locked = true
				defer func() { locked = false }()
				return op(ctx, "physical")
			}}
			if mode == "unready" {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, 100*time.Millisecond)
				defer c()
			}
			params, _ := json.Marshal(map[string]string{"activation_code": "LPA:1$example.test$test"})
			result, failure := service.downloadProfile(ctx, nodemqtt.Command{Target: &nodemqtt.Target{Device: "record", Slot: "123456789012345", BindingVersion: 1}, Params: params}, func(string, *int) {})
			if mode == "unready" {
				if failure == nil || failure.State != "uncertain" {
					t.Fatalf("unready line reported success: result=%v failure=%+v", result, failure)
				}
				r, ok := failure.Result.(map[string]any)
				if !ok || r["written"] != true || r["iccid"] != testDownloadICCID || r["sms_ready"] != false {
					t.Fatalf("write evidence lost: %+v", failure)
				}
			} else {
				if failure != nil {
					t.Fatalf("download failed: %+v", failure)
				}
				r := result.(map[string]any)
				if r["tunnel_state"] != "registered" || r["sms_ready"] != true {
					t.Fatalf("success without SMS readiness: %v", result)
				}
			}
			saved, err := db.MultiSIMConfig(context.Background(), "record")
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if mode == "running" {
				want = 2
			}
			if !saved.Enabled || len(saved.Profiles) != want {
				t.Fatalf("automatic group config=%+v", saved)
			}
			if p := saved.Profiles[len(saved.Profiles)-1]; p.ICCID != testDownloadICCID || p.AID != "A000" {
				t.Fatalf("wrong profile or ISD-R AID: %+v", p)
			}
			if mode == "running" && starts.Load() != 1 {
				t.Fatalf("existing line restarted: %d", starts.Load())
			}
			if f.downloads != 1 || f.reads != 1 {
				t.Fatalf("unexpected card operations: downloads=%d reads=%d", f.downloads, f.reads)
			}
		})
	}
}

func TestNodeFirstDownloadConfigRejectionDoesNotLeaveEnabledGroup(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	multi := multisim.New(multisim.Options{})
	defer multi.Close(ctx)
	service := &nodeActionService{database: db, multisim: multi}
	for _, existing := range []bool{false, true} {
		if existing {
			if _, err = db.SaveMultiSIMConfig(ctx, store.MultiSIMConfig{DeviceID: "record", Enabled: false, Profiles: []store.MultiSIMProfile{{ICCID: "8900000000000000001", AID: "A000"}}}); err != nil {
				t.Fatal(err)
			}
		}
		if err = service.ensureProfileConfigured(ctx, "record", device.EsimProfile{ICCID: testDownloadICCID, AID: "A000"}); err == nil {
			t.Fatal("invalid runtime accepted config")
		}
		got, err := db.MultiSIMConfig(ctx, "record")
		if !existing {
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("failed new group persisted: %+v %v", got, err)
			}
		} else if err != nil || got.Enabled || len(got.Profiles) != 1 || got.Profiles[0].ICCID == testDownloadICCID {
			t.Fatalf("stopped config not restored: %+v %v", got, err)
		}
	}
}
