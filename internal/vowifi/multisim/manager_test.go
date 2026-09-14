package multisim

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDisableCancelsStartupButRetainsOwnershipThroughRestore(t *testing.T) {
	entered, restoring, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m := New(Options{
		Prepare: func(ctx context.Context, _ Config) error { close(entered); <-ctx.Done(); return ctx.Err() },
		Restore: func(ctx context.Context, _ Config) error {
			close(restoring)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Factory: func(context.Context, Config, Profile, string) (*vowifi.Orchestrator, error) {
			t.Error("factory ran after canceled preparation")
			return nil, errors.New("unexpected")
		},
	})
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	cfg := testConfig()
	request, cancel := context.WithCancel(context.Background())
	if err := m.Apply(request, cfg); err != nil {
		t.Fatal(err)
	}
	cancel()
	if !m.Owns(cfg.DeviceID) {
		t.Fatal("ownership not acquired before prepare")
	}
	<-entered
	if err := m.Apply(context.Background(), cfg); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("busy change accepted: %v", err)
	}
	cfg.Enabled = false
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	<-restoring
	if !m.Owns(cfg.DeviceID) {
		t.Fatal("ownership released before restoration")
	}
	close(release)
	waitFor(t, func() bool { return !m.Owns(cfg.DeviceID) })
	if state := m.State(cfg.DeviceID); state.Enabled || state.Busy || state.Phase != "idle" {
		t.Fatalf("incorrect stopped state: %+v", state)
	}
}

func TestPrepareFailureRestoresAndRestoreFailureRetainsOwnershipForRetry(t *testing.T) {
	var restores atomic.Int32
	m := New(Options{
		Prepare: func(context.Context, Config) error { return errors.New("prepare failed") },
		Restore: func(context.Context, Config) error {
			if restores.Add(1) == 1 {
				return errors.New("restore failed")
			}
			return nil
		},
		Factory: func(context.Context, Config, Profile, string) (*vowifi.Orchestrator, error) {
			t.Error("factory called after preparation failure")
			return nil, nil
		},
	})
	cfg := testConfig()
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.State(cfg.DeviceID).Phase == "restore_failed" })
	if !m.Owns(cfg.DeviceID) {
		t.Fatal("failed restoration released the reader")
	}
	cfg.Enabled = false
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.Owns(cfg.DeviceID) })
	if restores.Load() != 2 {
		t.Fatalf("restoration retry count %d", restores.Load())
	}
	_ = m.Close(context.Background())
}

type lifecycleSIM struct{ profile Profile }

func (s lifecycleSIM) ReadIdentity(context.Context, string) (vowifi.SIMIdentity, error) {
	return vowifi.SIMIdentity{ICCID: s.profile.ICCID, IMSI: "310260000000001", HomeMCC: "310", HomeMNC: "260", HomeCountryCode: "US"}, nil
}

type lifecycleAKA struct{}

func (lifecycleAKA) CheckReady(context.Context, vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	return vowifi.AKAEvidence{Ready: true, Application: "USIM"}, nil
}
func (lifecycleAKA) Authenticate(context.Context, vowifi.SIMIdentity, vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return vowifi.AKAResult{}, nil
}

type lifecycleRadio struct{}

func (lifecycleRadio) Snapshot(context.Context, string) (vowifi.RadioSnapshot, error) {
	return vowifi.RadioSnapshot{}, nil
}
func (lifecycleRadio) StopCellularData(context.Context, string) error              { return nil }
func (lifecycleRadio) EnterVoWiFiRFOff(context.Context, string) error              { return nil }
func (lifecycleRadio) Restore(context.Context, string, vowifi.RadioSnapshot) error { return nil }

type lifecycleProxy struct{}

func (lifecycleProxy) Resolve(context.Context, vowifi.ProxyRequest) (vowifi.ProxyRoute, error) {
	return vowifi.ProxyRoute{Mode: vowifi.ProxyModeDirect}, nil
}

type lifecycleTunnel struct{ fail *atomic.Bool }

func (p lifecycleTunnel) Start(context.Context, vowifi.TunnelRequest) (vowifi.TunnelSession, error) {
	if p.fail != nil && p.fail.Load() {
		return nil, errors.New("temporary failure")
	}
	return lifecycleTunnelSession{}, nil
}

type lifecycleTunnelSession struct{}

func (lifecycleTunnelSession) Evidence() vowifi.TunnelEvidence {
	return vowifi.TunnelEvidence{Established: true, Name: "userspace", ResponderAUTH: vowifi.ResponderAUTHVerified}
}
func (lifecycleTunnelSession) Close(context.Context) error { return nil }

type lifecycleIMS struct{ closeEntered, closeRelease chan struct{} }

func (p lifecycleIMS) Start(context.Context, vowifi.IMSRequest) (vowifi.IMSSession, error) {
	return &lifecycleIMSSession{p: p}, nil
}

type lifecycleIMSSession struct{ p lifecycleIMS }

func (*lifecycleIMSSession) Evidence() vowifi.IMSEvidence {
	return vowifi.IMSEvidence{Registered: true, RegistrationState: "registered"}
}
func (*lifecycleIMSSession) EnableSMS(context.Context) (vowifi.SMSEvidence, error) {
	return vowifi.SMSEvidence{Ready: true}, nil
}
func (s *lifecycleIMSSession) Close(context.Context) error {
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

type lifecyclePhones struct{}

func (lifecyclePhones) SaveAssociatedNumber(context.Context, vowifi.PhoneRecord) error { return nil }
func lifecycleOrchestrator(p Profile, id string, tunnel vowifi.TunnelProvider, ims vowifi.IMSProvider) (*vowifi.Orchestrator, error) {
	return vowifi.New(vowifi.Dependencies{SIM: lifecycleSIM{p}, AKA: lifecycleAKA{}, Radio: lifecycleRadio{}, Proxy: lifecycleProxy{}, Tunnel: tunnel, IMS: ims, Phones: lifecyclePhones{}}, vowifi.Options{DeviceID: id})
}

func TestLinesRetryIndependentlyAndStatusDoesNotReadCard(t *testing.T) {
	cfg := testConfig()
	var fail atomic.Bool
	fail.Store(true)
	var factories atomic.Int32
	m := New(Options{RetryInitial: 5 * time.Millisecond, RetryMaximum: 10 * time.Millisecond, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		factories.Add(1)
		var flag *atomic.Bool
		if p.ICCID == cfg.Profiles[0].ICCID {
			flag = &fail
		}
		return lifecycleOrchestrator(p, id, lifecycleTunnel{flag}, lifecycleIMS{})
	}})
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return len(s.Lines) == 2 && s.Lines[0].State.Phase == vowifi.PhaseFailed && s.Lines[1].State.SMSReady
	})
	if !m.Owns(cfg.DeviceID) {
		t.Fatal("one failed line released group")
	}
	fail.Store(false)
	waitFor(t, func() bool { s := m.State(cfg.DeviceID); return s.Lines[0].State.SMSReady && s.Lines[1].State.SMSReady })
	for i := 0; i < 20; i++ {
		_ = m.State("unknown")
		_ = m.State(cfg.DeviceID)
	}
	if factories.Load() != 2 {
		t.Fatal("status read created hardware runtimes")
	}
}

func TestCloseTimeoutNeverRestoresWhileLineIsStillClosing(t *testing.T) {
	cfg := testConfig()
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var restored atomic.Bool
	m := New(Options{CleanupTimeout: 20 * time.Millisecond, Restore: func(context.Context, Config) error { restored.Store(true); return nil }, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, lifecycleIMS{entered, release})
	}})
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return len(s.Lines) == 2 && s.Lines[0].State.SMSReady && s.Lines[1].State.SMSReady
	})
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	err := m.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not report deadline: %v", err)
	}
	if restored.Load() || !m.Owns(cfg.DeviceID) {
		t.Fatal("reader restored/released while session cleanup pending")
	}
	close(release)
	waitFor(t, func() bool { return restored.Load() && !m.Owns(cfg.DeviceID) })
}

func TestFactoryFailureRetriesWithoutHoldingUpSibling(t *testing.T) {
	cfg := testConfig()
	var fail atomic.Bool
	fail.Store(true)
	m := New(Options{RetryInitial: 5 * time.Millisecond, RetryMaximum: 10 * time.Millisecond, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		if p.ICCID == cfg.Profiles[0].ICCID && fail.Load() {
			return nil, errors.New("temporarily unavailable")
		}
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, lifecycleIMS{})
	}})
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return len(s.Lines) == 2 && s.Lines[0].State.Phase == vowifi.PhaseFailed && s.Lines[1].State.SMSReady
	})
	fail.Store(false)
	waitFor(t, func() bool { return m.State(cfg.DeviceID).Lines[0].State.SMSReady })
}

func TestBlockedFactoryCannotPreventSiblingOrReleaseReaderOnStop(t *testing.T) {
	cfg := testConfig()
	entered, release := make(chan struct{}), make(chan struct{})
	var restored atomic.Bool
	m := New(Options{CleanupTimeout: 20 * time.Millisecond, Restore: func(context.Context, Config) error { restored.Store(true); return nil }, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		if p.ICCID == cfg.Profiles[0].ICCID {
			close(entered)
			<-release
		}
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, lifecycleIMS{})
	}})
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	<-entered
	waitFor(t, func() bool { s := m.State(cfg.DeviceID); return len(s.Lines) == 2 && s.Lines[1].State.SMSReady })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close ignored unfinished factory: %v", err)
	}
	if restored.Load() || !m.Owns(cfg.DeviceID) {
		t.Fatal("reader released while factory can still return resources")
	}
	close(release)
	waitFor(t, func() bool { return restored.Load() && !m.Owns(cfg.DeviceID) })
}

type refreshIMS struct{ entered, release chan struct{} }

func (p refreshIMS) Start(context.Context, vowifi.IMSRequest) (vowifi.IMSSession, error) {
	return &refreshIMSSession{lifecycleIMSSession: lifecycleIMSSession{}, p: p}, nil
}

type refreshIMSSession struct {
	lifecycleIMSSession
	p refreshIMS
}

func (s *refreshIMSSession) RefreshRegistration(context.Context) error {
	s.p.entered <- struct{}{}
	<-s.p.release
	return nil
}

func TestRefreshUsesExistingLineAndBlocksDuplicateUntilFinished(t *testing.T) {
	cfg := testConfig()
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var factories atomic.Int32
	var restored atomic.Bool
	m := New(Options{CleanupTimeout: 15 * time.Millisecond, Restore: func(context.Context, Config) error { restored.Store(true); return nil }, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		factories.Add(1)
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, refreshIMS{entered, release})
	}})
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return len(s.Lines) == 2 && s.Lines[0].State.SMSReady && s.Lines[1].State.SMSReady
	})
	if err := m.Refresh(cfg.DeviceID, cfg.Profiles[0].ICCID); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := m.Refresh(cfg.DeviceID, cfg.Profiles[0].ICCID); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("duplicate refresh accepted: %v", err)
	}
	if factories.Load() != 2 || !m.State(cfg.DeviceID).Lines[1].State.SMSReady {
		t.Fatal("refresh rebuilt a session or interrupted its sibling")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not wait for in-flight refresh: %v", err)
	}
	if restored.Load() || !m.Owns(cfg.DeviceID) {
		t.Fatal("restoration raced with late refresh")
	}
	close(release)
	waitFor(t, func() bool { return !m.Owns(cfg.DeviceID) })
}

func TestGroupWaitsForRealProviderAfterOrchestratorCleanupDeadline(t *testing.T) {
	cfg := testConfig()
	release := make(chan struct{})
	var restored atomic.Bool
	m := New(Options{CleanupTimeout: 20 * time.Millisecond, Restore: func(context.Context, Config) error { restored.Store(true); return nil }, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		return vowifi.New(vowifi.Dependencies{SIM: lifecycleSIM{p}, AKA: lifecycleAKA{}, Radio: lifecycleRadio{}, Proxy: lifecycleProxy{}, Tunnel: lifecycleTunnel{}, IMS: lifecycleIMS{closeRelease: release}, Phones: lifecyclePhones{}}, vowifi.Options{DeviceID: id, CleanupTimeout: 20 * time.Millisecond})
	}})
	if err := m.Apply(context.Background(), cfg); err != nil {
		close(release)
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return len(s.Lines) == 2 && s.Lines[0].State.SMSReady && s.Lines[1].State.SMSReady
	})
	cfg.Enabled = false
	if err := m.Apply(context.Background(), cfg); err != nil {
		close(release)
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return s.Lines[0].State.Phase == vowifi.PhaseIdle && s.Lines[1].State.Phase == vowifi.PhaseIdle
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := m.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || restored.Load() || !m.Owns(cfg.DeviceID) {
		close(release)
		t.Fatalf("idle phase/cleanup warning released real provider: err=%v restored=%v owned=%v", err, restored.Load(), m.Owns(cfg.DeviceID))
	}
	close(release)
	waitFor(t, func() bool { return restored.Load() && !m.Owns(cfg.DeviceID) })
}

type sendIMS struct{ requests chan sendIMSRequest }

type sendIMSRequest struct {
	session string
	request vowifi.SMSSubmitRequest
}

func (p sendIMS) Start(_ context.Context, request vowifi.IMSRequest) (vowifi.IMSSession, error) {
	return &sendIMSSession{session: request.DeviceID, p: p}, nil
}

type sendIMSSession struct {
	lifecycleIMSSession
	session string
	p       sendIMS
}

func (s *sendIMSSession) SendSMS(_ context.Context, request vowifi.SMSSubmitRequest) (vowifi.SMSSubmitResult, error) {
	s.p.requests <- sendIMSRequest{session: s.session, request: request}
	return vowifi.SMSSubmitResult{To: request.Recipient, PartsTotal: 1, PartsAttempted: 1, PartsAccepted: 1, AllPartsAccepted: true, SubmissionStatus: "accepted_by_ims"}, nil
}

func TestSendSMSSelectsLineByICCIDOrSessionAndRequiresSelectorForGroups(t *testing.T) {
	cfg := testConfig()
	requests := make(chan sendIMSRequest, 4)
	m := New(Options{CleanupTimeout: 15 * time.Millisecond, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, sendIMS{requests})
	}})
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return s.Phase == "running" && len(s.Lines) == 2 && s.Lines[0].State.SMSReady && s.Lines[1].State.SMSReady
	})
	second := LineID(cfg.DeviceID, cfg.Profiles[1].ICCID)
	if _, _, err := m.SendSMS(context.Background(), cfg.DeviceID, "", vowifi.SMSSubmitRequest{Recipient: "+12025550100", Text: "hi"}); !errors.Is(err, ErrLineRequired) {
		t.Fatalf("group send without selector = %v, want ErrLineRequired", err)
	}
	if _, _, err := m.SendSMS(context.Background(), cfg.DeviceID, "8910000000000000009", vowifi.SMSSubmitRequest{Recipient: "+12025550100", Text: "hi"}); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown line = %v, want ErrNotRegistered", err)
	}
	if _, _, err := m.SendSMS(context.Background(), "other-reader", cfg.Profiles[0].ICCID, vowifi.SMSSubmitRequest{Recipient: "+12025550100", Text: "hi"}); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown group = %v, want ErrNotRegistered", err)
	}
	result, identity, err := m.SendSMS(context.Background(), cfg.DeviceID, cfg.Profiles[1].ICCID, vowifi.SMSSubmitRequest{Recipient: "+12025550100", Text: "by iccid"})
	if err != nil || !result.AllPartsAccepted {
		t.Fatalf("send by ICCID = %+v, %v", result, err)
	}
	if identity.DeviceID != cfg.DeviceID || identity.SessionID != second || identity.ICCID != cfg.Profiles[1].ICCID {
		t.Fatalf("identity = %+v", identity)
	}
	got := <-requests
	if got.session != second || got.request.Text != "by iccid" {
		t.Fatalf("request reached %q with %+v", got.session, got.request)
	}
	if _, identity, err := m.SendSMS(context.Background(), cfg.DeviceID, second, vowifi.SMSSubmitRequest{Recipient: "+12025550100", Text: "by session"}); err != nil || identity.SessionID != second {
		t.Fatalf("send by session = %+v, %v", identity, err)
	}
	if got := <-requests; got.session != second || got.request.Text != "by session" {
		t.Fatalf("request reached %q with %+v", got.session, got.request)
	}
	if len(requests) != 0 {
		t.Fatal("rejected sends still reached a line")
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// countingIMS records how many IMS sessions were closed, per profile ICCID, so
// a test can prove that reconfiguring a running group touched only the lines
// that actually changed.
type countingIMS struct {
	mu     *sync.Mutex
	closes map[string]int
	iccid  string
}

func (p countingIMS) Start(context.Context, vowifi.IMSRequest) (vowifi.IMSSession, error) {
	return &countingIMSSession{p: p}, nil
}

type countingIMSSession struct{ p countingIMS }

func (*countingIMSSession) Evidence() vowifi.IMSEvidence {
	return vowifi.IMSEvidence{Registered: true, RegistrationState: "registered"}
}
func (*countingIMSSession) EnableSMS(context.Context) (vowifi.SMSEvidence, error) {
	return vowifi.SMSEvidence{Ready: true}, nil
}
func (s *countingIMSSession) Close(context.Context) error {
	s.p.mu.Lock()
	s.p.closes[s.p.iccid]++
	s.p.mu.Unlock()
	return nil
}

type incrementalFixture struct {
	m         *Manager
	prepares  *atomic.Int32
	restores  *atomic.Int32
	factories *atomic.Int32
	closes    map[string]int
	closesMu  *sync.Mutex
}

func newIncrementalFixture(t *testing.T, verify func(context.Context, Config, []Profile) error) incrementalFixture {
	t.Helper()
	f := incrementalFixture{prepares: &atomic.Int32{}, restores: &atomic.Int32{}, factories: &atomic.Int32{}, closes: map[string]int{}, closesMu: &sync.Mutex{}}
	f.m = New(Options{
		RetryInitial: 5 * time.Millisecond, RetryMaximum: 10 * time.Millisecond,
		Prepare: func(context.Context, Config) error { f.prepares.Add(1); return nil },
		Restore: func(context.Context, Config) error { f.restores.Add(1); return nil },
		Verify:  verify,
		Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
			f.factories.Add(1)
			return lifecycleOrchestrator(p, id, lifecycleTunnel{}, countingIMS{mu: f.closesMu, closes: f.closes, iccid: p.ICCID})
		},
	})
	t.Cleanup(func() { _ = f.m.Close(context.Background()) })
	return f
}

func (f incrementalFixture) closed(iccid string) int {
	f.closesMu.Lock()
	defer f.closesMu.Unlock()
	return f.closes[iccid]
}

func allReady(s GroupState, n int) bool {
	if len(s.Lines) != n {
		return false
	}
	for _, line := range s.Lines {
		if !line.State.SMSReady {
			return false
		}
	}
	return true
}

func TestApplyAddsLineToRunningGroupWithoutRestartingSiblings(t *testing.T) {
	f := newIncrementalFixture(t, nil)
	cfg := testConfig()
	if err := f.m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 2) })

	grown := testConfig()
	grown.Profiles[0].Name = "renamed"
	grown.Profiles = append(grown.Profiles, Profile{ICCID: "8910000000000000003", AID: "A000000003", Name: "third"})
	if err := f.m.Apply(context.Background(), grown); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 3) })

	s := f.m.State(cfg.DeviceID)
	if s.Phase != "running" || s.Busy {
		t.Fatalf("group phase %q busy=%v after incremental add", s.Phase, s.Busy)
	}
	if s.Lines[0].Name != "renamed" || s.Lines[2].ICCID != "8910000000000000003" {
		t.Fatalf("lines after add: %+v", s.Lines)
	}
	if f.prepares.Load() != 1 || f.restores.Load() != 0 {
		t.Fatalf("incremental add restarted the group: prepares=%d restores=%d", f.prepares.Load(), f.restores.Load())
	}
	if f.factories.Load() != 3 {
		t.Fatalf("factories = %d, want 3 (siblings must not be rebuilt)", f.factories.Load())
	}
	if f.closed(cfg.Profiles[0].ICCID) != 0 || f.closed(cfg.Profiles[1].ICCID) != 0 {
		t.Fatal("a sibling IMS session was closed by an incremental add")
	}
}

func TestApplyRemovesOnlyTheDroppedLineAndAllowsReAdd(t *testing.T) {
	f := newIncrementalFixture(t, nil)
	cfg := testConfig()
	if err := f.m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 2) })

	dropped := cfg.Profiles[1].ICCID
	shrunk := testConfig()
	shrunk.Profiles = shrunk.Profiles[:1]
	if err := f.m.Apply(context.Background(), shrunk); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.closed(dropped) == 1 })
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 1) })
	if f.closed(cfg.Profiles[0].ICCID) != 0 {
		t.Fatal("the surviving line was closed")
	}
	if err := f.m.Reconnect(cfg.DeviceID, dropped); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("removed line still addressable: %v", err)
	}
	if _, _, err := f.m.SendSMS(context.Background(), cfg.DeviceID, dropped, vowifi.SMSSubmitRequest{}); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("removed line still routable for SMS: %v", err)
	}
	if f.prepares.Load() != 1 || f.restores.Load() != 0 || !f.m.Owns(cfg.DeviceID) {
		t.Fatalf("incremental remove restarted or released the group: prepares=%d restores=%d owned=%v", f.prepares.Load(), f.restores.Load(), f.m.Owns(cfg.DeviceID))
	}

	// Re-adding the same ICCID must work once its old session is gone.
	if err := f.m.Apply(context.Background(), testConfig()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 2) })
	if f.factories.Load() != 3 {
		t.Fatalf("factories = %d, want 3", f.factories.Load())
	}

	// A full stop after incremental edits still releases the reader cleanly.
	stop := testConfig()
	stop.Enabled = false
	if err := f.m.Apply(context.Background(), stop); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !f.m.Owns(cfg.DeviceID) })
	if f.restores.Load() != 1 {
		t.Fatalf("restores = %d after stop", f.restores.Load())
	}
}

func TestApplyRejectsAddedProfileThatVerifyRefuses(t *testing.T) {
	refused := errors.New("multisim: selected profile is not present on this reader")
	var verified [][]Profile
	f := newIncrementalFixture(t, func(_ context.Context, _ Config, added []Profile) error {
		verified = append(verified, added)
		for _, p := range added {
			if p.ICCID == "8910000000000000009" {
				return refused
			}
		}
		return nil
	})
	cfg := testConfig()
	if err := f.m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return allReady(f.m.State(cfg.DeviceID), 2) })
	if len(verified) != 0 {
		t.Fatal("Verify ran for the initial start; Prepare owns that check")
	}

	bad := testConfig()
	bad.Profiles = append(bad.Profiles, Profile{ICCID: "8910000000000000009", AID: "A000000009"})
	if err := f.m.Apply(context.Background(), bad); !errors.Is(err, refused) {
		t.Fatalf("Apply error = %v, want the Verify refusal", err)
	}
	if len(verified) != 1 || len(verified[0]) != 1 || verified[0][0].ICCID != "8910000000000000009" {
		t.Fatalf("Verify saw %+v, want only the added profile", verified)
	}
	s := f.m.State(cfg.DeviceID)
	if !allReady(s, 2) || s.Phase != "running" || s.Busy {
		t.Fatalf("refused add disturbed the group: %+v", s)
	}
	if f.factories.Load() != 2 || f.prepares.Load() != 1 {
		t.Fatalf("refused add touched hardware: factories=%d prepares=%d", f.factories.Load(), f.prepares.Load())
	}
}
