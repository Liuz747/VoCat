package multisim

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

// callIMS is an IMS provider whose sessions record call signalling in memory
// so the manager's line selection can be observed per session.
type callIMS struct {
	mu       sync.Mutex
	sessions map[string]*callIMSSession
}

func newCallIMS() *callIMS { return &callIMS{sessions: make(map[string]*callIMSSession)} }

func (p *callIMS) Start(_ context.Context, request vowifi.IMSRequest) (vowifi.IMSSession, error) {
	session := &callIMSSession{}
	p.mu.Lock()
	p.sessions[request.DeviceID] = session
	p.mu.Unlock()
	return session, nil
}

func (p *callIMS) session(id string) *callIMSSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessions[id]
}

type callIMSSession struct {
	lifecycleIMSSession
	mu    sync.Mutex
	calls []vowifi.Call
}

func (s *callIMSSession) Calls() []vowifi.Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]vowifi.Call(nil), s.calls...)
}

func (s *callIMSSession) DialCall(_ context.Context, number string) (vowifi.Call, error) {
	call := vowifi.Call{ID: "call-" + number, Number: number, Direction: "outgoing", State: "dialing", StartedAt: time.Now().UTC()}
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
	return call, nil
}

func (s *callIMSSession) plantIncoming(id, number string) {
	s.mu.Lock()
	s.calls = append(s.calls, vowifi.Call{ID: id, Number: number, Direction: "incoming", State: "ringing", StartedAt: time.Now().UTC()})
	s.mu.Unlock()
}

func (s *callIMSSession) AnswerCall(_ context.Context, id string) (vowifi.Call, error) {
	return s.setState(id, "active")
}

func (s *callIMSSession) HangupCall(_ context.Context, id string) error {
	_, err := s.setState(id, "ended")
	return err
}

func (s *callIMSSession) setState(id, state string) (vowifi.Call, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := range s.calls {
		if s.calls[index].ID == id {
			s.calls[index].State = state
			return s.calls[index], nil
		}
	}
	return vowifi.Call{}, errors.New("ims: unknown call")
}

func newCallManager(t *testing.T) (*Manager, *callIMS, Config) {
	t.Helper()
	cfg := testConfig()
	ims := newCallIMS()
	m := New(Options{CleanupTimeout: 15 * time.Millisecond, Factory: func(_ context.Context, _ Config, p Profile, id string) (*vowifi.Orchestrator, error) {
		return lifecycleOrchestrator(p, id, lifecycleTunnel{}, ims)
	}})
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	if err := m.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := m.State(cfg.DeviceID)
		return s.Phase == "running" && len(s.Lines) == 2 && s.Lines[0].State.IMSReady && s.Lines[1].State.IMSReady
	})
	return m, ims, cfg
}

func TestDialCallSelectsLineLikeSMS(t *testing.T) {
	m, ims, cfg := newCallManager(t)
	ctx := context.Background()
	if _, _, err := m.DialCall(ctx, cfg.DeviceID, "", "+12025550100"); !errors.Is(err, ErrLineRequired) {
		t.Fatalf("dial without selector = %v, want ErrLineRequired", err)
	}
	if _, _, err := m.DialCall(ctx, cfg.DeviceID, "8910000000000000009", "+12025550100"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown line = %v, want ErrNotRegistered", err)
	}
	if _, _, err := m.DialCall(ctx, "other-reader", cfg.Profiles[0].ICCID, "+12025550100"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown group = %v, want ErrNotRegistered", err)
	}
	second := LineID(cfg.DeviceID, cfg.Profiles[1].ICCID)
	call, identity, err := m.DialCall(ctx, cfg.DeviceID, cfg.Profiles[1].ICCID, "+12025550100")
	if err != nil || call.State != "dialing" || call.Number != "+12025550100" {
		t.Fatalf("dial by ICCID = %+v, %v", call, err)
	}
	if identity.DeviceID != cfg.DeviceID || identity.SessionID != second || identity.ICCID != cfg.Profiles[1].ICCID {
		t.Fatalf("identity = %+v", identity)
	}
	if session := ims.session(second); session == nil || len(session.Calls()) != 1 {
		t.Fatal("call did not reach the selected line's IMS session")
	}
	if first := ims.session(LineID(cfg.DeviceID, cfg.Profiles[0].ICCID)); first == nil || len(first.Calls()) != 0 {
		t.Fatal("call leaked into the other line")
	}
	if _, _, err := m.DialCall(ctx, cfg.DeviceID, second, "+12025550101"); err != nil {
		t.Fatalf("dial by session id = %v", err)
	}
}

func TestLineCallsAndCallIDResolveAcrossLines(t *testing.T) {
	m, ims, cfg := newCallManager(t)
	ctx := context.Background()
	first := LineID(cfg.DeviceID, cfg.Profiles[0].ICCID)
	second := LineID(cfg.DeviceID, cfg.Profiles[1].ICCID)
	ims.session(first).plantIncoming("incoming-1", "+12025550199")
	if _, _, err := m.DialCall(ctx, cfg.DeviceID, second, "+12025550100"); err != nil {
		t.Fatal(err)
	}

	calls, err := m.LineCalls(cfg.DeviceID)
	if err != nil || len(calls) != 2 {
		t.Fatalf("LineCalls = %+v, %v", calls, err)
	}
	byID := make(map[string]LineCall)
	for _, item := range calls {
		byID[item.Call.ID] = item
	}
	if got := byID["incoming-1"].Line; got.SessionID != first || got.ICCID != cfg.Profiles[0].ICCID {
		t.Fatalf("incoming call line = %+v", got)
	}
	if got := byID["call-+12025550100"].Line; got.SessionID != second {
		t.Fatalf("outgoing call line = %+v", got)
	}
	if _, err := m.LineCalls("other-reader"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown group = %v", err)
	}

	// Answer and hang up without naming the line: the call ID is enough.
	answered, identity, err := m.AnswerCall(ctx, cfg.DeviceID, "", "incoming-1")
	if err != nil || answered.State != "active" || identity.SessionID != first {
		t.Fatalf("answer by call id = %+v, %+v, %v", answered, identity, err)
	}
	identity, err = m.HangupCall(ctx, cfg.DeviceID, "", "call-+12025550100")
	if err != nil || identity.SessionID != second {
		t.Fatalf("hangup by call id = %+v, %v", identity, err)
	}
	if _, _, err := m.AnswerCall(ctx, cfg.DeviceID, "", "missing"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown call id = %v, want ErrNotRegistered", err)
	}
	// A selector that names a line without that call is rejected by the session.
	if _, _, err := m.AnswerCall(ctx, cfg.DeviceID, second, "incoming-1"); err == nil {
		t.Fatal("answer on the wrong line succeeded")
	}
}

func TestCallMediaRequiresMediaCapableSession(t *testing.T) {
	m, ims, cfg := newCallManager(t)
	first := LineID(cfg.DeviceID, cfg.Profiles[0].ICCID)
	ims.session(first).plantIncoming("incoming-1", "+12025550199")
	if _, _, err := m.CallMedia(context.Background(), cfg.DeviceID, "", "incoming-1"); err == nil {
		t.Fatal("media from a signalling-only session succeeded")
	}
}
