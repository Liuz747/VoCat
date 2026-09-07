package vowifi

import (
	"context"
	"errors"
	"testing"
)

type manualRefreshIMS struct {
	calls  int
	closed bool
}

func (*manualRefreshIMS) Evidence() IMSEvidence { return IMSEvidence{Registered: true} }
func (*manualRefreshIMS) EnableSMS(context.Context) (SMSEvidence, error) {
	return SMSEvidence{Ready: true}, nil
}
func (s *manualRefreshIMS) Close(context.Context) error { s.closed = true; return nil }
func (s *manualRefreshIMS) RefreshRegistration(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.calls++
	return nil
}

func TestManualRefreshUsesExistingIMSWithoutClosingSession(t *testing.T) {
	session := &manualRefreshIMS{}
	o := &Orchestrator{state: State{IMSReady: true}, resources: &runtimeResources{ims: session}}
	if err := o.RefreshRegistration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if session.calls != 1 || session.closed {
		t.Fatalf("refresh rebuilt or skipped the current session: %+v", session)
	}
}

func TestManualRefreshRejectsStoppedAndCancelled(t *testing.T) {
	o := &Orchestrator{}
	if err := o.RefreshRegistration(context.Background()); !errors.Is(err, ErrIMSNotRegistered) {
		t.Fatalf("got %v", err)
	}
	session := &manualRefreshIMS{}
	o.resources = &runtimeResources{ims: session}
	o.state.IMSReady = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.RefreshRegistration(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if session.calls != 0 {
		t.Fatal("cancelled refresh touched the session")
	}
}
