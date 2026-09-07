package vowifi

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeFailureWithdrawsReadinessBeforeSlowCleanup(t *testing.T) {
	env := newFakeEnvironment()
	env.tunnelFailures = make(chan error, 1)
	o := newTestOrchestratorWithOptions(t, env, Options{DeviceID: "EC20", CleanupTimeout: 100 * time.Millisecond})
	if _, e := o.Enable(context.Background()); e != nil {
		t.Fatal(e)
	}
	env.mu.Lock()
	env.blockAt = "ims.close"
	env.mu.Unlock()
	env.tunnelFailures <- errors.New("terminal")
	select {
	case <-env.blocked:
	case <-time.After(time.Second):
		t.Fatal("cleanup not reached")
	}
	state := o.State()
	if state.Active || state.SMSReady || state.IMSReady || state.TunnelReady {
		t.Fatal("ready advertised while dead IMS cleanup blocks")
	}
	deadline := time.Now().Add(time.Second)
	for o.State().Phase != PhaseFailed {
		if time.Now().After(deadline) {
			t.Fatal("cleanup did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}
