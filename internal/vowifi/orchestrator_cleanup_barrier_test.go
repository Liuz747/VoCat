package vowifi

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitCleanupTracksProviderAfterContainmentDeadline(t *testing.T) {
	o := newTestOrchestratorWithOptions(t, newFakeEnvironment(), Options{DeviceID: "EC20", CleanupTimeout: 20 * time.Millisecond})
	release := make(chan struct{})
	done := make(chan struct{})
	if err := o.cleanupCall(func(context.Context) error { <-release; close(done); return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("containment returned %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := o.WaitCleanup(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		close(release)
		t.Fatalf("barrier ignored unfinished provider: %v", err)
	}
	cancel()
	close(release)
	if err := o.WaitCleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	default:
		t.Fatal("barrier returned before actual provider completion")
	}
}

func TestEnableWaitsForPreviousProviderCleanup(t *testing.T) {
	env := newFakeEnvironment()
	o := newTestOrchestratorWithOptions(t, env, Options{DeviceID: "EC20", CleanupTimeout: 20 * time.Millisecond})
	release := make(chan struct{})
	if err := o.cleanupCall(func(context.Context) error { <-release; return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("containment returned %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := o.Enable(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("enable bypassed old provider cleanup: %v", err)
	}
	env.mu.Lock()
	calls := append([]string(nil), env.calls...)
	env.mu.Unlock()
	if len(calls) != 0 {
		close(release)
		t.Fatalf("enable touched resources before old cleanup returned: %v", calls)
	}
	close(release)
	if state, err := o.Enable(context.Background()); err != nil || !state.SMSReady {
		t.Fatalf("enable after cleanup failed: state=%v err=%v", state.Phase, err)
	}
	if err := o.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := o.WaitCleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
}
