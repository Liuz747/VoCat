package device

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type ussdSuspender interface {
	SuspendUSSD(context.Context, string) (func(), error)
}

func getUSSDSuspender(t *testing.T, manager *Manager) ussdSuspender {
	t.Helper()
	suspender, ok := any(manager).(ussdSuspender)
	if !ok {
		t.Fatal("missing reader-scoped USSD suspension")
	}
	return suspender
}
func TestUSSDSuspensionInvalidatesOldSessionsAndBlocksNewOnSameReader(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{{command: `AT+CUSD=1,"*100#",15`, response: okResponse()}}, urcs: []string{`+CUSD: 0,"done",15`}}
	manager, id := newStartedTestManager(t, client)
	old := manager.openUSSDSession(id)
	other := manager.openUSSDSession("other-reader")
	resume, err := getUSSDSuspender(t, manager).SuspendUSSD(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	if _, err := manager.ContinueUSSD(context.Background(), old, "1"); !errors.Is(err, ErrUSSDSessionNotFound) {
		t.Fatalf("old Continue=%v", err)
	}
	if err := manager.CancelUSSD(context.Background(), old); !errors.Is(err, ErrUSSDSessionNotFound) {
		t.Fatalf("old Cancel=%v", err)
	}
	if _, err := manager.USSD(context.Background(), id, "*100#"); !errors.Is(err, ErrUSSDSuspended) {
		t.Fatalf("suspended reader start error=%v", err)
	}
	if _, err := manager.ussdSessionDevice(other); err != nil {
		t.Fatal("other reader session removed")
	}
	resume()
	if _, err := manager.ContinueUSSD(context.Background(), old, "1"); !errors.Is(err, ErrUSSDSessionNotFound) {
		t.Fatalf("old session revived after restore=%v", err)
	}
	if _, err := manager.USSD(context.Background(), id, "*100#"); err != nil {
		t.Fatalf("restored reader cannot start new USSD: %v", err)
	}
	client.assertDone(t)
}

type observedUSSDContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (ctx *observedUSSDContext) Err() error {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Err()
}
func TestQueuedUSSDRechecksSessionBeforeAT(t *testing.T) {
	for _, operation := range []string{"continue", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			client := &transcriptClient{}
			manager, id := newStartedTestManager(t, client)
			session := manager.openUSSDSession(id)
			state, _ := manager.lookup(id)
			state.opMu.Lock()
			ctx := &observedUSSDContext{Context: context.Background(), waiting: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				var err error
				if operation == "continue" {
					_, err = manager.ContinueUSSD(ctx, session, "1")
				} else {
					err = manager.CancelUSSD(ctx, session)
				}
				done <- err
			}()
			select {
			case <-ctx.waiting:
			case <-time.After(100 * time.Millisecond):
			}
			// Same invalidation performed by SuspendUSSD, while this old request has
			// already resolved its session ID and is waiting for the modem lock.
			manager.dropUSSDSession(session)
			state.opMu.Unlock()
			if err := <-done; !errors.Is(err, ErrUSSDSessionNotFound) {
				t.Fatalf("stale queued session reached modem: %v", err)
			}
			client.assertDone(t)
		})
	}
}
func TestUSSDSuspensionHonorsBusyModemDeadline(t *testing.T) {
	manager, id := newStartedTestManager(t, &transcriptClient{})
	session := manager.openUSSDSession(id)
	state, _ := manager.lookup(id)
	state.opMu.Lock()
	defer state.opMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	resume, err := getUSSDSuspender(t, manager).SuspendUSSD(ctx, id)
	if resume != nil {
		resume()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("suspend deadline=%v", err)
	}
	if _, err := manager.ussdSessionDevice(session); err != nil {
		t.Fatal("failed suspension destroyed session")
	}
}
