package device

import (
	"context"
	"errors"
	"testing"
	"time"
)

type uiccTransactions interface {
	BeginUICCTransaction(context.Context, string) (context.Context, func(), error)
}

func transactionManager(t *testing.T, manager *Manager) uiccTransactions {
	t.Helper()
	transaction, ok := any(manager).(uiccTransactions)
	if !ok {
		t.Fatal("manager has no per-device UICC transaction boundary")
	}
	return transaction
}

func TestUICCTransactionsIsolateReadersAndShareESIMLock(t *testing.T) {
	manager := &Manager{}
	transactions := transactionManager(t, manager)
	_, release, err := transactions.BeginUICCTransaction(context.Background(), "reader-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	_, otherRelease, err := transactions.BeginUICCTransaction(ctx, "reader-b")
	if err != nil {
		t.Fatalf("other reader was blocked: %v", err)
	}
	otherRelease()
	// Exercise the real eSIM API. It must wait on the same lock before trying
	// to find/open this fake reader, and return the caller's deadline.
	_, err = manager.ESIMListProfiles(ctx, "reader-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("eSIM escaped shared transaction: %v", err)
	}
}

func TestUICCTransactionCancellationDoesNotLeakLocks(t *testing.T) {
	manager := &Manager{}
	transactions := transactionManager(t, manager)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, unlock, err := transactions.BeginUICCTransaction(ctx, "reader"); !errors.Is(err, context.Canceled) {
		if unlock != nil {
			unlock()
		}
		t.Fatalf("canceled caller acquired: %v", err)
	}
	_, release, err := transactions.BeginUICCTransaction(context.Background(), "reader")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, unlock, err := transactions.BeginUICCTransaction(ctx, "reader"); !errors.Is(err, context.DeadlineExceeded) {
		if unlock != nil {
			unlock()
		}
		t.Fatalf("wait: %v", err)
	}
	release()
	done := make(chan struct{})
	go func() { manager.LockUICC(); manager.UnlockUICC(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation leaked legacy gate")
	}
}

func TestLegacyUICCAndKeyedTransactionsRemainExclusive(t *testing.T) {
	manager := &Manager{}
	transactions := transactionManager(t, manager)
	manager.LockUICC()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, release, err := transactions.BeginUICCTransaction(ctx, "reader")
	manager.UnlockUICC()
	if release != nil {
		release()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("legacy/keyed exclusion lost: %v", err)
	}
}

func TestATTransactionDeadlineWhileModemBusy(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "sensitive"}[sensitive], func(t *testing.T) {
			state := &managedDevice{discovered: true}
			manager := &Manager{started: true, devices: map[string]*managedDevice{"reader": state}}
			state.opMu.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if sensitive {
					_, err = manager.ExecuteSensitiveAT(ctx, "reader", "AT+CSIM")
				} else {
					_, err = manager.ExecuteAT(ctx, "reader", "AT+CCID")
				}
				done <- err
			}()
			select {
			case err := <-done:
				state.opMu.Unlock()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline=%v", err)
				}
			case <-time.After(300 * time.Millisecond):
				// Release before reporting failure so the test leaves no blocked worker.
				manager.mu.Lock()
				state.discovered = false
				manager.mu.Unlock()
				state.opMu.Unlock()
				<-done
				t.Fatal("AT transaction did not honor deadline while modem busy")
			}
		})
	}
}
