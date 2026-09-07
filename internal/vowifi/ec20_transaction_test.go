package vowifi

import (
	"context"
	"errors"
	"testing"
	"time"

	"vocat/internal/modem"
)

type transactionCheckingExecutor struct {
	onBegin        func()
	locked         bool
	checkedOutside bool
	commands       []string
}

func (executor *transactionCheckingExecutor) BeginUICCTransaction(ctx context.Context, _ string) (context.Context, func(), error) {
	if executor.onBegin != nil {
		executor.onBegin()
	}
	executor.locked = true
	return ctx, func() { executor.locked = false }, nil
}
func (executor *transactionCheckingExecutor) ExecuteAT(_ context.Context, _ string, command string) (modem.Response, error) {
	executor.commands = append(executor.commands, command)
	if !executor.locked {
		executor.checkedOutside = true
	}
	if command == "AT+CPIN?" {
		return modem.Response{Final: "OK", Lines: []string{"+CPIN: READY"}}, nil
	}
	// Simulate a profile changed before the transaction was acquired. No AKA
	// application selection or authentication may follow this identity.
	if command == "AT+QCCID" || command == "AT+CCID" {
		return modem.Response{Final: "OK", Lines: []string{"+QCCID: 8988000000000000002", "+CCID: 8988000000000000002"}}, nil
	}
	return modem.Response{}, errors.New("stop identity read")
}
func TestEC20IdentityAndReadinessUseTransactionBeforeCardAccess(t *testing.T) {
	for _, operation := range []string{"identity", "ready", "authenticate"} {
		t.Run(operation, func(t *testing.T) {
			executor := &transactionCheckingExecutor{}
			adapter, _ := NewEC20Adapter(executor, EC20AdapterOptions{})
			identity := SIMIdentity{ICCID: "8988000000000000001", IMSI: "001010000000001"}
			adapter.bindings[identity.ICCID] = ec20SIMBinding{deviceID: "reader", iccid: identity.ICCID, imsi: identity.IMSI, aid: "A0000000871002", application: "USIM"}
			switch operation {
			case "identity":
				_, _ = adapter.ReadIdentity(context.Background(), "reader")
			case "ready":
				_, _ = adapter.CheckReady(context.Background(), identity)
			case "authenticate":
				_, _ = adapter.Authenticate(context.Background(), identity, AKAChallenge{})
			}
			if executor.checkedOutside {
				t.Fatal("card identity accessed outside transaction")
			}
			if executor.locked {
				t.Fatal("transaction leaked")
			}
			if operation != "identity" {
				for _, command := range executor.commands {
					if command != "AT+QCCID" && command != "AT+CCID" {
						t.Fatalf("stale identity issued %s", command)
					}
				}
			}
		})
	}
}

type blockingReaderExecutor struct {
	entered chan string
	unblock chan struct{}
}

func (executor *blockingReaderExecutor) ExecuteAT(ctx context.Context, id, command string) (modem.Response, error) {
	executor.entered <- id
	if id == "reader-a" {
		select {
		case <-executor.unblock:
		case <-ctx.Done():
			return modem.Response{}, ctx.Err()
		}
	}
	return modem.Response{}, errors.New("identity unavailable")
}
func TestEC20BlockedReaderDoesNotBlockOtherReadersOrCanceledCallers(t *testing.T) {
	executor := &blockingReaderExecutor{entered: make(chan string, 4), unblock: make(chan struct{})}
	adapter, _ := NewEC20Adapter(executor, EC20AdapterOptions{})
	done := make(chan struct{})
	go func() { defer close(done); _, _ = adapter.ReadIdentity(context.Background(), "reader-a") }()
	defer func() { close(executor.unblock); <-done }()
	<-executor.entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := adapter.ReadIdentity(ctx, "reader-b")
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("independent reader blocked behind reader-a")
	}
	select {
	case id := <-executor.entered:
		if id != "reader-b" {
			t.Fatal("wrong reader")
		}
	default:
		t.Fatal("other reader made no progress")
	}
	ctx, cancelWait := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelWait()
	_, err = adapter.ReadIdentity(ctx, "reader-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked caller cancellation=%v", err)
	}
	select {
	case <-executor.entered:
		t.Fatal("canceled waiter touched reader")
	default:
	}
}

func TestEC20BindingCannotMoveToAnotherReaderWhileWaiting(t *testing.T) {
	for _, operation := range []string{"ready", "authenticate"} {
		t.Run(operation, func(t *testing.T) {
			executor := &transactionCheckingExecutor{}
			adapter, _ := NewEC20Adapter(executor, EC20AdapterOptions{})
			identity := SIMIdentity{ICCID: "8988000000000000001", IMSI: "001010000000001"}
			adapter.bindings[identity.ICCID] = ec20SIMBinding{deviceID: "reader-a", iccid: identity.ICCID, imsi: identity.IMSI, aid: "A0000000871002", application: "USIM"}
			executor.onBegin = func() {
				binding := adapter.bindings[identity.ICCID]
				binding.deviceID = "reader-b"
				adapter.bindings[identity.ICCID] = binding
			}
			var err error
			if operation == "ready" {
				_, err = adapter.CheckReady(context.Background(), identity)
			} else {
				_, err = adapter.Authenticate(context.Background(), identity, AKAChallenge{})
			}
			if !errors.Is(err, ErrEC20IdentityChanged) {
				t.Fatalf("moved binding error=%v", err)
			}
			if len(executor.commands) > 0 {
				t.Fatal("binding moved after acquisition and still issued AT to unlocked reader")
			}
		})
	}
}

type cleanupPinKey struct{}
type cleanupPinnedExecutor struct {
	cancel  context.CancelFunc
	cleaned bool
}

func (executor *cleanupPinnedExecutor) BeginUICCTransaction(ctx context.Context, _ string) (context.Context, func(), error) {
	return context.WithValue(ctx, cleanupPinKey{}, "reader-a"), func() {}, nil
}
func (executor *cleanupPinnedExecutor) ExecuteAT(ctx context.Context, _ string, command string) (modem.Response, error) {
	switch command {
	case "AT+CCID":
		return modem.Response{Final: "OK", Lines: []string{"+CCID: 8988000000000000001"}}, nil
	case "AT+CUAD":
		return modem.Response{Final: "OK", Lines: []string{`+CUAD: "61094F07A0000000871002"`}}, nil
	case `AT+CCHO="A0000000871002"`:
		executor.cancel()
		return modem.Response{Final: "OK", Lines: []string{"+CCHO: 1"}}, nil
	case "AT+CCHC=1":
		if ctx.Err() != nil {
			return modem.Response{}, errors.New("cleanup inherited cancellation")
		}
		if ctx.Value(cleanupPinKey{}) != "reader-a" {
			return modem.Response{}, errors.New("cleanup lost pinned reader")
		}
		executor.cleaned = true
		return modem.Response{Final: "OK"}, nil
	}
	return modem.Response{}, errors.New("unexpected command")
}
func TestEC20CleanupKeepsPhysicalReaderAfterCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &cleanupPinnedExecutor{cancel: cancel}
	adapter, _ := NewEC20Adapter(executor, EC20AdapterOptions{})
	identity := SIMIdentity{ICCID: "8988000000000000001", IMSI: "001010000000001"}
	adapter.bindings[identity.ICCID] = ec20SIMBinding{deviceID: "reader", iccid: identity.ICCID, imsi: identity.IMSI}
	_, err := adapter.CheckReady(ctx, identity)
	if err != nil || !executor.cleaned {
		t.Fatalf("cleanup did not preserve transaction reader: %v", err)
	}
}
