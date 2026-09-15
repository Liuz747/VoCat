package multisim

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A reader rebind must not move the physical reader underneath an
// authentication, so Exclusive shares the transaction token with every
// profile adapter.
func TestBrokerExclusiveBlocksAuthenticationUntilReleased(t *testing.T) {
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: newBackend()})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := broker.ForProfile(testConfig().Profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- broker.Exclusive(context.Background(), func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	waiting, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := adapter.ReadIdentity(waiting, "line-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("identity read ran while the reader was held exclusively: %v", err)
	}

	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ReadIdentity(context.Background(), "line-a"); err != nil {
		t.Fatalf("identity read did not resume after release: %v", err)
	}
}

func TestBrokerExclusiveSkipsOperationWhenCancelledWhileQueued(t *testing.T) {
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: newBackend()})
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = broker.Exclusive(context.Background(), func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	defer func() {
		close(release)
		<-finished
	}()

	queued, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ran := false
	err = broker.Exclusive(queued, func(context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued exclusive operation returned %v", err)
	}
	if ran {
		t.Fatal("operation ran after its context expired in the queue")
	}
}

func TestBrokerExclusiveReturnsOperationError(t *testing.T) {
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: newBackend()})
	if err != nil {
		t.Fatal(err)
	}
	refused := errors.New("different card")
	if err := broker.Exclusive(context.Background(), func(context.Context) error { return refused }); !errors.Is(err, refused) {
		t.Fatalf("got %v", err)
	}
	// The token must be back: a second exclusive section runs immediately.
	quick, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := broker.Exclusive(quick, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("token not returned after a failed operation: %v", err)
	}
}
