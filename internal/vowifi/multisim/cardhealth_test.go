package multisim

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

type probeBackend struct {
	mu       sync.Mutex
	iccid    string
	err      error
	reads    int
	switches int
}

func (backend *probeBackend) ActiveICCID(context.Context) (string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.reads++
	if backend.err != nil {
		return "", backend.err
	}
	return backend.iccid, nil
}

func (backend *probeBackend) SwitchProfile(context.Context, Profile) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.switches++
	return nil
}

func (backend *probeBackend) ReadIdentity(context.Context, string) (vowifi.SIMIdentity, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return vowifi.SIMIdentity{ICCID: backend.iccid, IMSI: "310240123456789"}, nil
}

func (backend *probeBackend) CheckReady(context.Context, vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	return vowifi.AKAEvidence{Ready: true, Application: "usim"}, nil
}

func (backend *probeBackend) Authenticate(context.Context, vowifi.SIMIdentity, vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return vowifi.AKAResult{RES: []byte{1, 2, 3, 4}}, nil
}

func (backend *probeBackend) AuthenticateWithPreference(ctx context.Context, identity vowifi.SIMIdentity, challenge vowifi.AKAChallenge, _ string) (vowifi.AKAResult, error) {
	return backend.Authenticate(ctx, identity, challenge)
}

func (backend *probeBackend) counts() (int, int) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.reads, backend.switches
}

func newProbeBroker(t *testing.T, backend *probeBackend) *AuthBroker {
	t.Helper()
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "dev", Backend: backend, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return broker
}

func TestProbeCardRecordsLiveness(t *testing.T) {
	backend := &probeBackend{iccid: "8988211000000000042"}
	broker := newProbeBroker(t, backend)

	if health := broker.CardHealth(); !health.VerifiedAt.IsZero() {
		t.Fatal("a fresh broker must not claim the card answered")
	}

	probed, err := broker.ProbeCard(context.Background(), time.Minute)
	if err != nil || !probed {
		t.Fatalf("ProbeCard = %v, %v; want probed with no error", probed, err)
	}
	health := broker.CardHealth()
	if health.VerifiedAt.IsZero() || health.LastError != "" {
		t.Fatalf("health = %+v", health)
	}
	if health.ActiveICCID != backend.iccid {
		t.Fatalf("active iccid = %q, want %q", health.ActiveICCID, backend.iccid)
	}
}

// A probe must not spend a read when something else already proved the card is
// alive inside the staleness window. At 60 modems this is the difference
// between a background trickle and a steady stream of reader contention.
func TestProbeCardSkipsWhenRecentlyVerified(t *testing.T) {
	backend := &probeBackend{iccid: "8988211000000000042"}
	broker := newProbeBroker(t, backend)

	if _, err := broker.ProbeCard(context.Background(), time.Minute); err != nil {
		t.Fatal(err)
	}
	readsAfterFirst, _ := backend.counts()

	probed, err := broker.ProbeCard(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if probed {
		t.Fatal("second probe inside the window should have been skipped")
	}
	reads, _ := backend.counts()
	if reads != readsAfterFirst {
		t.Fatalf("reads = %d, want unchanged %d", reads, readsAfterFirst)
	}
}

func TestProbeCardRecordsFailureWithoutLosingTheLastGoodTime(t *testing.T) {
	backend := &probeBackend{iccid: "8988211000000000042"}
	broker := newProbeBroker(t, backend)
	if _, err := broker.ProbeCard(context.Background(), time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	good := broker.CardHealth().VerifiedAt
	if good.IsZero() {
		t.Fatal("expected a first verification")
	}

	backend.mu.Lock()
	backend.err = errors.New("no card")
	backend.mu.Unlock()

	probed, err := broker.ProbeCard(context.Background(), time.Nanosecond)
	if !probed || err == nil {
		t.Fatalf("ProbeCard = %v, %v; want a probed failure", probed, err)
	}
	health := broker.CardHealth()
	if health.LastError == "" {
		t.Fatal("failure was not recorded")
	}
	// The age of the last good answer is what says how stale the card state is;
	// a failure must not erase it.
	if !health.VerifiedAt.Equal(good) {
		t.Fatalf("VerifiedAt = %v, want the earlier good time %v", health.VerifiedAt, good)
	}
}

// The probe shares the broker's transaction token, so it can never run while a
// profile switch or an authentication holds the reader.
func TestProbeCardWaitsForTheTransactionToken(t *testing.T) {
	backend := &probeBackend{iccid: "8988211000000000042"}
	broker := newProbeBroker(t, backend)

	<-broker.transaction // hold the reader as an authentication would
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := broker.ProbeCard(ctx, time.Nanosecond)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("probe error = %v, want deadline exceeded while the reader is held", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not give up while the reader was held")
	}
	broker.transaction <- struct{}{}

	if _, err := broker.ProbeCard(context.Background(), time.Nanosecond); err != nil {
		t.Fatalf("probe after the reader was released: %v", err)
	}
}
