package multisim

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

func TestBrokerLogsLifecycleWithoutAuthenticationMaterial(t *testing.T) {
	backend := newBackend()
	var output bytes.Buffer
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend, Logger: slog.New(slog.NewJSONHandler(&output, nil))})
	if err != nil {
		t.Fatal(err)
	}
	a, err := broker.ForProfile(testConfig().Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	id, err := a.ReadIdentity(context.Background(), "line-b")
	if err != nil {
		t.Fatal(err)
	}
	backend.authError = errors.New("private-authentication-vector")
	if _, err := a.Authenticate(context.Background(), id, vowifi.AKAChallenge{}); err == nil {
		t.Fatal("expected backend failure")
	}
	log := output.String()
	for _, event := range []string{"AKA queued", "AKA reader acquired", "AKA failed", "profile switch requested", "profile switch verified", "wait_ms", "total_ms", "error_class"} {
		if !strings.Contains(log, event) {
			t.Fatalf("missing lifecycle evidence %q: %s", event, log)
		}
	}
	for _, secret := range []string{id.ICCID, id.IMSI, "private-authentication-vector"} {
		if strings.Contains(log, secret) {
			t.Fatal("authentication lifecycle log leaked full identity or material")
		}
	}
}

// The backend models the physical card; only the broker supplies serialization.
type testBackend struct {
	mu              sync.Mutex
	active          string
	identities      map[string]vowifi.SIMIdentity
	switches        int
	authentications int
	entered         chan struct{}
	release         chan struct{}
	wrongSwitch     bool
	authError       error
	ignoreCancel    bool
	smscReads       int
	smscError       error
}

func newBackend() *testBackend {
	c := testConfig()
	return &testBackend{active: c.Profiles[0].ICCID, identities: map[string]vowifi.SIMIdentity{
		c.Profiles[0].ICCID: {ICCID: c.Profiles[0].ICCID, IMSI: "310260000000001", HomeMCC: "310", HomeMNC: "260"},
		c.Profiles[1].ICCID: {ICCID: c.Profiles[1].ICCID, IMSI: "310260000000002", HomeMCC: "310", HomeMNC: "260"},
	}}
}
func (b *testBackend) ActiveICCID(context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.active, nil
}
func (b *testBackend) SwitchProfile(_ context.Context, p Profile) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.switches++
	if !b.wrongSwitch {
		b.active = p.ICCID
	}
	return nil
}
func (b *testBackend) ReadIdentity(_ context.Context, id string) (vowifi.SIMIdentity, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if id != "physical-reader" {
		return vowifi.SIMIdentity{}, errors.New("logical ID reached physical backend")
	}
	return b.identities[b.active], nil
}
func (b *testBackend) CheckReady(_ context.Context, id vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != id.ICCID {
		return vowifi.AKAEvidence{}, ErrIdentityMismatch
	}
	return vowifi.AKAEvidence{Ready: true, Application: "USIM"}, nil
}
func (b *testBackend) Authenticate(ctx context.Context, id vowifi.SIMIdentity, c vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return b.AuthenticateWithPreference(ctx, id, c, "")
}
func (b *testBackend) AuthenticateWithPreference(ctx context.Context, id vowifi.SIMIdentity, c vowifi.AKAChallenge, _ string) (vowifi.AKAResult, error) {
	b.mu.Lock()
	b.authentications++
	entered, release, authErr, ignoreCancel := b.entered, b.release, b.authError, b.ignoreCancel
	b.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		if ignoreCancel {
			<-release
		} else {
			select {
			case <-release:
			case <-ctx.Done():
				return vowifi.AKAResult{}, ctx.Err()
			}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != id.ICCID {
		return vowifi.AKAResult{}, ErrIdentityMismatch
	}
	if authErr != nil {
		return vowifi.AKAResult{}, authErr
	}
	return vowifi.AKAResult{RES: []byte{c.RAND[0]}}, nil
}

func testBroker(t *testing.T, b *testBackend) (*AuthBroker, *ProfileAdapter, *ProfileAdapter) {
	t.Helper()
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: b, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a, err := broker.ForProfile(testConfig().Profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	other, err := broker.ForProfile(testConfig().Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	return broker, a, other
}

func TestBrokerSerializesSwitchAndAKATransactionWithCancelableQueue(t *testing.T) {
	backend := newBackend()
	backend.entered = make(chan struct{}, 1)
	backend.release = make(chan struct{})
	_, a, b := testBroker(t, backend)
	identity, err := a.ReadIdentity(context.Background(), "logical-a")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := a.Authenticate(context.Background(), identity, vowifi.AKAChallenge{}); done <- err }()
	<-backend.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.ReadIdentity(ctx, "logical-b"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting request ignored deadline: %v", err)
	}
	backend.mu.Lock()
	switches := backend.switches
	backend.mu.Unlock()
	if switches != 0 {
		t.Fatal("queued request switched card during another AKA")
	}
	close(backend.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadIdentity(context.Background(), "logical-b"); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerVerifiesCommittedSwitchAndPinnedIdentity(t *testing.T) {
	backend := newBackend()
	_, a, b := testBroker(t, backend)
	original, err := a.ReadIdentity(context.Background(), "logical-a")
	if err != nil {
		t.Fatal(err)
	}
	backend.wrongSwitch = true
	if _, err := b.ReadIdentity(context.Background(), "logical-b"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("uncommitted switch accepted: %v", err)
	}
	backend.mu.Lock()
	changed := backend.identities[original.ICCID]
	changed.IMSI = "310260000009999"
	backend.identities[original.ICCID] = changed
	backend.mu.Unlock()
	if _, err := a.ReadIdentity(context.Background(), "logical-a"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("identity silently repinned: %v", err)
	}
	if _, err := a.Authenticate(context.Background(), original, vowifi.AKAChallenge{}); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong IMSI authenticated: %v", err)
	}
	if backend.authentications != 0 {
		t.Fatal("authentication reached mismatched physical identity")
	}
}

func TestBrokerNeverCachesAKAAndRedactsBackendErrors(t *testing.T) {
	backend := newBackend()
	_, a, _ := testBroker(t, backend)
	id, err := a.ReadIdentity(context.Background(), "line-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, rand := range []byte{1, 2, 2} {
		c := vowifi.AKAChallenge{}
		c.RAND[0] = rand
		result, err := a.Authenticate(context.Background(), id, c)
		if err != nil || len(result.RES) != 1 || result.RES[0] != rand {
			t.Fatalf("challenge mismatch: %v", err)
		}
	}
	if backend.authentications != 3 {
		t.Fatal("AKA result cached")
	}
	backend.authError = errors.New("AT AUTH secret RAND AUTN CK IK")
	_, err = a.Authenticate(context.Background(), id, vowifi.AKAChallenge{})
	if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "RAND") {
		t.Fatalf("backend authentication material leaked: %v", err)
	}
}

func TestCanceledAuthenticationHoldsReaderUntilBackendReturns(t *testing.T) {
	backend := newBackend()
	backend.entered = make(chan struct{}, 1)
	backend.release = make(chan struct{})
	backend.ignoreCancel = true
	_, a, b := testBroker(t, backend)
	identity, err := a.ReadIdentity(context.Background(), "line-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := a.Authenticate(ctx, identity, vowifi.AKAChallenge{}); done <- err }()
	<-backend.entered
	cancel()
	waiting, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := b.ReadIdentity(waiting, "line-b"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reader released while cancelled hardware still running: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("authentication returned before hardware finished: %v", err)
	default:
	}
	close(backend.release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("late successful vector escaped cancellation: %v", err)
	}
	if _, err := b.ReadIdentity(context.Background(), "line-b"); err != nil {
		t.Fatal(err)
	}
}

// The backend exposes the service-centre address of the profile that is
// switched in, the same way the single-line EC20 adapter answers AT+CSCA?.
type smscBackend struct{ *testBackend }

func (b *smscBackend) ReadSMSCenter(_ context.Context, id string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.smscReads++
	if id != "physical-reader" {
		return "", errors.New("logical ID reached physical backend")
	}
	if b.smscError != nil {
		return "", b.smscError
	}
	return "+1206313000" + b.active[len(b.active)-1:], nil
}

func TestProfileAdapterReadsServiceCentreOfItsOwnProfileAndCachesIt(t *testing.T) {
	backend := &smscBackend{newBackend()}
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	a, err := broker.ForProfile(cfg.Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.ForProfile(cfg.Profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadIdentity(context.Background(), "line-b"); err != nil {
		t.Fatal(err)
	}
	smsc, err := a.ReadSMSCenter(context.Background(), "line-b")
	if err != nil || smsc != "+12063130002" {
		t.Fatalf("line B service centre = %q, %v", smsc, err)
	}
	switches := backend.switches
	if smsc, err := a.ReadSMSCenter(context.Background(), "line-b"); err != nil || smsc != "+12063130002" || backend.switches != switches || backend.smscReads != 1 {
		t.Fatalf("cached read = %q, %v (switches %d→%d, reads %d)", smsc, err, switches, backend.switches, backend.smscReads)
	}
	// Reading through a sibling adapter switches the card and reads that
	// sibling's own address; it must not return the cached value of line B.
	if smsc, err := b.ReadSMSCenter(context.Background(), "line-a"); err != nil || smsc != "+12063130001" || backend.active != cfg.Profiles[0].ICCID {
		t.Fatalf("line A service centre = %q, %v (active %s)", smsc, err, backend.active)
	}
	plain, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: newBackend()})
	if err != nil {
		t.Fatal(err)
	}
	c, err := plain.ForProfile(cfg.Profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	if smsc, err := c.ReadSMSCenter(context.Background(), "line-a"); err == nil || smsc != "" {
		t.Fatalf("backend without a service-centre reader answered %q, %v", smsc, err)
	}
}

func TestProfileAdapterServiceCentreFailureDoesNotBreakIdentityReads(t *testing.T) {
	backend := &smscBackend{newBackend()}
	backend.smscError = errors.New("+CMS ERROR: 302")
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	a, err := broker.ForProfile(testConfig().Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	if id, err := a.ReadIdentity(context.Background(), "line-b"); err != nil || id.ICCID != testConfig().Profiles[1].ICCID {
		t.Fatalf("identity = %+v, %v", id, err)
	}
	smsc, readErr := a.ReadSMSCenter(context.Background(), "line-b")
	if readErr == nil || smsc != "" {
		t.Fatalf("failed service-centre read answered %q, %v", smsc, readErr)
	}
	if strings.Contains(readErr.Error(), "+CMS ERROR") {
		t.Fatal("raw modem text leaked through the broker")
	}
	backend.smscError = nil
	if smsc, err := a.ReadSMSCenter(context.Background(), "line-b"); err != nil || smsc != "+12063130002" {
		t.Fatalf("retry after failure = %q, %v", smsc, err)
	}
}
