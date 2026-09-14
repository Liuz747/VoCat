package multisim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/modem"
	"vocat/internal/vowifi"
)

func TestBrokerLogsLifecycleWithoutAuthenticationMaterial(t *testing.T) {
	backend := newBackend()
	var output bytes.Buffer
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend, Logger: slog.New(slog.NewJSONHandler(&output, nil)), AKARetryDelays: quickAKARetries})
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
	authErrors      []error // consumed one per authentication before authError
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
	if len(b.authErrors) > 0 {
		authErr, b.authErrors = b.authErrors[0], b.authErrors[1:]
	}
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
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: b, RequestTimeout: time.Second, AKARetryDelays: quickAKARetries})
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

var quickAKARetries = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond}

func statusWord6985() error {
	return fmt.Errorf("%w: %w", vowifi.ErrEC20AKAResponse, &vowifi.APDUStatusError{SW: 0x6985})
}

func loggedBroker(t *testing.T, backend *testBackend) (*ProfileAdapter, *ProfileAdapter, *bytes.Buffer) {
	t.Helper()
	output := &bytes.Buffer{}
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend, RequestTimeout: time.Second,
		Logger: slog.New(slog.NewJSONHandler(output, nil)), AKARetryDelays: quickAKARetries})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	a, err := broker.ForProfile(cfg.Profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.ForProfile(cfg.Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	return a, b, output
}

// switchedAway returns a's identity with the card left on b, so a's next
// authentication has to switch profiles first.
func switchedAway(t *testing.T, a, b *ProfileAdapter) vowifi.SIMIdentity {
	t.Helper()
	identity, err := a.ReadIdentity(context.Background(), "line-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadIdentity(context.Background(), "line-b"); err != nil {
		t.Fatal(err)
	}
	return identity
}

func logLine(log, message string) string {
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, `"msg":"`+message+`"`) {
			return line
		}
	}
	return ""
}

func TestAKARepeatsTransientCardFailureInsideTheSameTransaction(t *testing.T) {
	backend := newBackend()
	a, b, output := loggedBroker(t, backend)
	identity := switchedAway(t, a, b)
	backend.authErrors = []error{statusWord6985()}
	challenge := vowifi.AKAChallenge{}
	challenge.RAND[0] = 7
	result, err := a.Authenticate(context.Background(), identity, challenge)
	if err != nil || len(result.RES) != 1 || result.RES[0] != 7 {
		t.Fatalf("authentication after one transient failure = %+v, %v", result, err)
	}
	if backend.authentications != 2 || backend.switches != 2 {
		t.Fatalf("authentications=%d switches=%d, want 2 and 2 (no extra switch between attempts)", backend.authentications, backend.switches)
	}
	log := output.String()
	retry := logLine(log, "multisim AKA authenticate retry")
	for _, field := range []string{`"attempt":1`, `"delay_ms":`, `"detail":"apdu_sw_6985"`, `"after_profile_switch":true`} {
		if !strings.Contains(retry, field) {
			t.Fatalf("retry log lacks %s: %s", field, log)
		}
	}
	if logLine(log, "multisim AKA completed") == "" || logLine(log, "multisim AKA failed") != "" {
		t.Fatalf("final outcome not logged as completed: %s", log)
	}
}

func TestAKAGivesUpAfterFourRetriesAndLogsSafeDetail(t *testing.T) {
	backend := newBackend()
	a, _, output := loggedBroker(t, backend)
	identity, err := a.ReadIdentity(context.Background(), "line-a")
	if err != nil {
		t.Fatal(err)
	}
	const secret = "0088008122101112131415161718"
	backend.authError = fmt.Errorf("%w: %w", vowifi.ErrEC20AKACommand, &modem.CommandError{Command: `AT+CSIM=76,"` + secret + `"`, Final: "+CME ERROR: 14"})
	if _, err := a.Authenticate(context.Background(), identity, vowifi.AKAChallenge{}); err == nil {
		t.Fatal("persistent failure reported success")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("returned error leaked command text: %v", err)
	}
	if backend.authentications != 5 {
		t.Fatalf("authentications = %d, want 1 + 4 retries", backend.authentications)
	}
	log := output.String()
	if got := strings.Count(log, "multisim AKA authenticate retry"); got != 4 {
		t.Fatalf("retry log lines = %d, want 4: %s", got, log)
	}
	failed := logLine(log, "multisim AKA failed")
	if !strings.Contains(failed, `"detail":"cme_error_14"`) || !strings.Contains(failed, `"after_profile_switch":false`) {
		t.Fatalf("failure log lacks safe detail: %s", log)
	}
	if strings.Contains(log, secret) {
		t.Fatal("log leaked APDU command text")
	}
}

func TestAKADoesNotRepeatCancellationRejectionOrIdentityFailures(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled, vowifi.ErrEAPAuthenticationRejected,
		ErrIdentityMismatch, vowifi.ErrInvalidIdentity, vowifi.ErrEC20IdentityChanged} {
		backend := newBackend()
		a, _, output := loggedBroker(t, backend)
		identity, err := a.ReadIdentity(context.Background(), "line-a")
		if err != nil {
			t.Fatal(err)
		}
		backend.authError = cause
		if _, err := a.Authenticate(context.Background(), identity, vowifi.AKAChallenge{}); err == nil {
			t.Fatalf("%v: reported success", cause)
		}
		if backend.authentications != 1 || strings.Contains(output.String(), "authenticate retry") {
			t.Fatalf("%v: repeated %d times", cause, backend.authentications)
		}
	}
}

func TestProfileSwitchFailureLogsSafeDetail(t *testing.T) {
	backend := &failingSwitchBackend{testBackend: newBackend(), err: &modem.CommandError{Command: `AT+CSIM=42,"secret-apdu"`, Final: "+CME ERROR: 13"}}
	output := &bytes.Buffer{}
	broker, err := NewAuthBroker(BrokerOptions{DeviceID: "physical-reader", Backend: backend, Logger: slog.New(slog.NewJSONHandler(output, nil))})
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.ForProfile(testConfig().Profiles[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadIdentity(context.Background(), "line-b"); err == nil {
		t.Fatal("failed switch reported success")
	}
	failed := logLine(output.String(), "multisim profile switch failed")
	if !strings.Contains(failed, `"detail":"cme_error_13"`) || strings.Contains(output.String(), "secret-apdu") {
		t.Fatalf("switch failure log = %s", output.String())
	}
}

type failingSwitchBackend struct {
	*testBackend
	err error
}

func (b *failingSwitchBackend) SwitchProfile(context.Context, Profile) error { return b.err }

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
