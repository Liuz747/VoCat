package ims

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

func TestContactInstanceIDs(t *testing.T) {
	contacts := []string{
		`<sip:u@10.0.0.1:5060;transport=tcp>;+sip.instance="<urn:gsma:imei:86210704-811389-0>";+g.3gpp.smsip;expires=600`,
		`<sip:other@10.0.0.2:5060>;expires=600`,
		`<sip:u@10.0.0.3:5060>;expires=300;+sip.instance="<urn:uuid:aaaa>"`,
	}
	got := contactInstanceIDs(contacts)
	want := []string{"urn:gsma:imei:86210704-811389-0", "-", "urn:uuid:aaaa"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("contactInstanceIDs() = %v, want %v", got, want)
	}
}

func newExpiryTestProvider(t *testing.T, listener *net.UDPConn, expiry time.Duration) (*Provider, vowifi.IMSRequest) {
	t.Helper()
	provider, err := NewProvider(
		&recordingAKA{result: vowifi.AKAResult{RES: []byte{1, 2, 3, 4, 5, 6, 7, 8}}},
		Config{
			PCSCF:              listener.LocalAddr().String(),
			LocalAddress:       "127.0.0.1",
			Transport:          "udp",
			TransactionTimeout: 3 * time.Second,
			SecurityMode:       SecurityDisabled,
			RegistrationExpiry: expiry,
		},
	)
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	request := vowifi.IMSRequest{
		DeviceID: "ec20",
		Identity: vowifi.SIMIdentity{
			ICCID: "8901000000000000000", IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01",
		},
		Tunnel: evidenceTunnel{evidence: vowifi.TunnelEvidence{
			Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()},
		}},
	}
	return provider, request
}

// registrarScript answers one REGISTER per step. It returns the response bytes
// and whether the conversation is complete.
var errNoAnswer = errors.New("registrar: intentionally silent")

type registrarScript func(step int, headers map[string]string, callID string) (response []byte, done bool, err error)

func serveScriptedRegistrar(listener *net.UDPConn, script registrarScript) error {
	callID := ""
	for step := 0; ; step++ {
		packet := make([]byte, 65535)
		count, remote, err := listener.ReadFromUDP(packet)
		if err != nil {
			return err
		}
		startLine, headers, err := parseTestRequest(packet[:count])
		if err != nil {
			return err
		}
		if !strings.HasPrefix(startLine, "REGISTER ") {
			return fmt.Errorf("unexpected start line %q", startLine)
		}
		if step == 0 {
			callID = headers["call-id"]
		}
		response, done, err := script(step, headers, callID)
		if errors.Is(err, errNoAnswer) {
			// Model a silently discarded request: no response at all.
			continue
		}
		if err != nil {
			return err
		}
		if _, err := listener.WriteToUDP(response, remote); err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func challengeResponse(callID, cseq, nonce string) []byte {
	return testResponse(401, "Unauthorized", callID, cseq, []string{
		`WWW-Authenticate: Digest realm="ims.mnc001.mcc001.3gppnetwork.org", nonce="` + nonce + `", algorithm=AKAv1-MD5, qop="auth"`,
	})
}

func okRegistration(callID, cseq, contact string) []byte {
	return testResponse(200, "OK", callID, cseq, []string{
		"P-Associated-URI: <sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org>, <tel:+8613800138000>",
		"Contact: " + contact + ";expires=600",
		"Service-Route: <sip:route.ims.example;lr>",
	})
}

// Regression: a 423 Interval Too Brief used to be reported as a plain
// rejection. RFC 3261 §10.3 says to resend with the registrar's Min-Expires.
func TestRegisterRetriesWithMinExpiresOn423(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				if headers["expires"] != "300" {
					return nil, true, fmt.Errorf("initial Expires = %q, want 300", headers["expires"])
				}
				return testResponse(423, "Interval Too Brief", callID, headers["cseq"], []string{"Min-Expires: 1800"}), false, nil
			case 2:
				if headers["expires"] != "1800" {
					return nil, true, fmt.Errorf("retry Expires = %q, want Min-Expires 1800", headers["expires"])
				}
				if headers["authorization"] == "" {
					return nil, true, errors.New("retry after 423 dropped Authorization")
				}
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			default:
				if headers["expires"] != "0" {
					return nil, true, fmt.Errorf("deregister Expires = %q, want 0", headers["expires"])
				}
				return testResponse(200, "OK", callID, headers["cseq"], nil), true, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 5*time.Minute)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	if evidence := session.Evidence(); !evidence.Registered || evidence.LastSIPCode != 200 {
		t.Fatalf("evidence = %#v", evidence)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("registrar error = %v", err)
	}
}

// Regression: Close() only reported "SIP deregistration returned 400". The
// reason phrase and Reason/Warning headers are needed to debug why T-Mobile
// rejects the de-registration (which leaves an orphan binding for ~3600 s).
func TestCloseSurfacesDeregistrationRejectionReason(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			default:
				if headers["expires"] != "0" {
					return nil, true, fmt.Errorf("deregister Expires = %q, want 0", headers["expires"])
				}
				// Close() falls back to the wildcard once; answer both attempts.
				return testResponse(400, "Bad Request", callID, headers["cseq"], []string{
					`Warning: 399 registrar "security verify missing"`,
				}), step >= 3, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	err = session.Close(context.Background())
	if err == nil {
		t.Fatal("Close() error = nil, want rejected de-registration")
	}
	for _, want := range []string{"de-registration REGISTER was rejected: SIP 400 Bad Request", "security verify missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Close() error = %q, want it to contain %q", err, want)
		}
	}
	if session.Evidence().Registered {
		t.Fatal("Evidence().Registered = true after Close")
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("registrar error = %v", err)
	}
}

func TestBuildContactAddsRegIDForOutbound(t *testing.T) {
	session := &Session{
		identity:   identitySet{user: "310240528577874"},
		transport:  "tcp",
		instanceID: "urn:gsma:imei:86210704-811389-0",
	}
	for _, format := range []string{vowifi.IMSContactFormatStandard, vowifi.IMSContactFormatATT, vowifi.IMSContactFormatGSMA} {
		with := session.buildContact("[2607:fc20::1]:5060", vowifi.IMSRegisterOptions{ContactFormat: format, OutboundRegID: 1})
		if !strings.Contains(with, `+sip.instance="<urn:gsma:imei:86210704-811389-0>";reg-id=1`) {
			t.Fatalf("%s Contact = %q, want reg-id right after +sip.instance", format, with)
		}
		without := session.buildContact("[2607:fc20::1]:5060", vowifi.IMSRegisterOptions{ContactFormat: format})
		if strings.Contains(without, "reg-id") {
			t.Fatalf("%s Contact = %q, want no reg-id when outbound is off", format, without)
		}
	}
}

func TestSupportedHeaderAddsOutboundOnlyWithRegID(t *testing.T) {
	custom := "path, gruu, sec-agree"
	tests := []struct {
		name string
		opts vowifi.IMSRegisterOptions
		want string
	}{
		{"default off", vowifi.IMSRegisterOptions{}, "path, gruu"},
		{"default on", vowifi.IMSRegisterOptions{OutboundRegID: 1}, "path, gruu, outbound"},
		{"profile on", vowifi.IMSRegisterOptions{SupportedHeader: &custom, OutboundRegID: 1}, "path, gruu, sec-agree, outbound"},
		{"profile already lists outbound", vowifi.IMSRegisterOptions{SupportedHeader: strPtr("path, Outbound"), OutboundRegID: 2}, "path, Outbound"},
	}
	for _, test := range tests {
		if got := supportedHeaderValue(test.opts); got != test.want {
			t.Fatalf("%s: supportedHeaderValue() = %q, want %q", test.name, got, test.want)
		}
	}
}

func strPtr(value string) *string { return &value }

func TestSessionInstanceURIIsStableWhenOutboundIsEnabled(t *testing.T) {
	identity := vowifi.SIMIdentity{IMEI: "862107048113898"}
	if got := sessionInstanceURI(vowifi.IMSRegisterOptions{OutboundRegID: 1}, identity, "uuid-1"); got != "urn:gsma:imei:86210704-811389-8" {
		t.Fatalf("outbound instance = %q, want GSMA IMEI URN", got)
	}
	if got := sessionInstanceURI(vowifi.IMSRegisterOptions{}, identity, "uuid-1"); got != "urn:uuid:uuid-1" {
		t.Fatalf("default instance = %q, want per-session UUID", got)
	}
	if got := sessionInstanceURI(vowifi.IMSRegisterOptions{OutboundRegID: 1}, vowifi.SIMIdentity{}, "uuid-2"); got != "urn:uuid:uuid-2" {
		t.Fatalf("outbound without IMEI = %q, want UUID fallback", got)
	}
}

func TestContactExpires(t *testing.T) {
	got := contactExpires([]string{"<sip:a@b>;expires=600", "<sip:c@d>", "<sip:e@f>;+sip.instance=\"<x>\";expires=3599"})
	if fmt.Sprint(got) != "[600 -1 3599]" {
		t.Fatalf("contactExpires() = %v", got)
	}
}

func TestRedactSIPRequestForLog(t *testing.T) {
	raw := []byte("REGISTER sip:ims.mnc240.mcc310.3gppnetwork.org SIP/2.0\r\n" +
		"Via: SIP/2.0/TCP [2607::1]:5060;branch=z9hG4bKabc;rport\r\n" +
		"From: <sip:310240528577874@ims.mnc240.mcc310.3gppnetwork.org>;tag=t1\r\n" +
		"To: <sip:310240528577874@ims.mnc240.mcc310.3gppnetwork.org>\r\n" +
		"Contact: <sip:310240528577874@[2607::1]:5060;transport=tcp>;+sip.instance=\"<urn:gsma:imei:86210704-811389-8>\";reg-id=1\r\n" +
		"Expires: 0\r\n" +
		"Authorization: Digest username=\"310240528577874@ims.mnc240.mcc310.3gppnetwork.org\", response=\"deadbeef\", nonce=\"n1\", nc=00000002\r\n" +
		"Supported: path, gruu, outbound\r\n" +
		"Content-Length: 5\r\n\r\nhello")
	got := redactSIPRequestForLog(raw, "310240528577874", "310240528577874@ims.mnc240.mcc310.3gppnetwork.org")
	for _, forbidden := range []string{"deadbeef", "310240528577874", "hello", "nonce"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("redacted request still contains %q: %s", forbidden, got)
		}
	}
	for _, want := range []string{"Expires: 0", "reg-id=1", "Authorization: <redacted>", "…7874@", "Supported: path, gruu, outbound", "branch=z9hG4bKabc"} {
		if !strings.Contains(got, want) {
			t.Fatalf("redacted request lacks %q: %s", want, got)
		}
	}
}

// Purge: with deregisterAll the Close() REGISTER carries "Contact: *" and
// "Expires: 0" (RFC 3261 §10.2.2) plus Authorization, and a 200 clears the
// session's registered state.
func TestCloseDeregistersAllBindingsWithWildcardContact(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			case 2:
				// The specific binding goes first; the 200 OK still lists one
				// binding this session did not create, which triggers the purge.
				if headers["contact"] == "*" {
					return nil, true, errors.New("first de-registration used the wildcard Contact")
				}
				return testResponse(200, "OK", callID, headers["cseq"], []string{
					"Contact: <sip:001010123456789@127.0.0.1:9999;transport=udp>;expires=1800",
				}), false, nil
			default:
				if headers["contact"] != "*" || headers["expires"] != "0" {
					return nil, true, fmt.Errorf("purge REGISTER Contact=%q Expires=%q, want * and 0", headers["contact"], headers["expires"])
				}
				if headers["authorization"] == "" {
					return nil, true, errors.New("purge REGISTER omitted Authorization")
				}
				return testResponse(200, "OK", callID, headers["cseq"], nil), true, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	concrete := session.(*Session)
	concrete.mu.Lock()
	concrete.deregisterAll = true
	concrete.mu.Unlock()
	// Close() re-reads the profile option; the test identity has no profile with
	// deregister_all_on_close, so force the flag through a helper hook instead.
	deregisterAllOverride = true
	t.Cleanup(func() { deregisterAllOverride = false })
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if session.Evidence().Registered {
		t.Fatal("Evidence().Registered = true after purge")
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("registrar error = %v", err)
	}
}

// RFC 3261 §10.2.2: the wildcard Contact exists for clients that do not know
// their own bindings. This client does, so it de-registers the specific
// binding first; T-Mobile answers 480 "Function is not allowed" to the
// wildcard form often enough that leading with it costs a round trip.
func TestCloseDeregistersTheSpecificContactFirst(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	var mu sync.Mutex
	var deregisterContacts []string
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			default:
				mu.Lock()
				deregisterContacts = append(deregisterContacts, headers["contact"])
				mu.Unlock()
				// 200 OK with no Contact: nothing is left bound.
				return testResponse(200, "OK", callID, headers["cseq"], nil), true, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deregisterContacts) != 1 {
		t.Fatalf("de-registration attempts = %v, want exactly 1", deregisterContacts)
	}
	if strings.TrimSpace(deregisterContacts[0]) == "*" {
		t.Fatalf("first de-registration used the wildcard Contact: %v", deregisterContacts)
	}
}

// The wildcard remains the fallback: it is the only way to clear bindings this
// client did not create.
func TestCloseFallsBackToWildcardWhenTheSpecificContactIsRejected(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	var mu sync.Mutex
	var deregisterContacts []string
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			case 2:
				mu.Lock()
				deregisterContacts = append(deregisterContacts, headers["contact"])
				mu.Unlock()
				return testResponse(480, "Temporarily Unavailable", callID, headers["cseq"], []string{
					`Warning: 122 10.180.102.4 "Function is not allowed"`,
				}), false, nil
			default:
				mu.Lock()
				deregisterContacts = append(deregisterContacts, headers["contact"])
				mu.Unlock()
				return testResponse(200, "OK", callID, headers["cseq"], nil), true, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v, want the wildcard fallback to succeed", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deregisterContacts) != 2 || strings.TrimSpace(deregisterContacts[1]) != "*" {
		t.Fatalf("de-registration attempts = %v, want a specific one then the wildcard", deregisterContacts)
	}
}

// Regression: T-Mobile sometimes never answers a de-registration for a number
// that has re-registered many times. Close() must give up quickly instead of
// blocking teardown (and the eSIM switch behind it) for the caller's whole
// cleanup budget; answers to a de-registration arrive in well under a second.
func TestCloseStopsWaitingForAnUnansweredDeregistration(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(30 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	go func() {
		_ = serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
			switch step {
			case 0:
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			case 1:
				return okRegistration(callID, headers["cseq"], headers["contact"]), false, nil
			default:
				// Silence: answer only long after the de-registration budget.
				time.Sleep(20 * time.Second)
				return testResponse(200, "OK", callID, headers["cseq"], nil), true, nil
			}
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		t.Fatalf("Provider.Start() error = %v", err)
	}
	started := time.Now()
	err = session.Close(context.Background())
	elapsed := time.Since(started)
	// TS 33.203 §7.4.1a: no response to a protected message means the network
	// already dropped the security association, which is exactly what a
	// de-registration asks for. Treat it as done rather than an error.
	if err != nil {
		t.Fatalf("Close() error = %v, want an unanswered de-registration treated as complete", err)
	}
	if elapsed > 4*deregisterTimeout {
		t.Fatalf("Close() took %v; an unanswered de-registration must not block teardown", elapsed)
	}
}

// failingAKA models the SIM going away mid-registration: the rotation switched
// the eUICC while a stale retry was still using the previous identity.
type failingAKA struct{ err error }

func (fake *failingAKA) Authenticate(context.Context, vowifi.SIMIdentity, vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return vowifi.AKAResult{}, fake.err
}

func (fake *failingAKA) CheckReady(context.Context, vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	return vowifi.AKAEvidence{Ready: true, Application: "usim"}, nil
}

// Regression: an AKA failure is a property of the SIM, not of the P-CSCF, but
// Start used to walk every remaining P-CSCF (12 s each, silently) before
// giving up. Fail immediately so the runtime can retry with a fresh identity.
func TestStartAbortsImmediatelyWhenTheSIMIdentityChanged(t *testing.T) {
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	var mu sync.Mutex
	attempts := 0
	addresses := make([]string, 0, 2)
	for index := 0; index < 2; index++ {
		listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
		addresses = append(addresses, listener.LocalAddr().String())
		go func() {
			_ = serveScriptedRegistrar(listener, func(step int, headers map[string]string, callID string) ([]byte, bool, error) {
				mu.Lock()
				attempts++
				mu.Unlock()
				return challengeResponse(callID, headers["cseq"], nonce), false, nil
			})
		}()
	}
	provider, err := NewProvider(&failingAKA{err: vowifi.ErrEC20IdentityChanged}, Config{
		LocalAddress:       "127.0.0.1",
		Transport:          "udp",
		TransactionTimeout: 3 * time.Second,
		SecurityMode:       SecurityDisabled,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	request := vowifi.IMSRequest{
		DeviceID: "ec20",
		Identity: vowifi.SIMIdentity{ICCID: "8901000000000000000", IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel: evidenceTunnel{evidence: vowifi.TunnelEvidence{
			Established: true, LocalIPv4: "127.0.0.1", PCSCF: addresses,
		}},
	}
	started := time.Now()
	if _, err := provider.Start(context.Background(), request); !errors.Is(err, vowifi.ErrEC20IdentityChanged) {
		t.Fatalf("Start() error = %v, want %v", err, vowifi.ErrEC20IdentityChanged)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Start() took %v; a SIM identity change must abort at once", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Fatalf("REGISTER attempts = %d, want 1 (another P-CSCF cannot fix a changed SIM)", attempts)
	}
}

// Regression: T-Mobile's P-CSCF installs the IPsec SAs only after it has sent
// the 401 (TS 33.203 §7.4.2a orders it that way) and silently discards
// anything that arrives on a mismatched SA, so the authenticated REGISTER is
// lost about one attempt in seven. TS 33.203 §7.3.1.4 says a fresh challenge
// makes the P-CSCF drop the previous registration's state, so retrying the
// same endpoint with new SPIs and ports is the documented way out; moving to
// another P-CSCF just abandons a healthy one.
func TestStartRetriesTheSameEndpointWhenTheAuthenticatedRegisterIsLost(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.SetDeadline(time.Now().Add(20 * time.Second))
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	var mu sync.Mutex
	firstCallID := ""
	callIDs := map[string]bool{}
	go func() {
		_ = serveScriptedRegistrar(listener, func(step int, headers map[string]string, _ string) ([]byte, bool, error) {
			id := headers["call-id"]
			mu.Lock()
			if firstCallID == "" {
				firstCallID = id
			}
			callIDs[id] = true
			isFirstAttempt := id == firstCallID
			mu.Unlock()
			if headers["authorization"] == "" {
				return challengeResponse(id, headers["cseq"], nonce), false, nil
			}
			if isFirstAttempt {
				// The authenticated REGISTER of the first attempt is swallowed,
				// exactly as T-Mobile does when its SA is not installed yet.
				return nil, false, errNoAnswer
			}
			return okRegistration(id, headers["cseq"], headers["contact"]), false, nil
		})
	}()
	provider, request := newExpiryTestProvider(t, listener, 0)
	provider.config.TransactionTimeout = time.Second
	provider.config.RetryLostRegistration = true
	session, err := provider.Start(context.Background(), request)
	if err != nil {
		mu.Lock()
		seen := len(callIDs)
		mu.Unlock()
		t.Fatalf("Start() error = %v (attempts seen = %d), want the same endpoint retried", err, seen)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	mu.Lock()
	defer mu.Unlock()
	if len(callIDs) != 2 {
		t.Fatalf("registration attempts = %d, want 2 on the same endpoint", len(callIDs))
	}
}
