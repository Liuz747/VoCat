package ims

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

// The first AKA establishes registration. A fresh challenge during refresh
// stops at a cancellable barrier, representing a slow modem operation.
type refreshBarrierAKA struct {
	calls        atomic.Int32
	entered      chan struct{}
	release      chan struct{}
	releaseOnce  sync.Once
	ignoreCancel bool
}

func (*refreshBarrierAKA) CheckReady(context.Context, vowifi.SIMIdentity) (vowifi.AKAEvidence, error) {
	return vowifi.AKAEvidence{Ready: true, Application: "usim"}, nil
}

func (aka *refreshBarrierAKA) Authenticate(ctx context.Context, _ vowifi.SIMIdentity, _ vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	if aka.calls.Add(1) == 2 {
		close(aka.entered)
		if aka.ignoreCancel {
			<-aka.release
			return vowifi.AKAResult{RES: []byte{5, 6, 7, 8}}, nil
		}
		select {
		case <-ctx.Done():
			return vowifi.AKAResult{}, ctx.Err()
		case <-aka.release:
		}
	}
	return vowifi.AKAResult{RES: []byte{1, 2, 3, 4}}, nil
}

func (aka *refreshBarrierAKA) unblock() {
	aka.releaseOnce.Do(func() { close(aka.release) })
}

type refreshTestRequest struct {
	request *sipRequest
	remote  *net.UDPAddr
}

func TestRefreshAllowsInboundRPACKDuringAKAAndRegisterWait(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	_ = listener.SetDeadline(time.Now().Add(15 * time.Second))
	aka := &refreshBarrierAKA{entered: make(chan struct{}), release: make(chan struct{})}
	stored := make(chan ReceivedSMS, 2)
	acknowledged := make(chan sipTransactionKey, 2)
	heldRegister := make(chan refreshTestRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveRefreshConcurrency(listener, heldRegister, acknowledged)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	var session *Session
	refreshDone := make(chan error, 1)
	refreshStarted := false
	t.Cleanup(func() {
		cancel() // Unblock AKA before Close, which waits for refresh work.
		if refreshStarted {
			select {
			case <-refreshDone:
			case <-time.After(3 * time.Second):
				t.Error("refresh did not stop during cleanup")
			}
		}
		if session != nil {
			closedCtx, closedCancel := context.WithCancel(context.Background())
			closedCancel() // Cleanup does not require a network deregistration.
			if err := session.Close(closedCtx); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close session: %v", err)
			}
		}
		_ = listener.Close()
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("loopback registrar: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("loopback registrar did not stop")
		}
	})
	provider, err := NewProvider(aka, Config{
		PCSCF: listener.LocalAddr().String(), LocalAddress: "127.0.0.1",
		Transport: "udp", TransactionTimeout: 5 * time.Second, SecurityMode: SecurityDisabled,
		OnSMS: func(_ context.Context, message ReceivedSMS) error {
			// A completed channel write models successful persistence before ACK.
			stored <- message
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := provider.Start(ctx, vowifi.IMSRequest{
		DeviceID: "refresh-loopback",
		Identity: vowifi.SIMIdentity{IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel: evidenceTunnel{evidence: vowifi.TunnelEvidence{
			Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	session = started.(*Session)
	refreshStarted = true
	go func() { refreshDone <- session.refreshOnce(ctx) }()
	select {
	case <-aka.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the new 401 AKA challenge")
	}

	assertDelivery := func(callID string, reference byte) {
		t.Helper()
		remote := session.conn.LocalAddr().(*net.UDPAddr)
		if _, err := listener.WriteToUDP(refreshTestMT(listener.LocalAddr().String(), callID, reference), remote); err != nil {
			t.Fatal(err)
		}
		select {
		case message := <-stored:
			if message.Text != "HELLO" || message.From != "+12345" || message.DecodeError != "" || message.CallID != callID {
				t.Fatalf("persisted invalid MT: %#v", message)
			}
		case <-time.After(time.Second):
			t.Fatal("valid MT persistence callback blocked during refresh")
		}
		var key sipTransactionKey
		select {
		case key = <-acknowledged:
		case <-time.After(time.Second):
			t.Fatal("RP-ACK blocked during refresh after valid MT persistence completed")
		}
		// The server has validated the RP-ACK bytes and returned 200. Wait for
		// exchangeRuntime to consume that response and finish the transaction.
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			session.transactionsMu.Lock()
			_, pending := session.transactions[key]
			session.transactionsMu.Unlock()
			if !pending {
				return
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatal("RP-ACK 200 was not consumed while refresh was blocked")
			}
		}
	}
	assertDelivery("during-aka", 42)
	aka.unblock()
	var pending refreshTestRequest
	select {
	case pending = <-heldRegister:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not send the newly authenticated REGISTER")
	}
	assertDelivery("during-register-response", 43)
	if _, err := listener.WriteToUDP(testResponse(200, "OK", pending.request.value("Call-ID"), pending.request.value("CSeq"), []string{
		"Contact: " + pending.request.value("Contact") + ";expires=600",
	}), pending.remote); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-refreshDone:
		refreshStarted = false
		if err != nil {
			t.Fatalf("refresh after barrier release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not complete after REGISTER 200")
	}
	if !session.Evidence().Registered {
		t.Fatal("successful refresh did not retain registration")
	}
}

func serveRefreshConcurrency(listener *net.UDPConn, held chan<- refreshTestRequest, acknowledged chan<- sipTransactionKey) error {
	buffer := make([]byte, 65535)
	seen := make(map[uint32]string)
	registers := 0
	for {
		count, remote, err := listener.ReadFromUDP(buffer)
		if err != nil {
			return err
		}
		packet, err := parseSIPPacket(buffer[:count])
		if err != nil {
			return err
		}
		if packet.Response != nil {
			if packet.Response.StatusCode != 200 {
				return fmt.Errorf("MT SIP response = %d", packet.Response.StatusCode)
			}
			continue
		}
		request := packet.Request
		cseq, method, err := cseqNumber(request.value("CSeq"))
		if err != nil {
			return err
		}
		if previous, exists := seen[cseq]; exists {
			return fmt.Errorf("CSeq %d reused by %s and %s", cseq, previous, method)
		}
		seen[cseq] = method
		switch method {
		case "REGISTER":
			registers++
			if registers == 4 && held != nil {
				if request.value("Authorization") == "" {
					return errors.New("refresh REGISTER omitted new AKA credentials")
				}
				held <- refreshTestRequest{request: request, remote: remote}
				continue
			}
			status, reason := 200, "OK"
			headers := []string{"Contact: " + request.value("Contact") + ";expires=600"}
			if registers == 1 || registers == 3 {
				status, reason = 401, "Unauthorized"
				nonce := make([]byte, 32)
				nonce[0] = byte(registers)
				headers = []string{`WWW-Authenticate: Digest realm="ims.mnc001.mcc001.3gppnetwork.org", nonce="` + base64.StdEncoding.EncodeToString(nonce) + `", algorithm=AKAv1-MD5, qop="auth"`}
			}
			if _, err := listener.WriteToUDP(testResponse(status, reason, request.value("Call-ID"), request.value("CSeq"), headers), remote); err != nil {
				return err
			}
		case "MESSAGE":
			reference := byte(42)
			if request.value("In-Reply-To") == "during-register-response" {
				reference = 43
			} else if request.value("In-Reply-To") != "during-aka" {
				return fmt.Errorf("unexpected RP-ACK target %q", request.value("In-Reply-To"))
			}
			if !bytes.Equal(request.Body, []byte{0x02, reference}) {
				return fmt.Errorf("RP-ACK body = %x, want 02%02x", request.Body, reference)
			}
			if _, err := listener.WriteToUDP(testResponse(200, "OK", request.value("Call-ID"), request.value("CSeq"), nil), remote); err != nil {
				return err
			}
			acknowledged <- sipTransactionKey{callID: request.value("Call-ID"), cseq: cseq, method: method}
		case "SUBSCRIBE":
			// TS 24.229 5.1.1.3 has the UE subscribe to the reg event package
			// after registering. A real registrar answers 200; these fixtures
			// only care that it never disturbs the registration or RP-ACK work.
			if _, err := listener.WriteToUDP(testResponse(200, "OK", request.value("Call-ID"), request.value("CSeq"), []string{"Expires: 600000"}), remote); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected SIP method %s", method)
		}
	}
}

func refreshTestMT(address, callID string, reference byte) []byte {
	// Valid SMS-DELIVER fixture: +12345, GSM-7 HELLO.
	tpdu := []byte{0x04, 0x05, 0x91, 0x21, 0x43, 0xf5, 0x00, 0x00,
		0x42, 0x10, 0x20, 0x30, 0x40, 0x50, 0x00, 0x05, 0xc8, 0x22, 0x93, 0xf9, 0x04}
	body := append([]byte{0x01, reference, 0x00, 0x00, byte(len(tpdu))}, tpdu...)
	headers := strings.Join([]string{
		"MESSAGE sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org SIP/2.0",
		"Via: SIP/2.0/UDP " + address + ";branch=z9hG4bK" + callID,
		"From: <sip:ipsmgw@example.test>;tag=gw",
		"To: <sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org>",
		"P-Asserted-Identity: <sip:ipsmgw@example.test>",
		"Call-ID: " + callID, "CSeq: 1 MESSAGE",
		"Content-Type: application/vnd.3gpp.sms",
		"Content-Length: " + strconv.Itoa(len(body)), "", "",
	}, "\r\n")
	return append([]byte(headers), body...)
}

func TestRefreshCloseCancelsBackgroundAKA(t *testing.T) {
	fixture := startRefreshCloseFixture(t, false)
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- fixture.session.Close(closeContext) }()
	select {
	case err := <-closed:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel background refresh AKA")
	}
	select {
	case <-fixture.refreshed:
		if fixture.refreshErr == nil {
			t.Error("cancelled refresh reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("background refresh survived Close")
	}
	fixture.assertClosed(t)
}

func TestRefreshCloseBoundsUncooperativeAKAAndRejectsLateResult(t *testing.T) {
	fixture := startRefreshCloseFixture(t, true)
	closeContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closed := make(chan error, 2)
	// Both callers must share teardown, including the same SA handle.
	for i := 0; i < 2; i++ {
		go func() { closed <- fixture.session.Close(closeContext) }()
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Close: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close exceeded its deadline waiting for an AKA provider that ignores cancellation")
		}
	}
	// Check teardown while AKA remains blocked, not after the fake is released.
	fixture.assertClosed(t)
	fixture.aka.unblock()
	select {
	case <-fixture.refreshed:
		if fixture.refreshErr == nil {
			t.Error("late AKA result allowed a closed refresh to succeed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not terminate after late AKA result")
	}
	fixture.assertClosed(t)
}

type refreshCloseHandle struct{ closes atomic.Int32 }

func (handle *refreshCloseHandle) Close(context.Context) error {
	handle.closes.Add(1)
	return nil
}

type refreshCloseFixture struct {
	session    *Session
	aka        *refreshBarrierAKA
	handle     *refreshCloseHandle
	refreshed  chan struct{}
	refreshErr error // Written before refreshed is closed; read only after it.
}

func startRefreshCloseFixture(t *testing.T, ignoreCancel bool, refreshContexts ...context.Context) *refreshCloseFixture {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
	fixture := &refreshCloseFixture{
		aka: &refreshBarrierAKA{
			entered: make(chan struct{}), release: make(chan struct{}), ignoreCancel: ignoreCancel,
		},
		handle: &refreshCloseHandle{}, refreshed: make(chan struct{}),
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- serveRefreshConcurrency(listener, nil, nil) }()
	t.Cleanup(func() {
		// Release the deliberately uncooperative dependency before waiting on
		// failed assertions, so the baseline also has bounded cleanup.
		fixture.aka.unblock()
		if fixture.session != nil {
			select {
			case <-fixture.refreshed:
			case <-time.After(3 * time.Second):
				t.Error("refresh did not terminate during fixture cleanup")
			}
			closedCtx, cancel := context.WithCancel(context.Background())
			cancel()
			closed := make(chan struct{})
			go func() {
				_ = fixture.session.Close(closedCtx)
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Error("session did not terminate during fixture cleanup")
			}
		}
		_ = listener.Close()
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("loopback registrar: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("loopback registrar did not terminate")
		}
	})
	provider, err := NewProvider(fixture.aka, Config{
		PCSCF: listener.LocalAddr().String(), LocalAddress: "127.0.0.1",
		Transport: "udp", TransactionTimeout: time.Second, SecurityMode: SecurityDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	startContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started, err := provider.Start(startContext, vowifi.IMSRequest{
		DeviceID: "refresh-close-loopback",
		Identity: vowifi.SIMIdentity{IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel: evidenceTunnel{evidence: vowifi.TunnelEvidence{
			Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.session = started.(*Session)
	fixture.session.mu.Lock()
	fixture.session.ipsecHandle = fixture.handle
	fixture.session.mu.Unlock()
	refreshContext := context.Background()
	if len(refreshContexts) > 0 {
		refreshContext = refreshContexts[0]
	}
	go func() {
		fixture.refreshErr = fixture.session.refreshOnce(refreshContext)
		close(fixture.refreshed)
	}()
	select {
	case <-fixture.aka.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not reach AKA barrier")
	}
	return fixture
}

func (fixture *refreshCloseFixture) assertClosed(t *testing.T) {
	t.Helper()
	if _, err := fixture.session.conn.Write([]byte("closed-transport-check")); !errors.Is(err, net.ErrClosed) {
		t.Errorf("transport was not closed: %v", err)
	}
	if got := fixture.handle.closes.Load(); got != 1 {
		t.Errorf("SA cleanup count = %d, want exactly 1", got)
	}
	type state struct{ closed, registered, authenticated bool }
	snapshot := make(chan state, 1)
	go func() {
		fixture.session.mu.Lock()
		value := state{fixture.session.closed, fixture.session.evidence.Registered, fixture.session.auth != nil}
		fixture.session.mu.Unlock()
		snapshot <- value
	}()
	select {
	case value := <-snapshot:
		if !value.closed || value.registered || value.authenticated {
			t.Errorf("closed session retained or revived state: closed=%v registered=%v auth_present=%v",
				value.closed, value.registered, value.authenticated)
		}
	case <-time.After(time.Second):
		t.Fatal("closed session state remained locked by AKA")
	}
}

func TestRefreshDeadlineClearsRegistrationAndAuthentication(t *testing.T) {
	refreshContext, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	fixture := startRefreshCloseFixture(t, false, refreshContext)
	// The fixture returns only after the new 401 enters AKA. Let the real
	// context deadline expire without Close or an explicit cancellation.
	select {
	case <-fixture.refreshed:
		if !errors.Is(fixture.refreshErr, context.DeadlineExceeded) {
			t.Fatalf("refresh error = %v, want context deadline exceeded", fixture.refreshErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not stop when its context deadline expired")
	}
	if fixture.session.Evidence().Registered {
		t.Error("timed-out refresh left the session registered and ready")
	}
	fixture.session.mu.Lock()
	defer fixture.session.mu.Unlock()
	if fixture.session.auth != nil {
		t.Error("timed-out refresh retained stale AKA authentication")
	}
	if fixture.session.closed {
		t.Error("refresh deadline unexpectedly closed the session")
	}
}
