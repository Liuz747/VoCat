package ims

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

// Closing a line must never enqueue fresh physical AKA work merely because
// the registrar challenges the best-effort de-registration.
func TestCloseDoesNotStartFreshAKA(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	aka := &refreshBarrierAKA{entered: make(chan struct{}), release: make(chan struct{}), ignoreCancel: true}
	serverDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 65535)
		for n := 0; ; n++ {
			count, remote, err := listener.ReadFromUDP(buffer)
			if err != nil {
				serverDone <- err
				return
			}
			packet, err := parseSIPPacket(buffer[:count])
			if err != nil || packet.Request == nil {
				serverDone <- errors.New("invalid REGISTER")
				return
			}
			request := packet.Request
			status, reason := 401, "Unauthorized"
			headers := []string{`WWW-Authenticate: Digest realm="ims.mnc001.mcc001.3gppnetwork.org", nonce="` + base64.StdEncoding.EncodeToString(make([]byte, 32)) + `", algorithm=AKAv1-MD5, qop="auth"`}
			if n == 1 {
				status, reason = 200, "OK"
				headers = []string{"Contact: " + request.value("Contact") + ";expires=600"}
			}
			if _, err := listener.WriteToUDP(testResponse(status, reason, request.value("Call-ID"), request.value("CSeq"), headers), remote); err != nil {
				serverDone <- err
				return
			}
		}
	}()
	t.Cleanup(func() {
		aka.unblock()
		_ = listener.Close()
		if err := <-serverDone; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	provider, err := NewProvider(aka, Config{
		PCSCF: listener.LocalAddr().String(), LocalAddress: "127.0.0.1", Transport: "udp",
		TransactionTimeout: time.Second, SecurityMode: SecurityDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := provider.Start(context.Background(), vowifi.IMSRequest{
		Identity: vowifi.SIMIdentity{IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel:   evidenceTunnel{evidence: vowifi.TunnelEvidence{Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- session.Close(ctx) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Error("Close started a fresh AKA that outlived its deadline")
		aka.unblock()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close did not finish after releasing test AKA")
		}
	}
	if got := aka.calls.Load(); got != 1 {
		t.Errorf("AKA calls = %d; only initial registration may authenticate", got)
	}
	if evidence := session.Evidence(); evidence.Registered {
		t.Error("closed session still registered")
	}
}
