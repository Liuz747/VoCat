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

type registrationRefresher interface {
	RefreshRegistration(context.Context) error
}

func TestRefreshRegistrationReusesSessionWithoutForcingAKA(t *testing.T) {
	session, aka := registrationMaintenanceFixture(t, false)
	refresh, ok := any(session).(registrationRefresher)
	if !ok {
		t.Fatal("IMS session does not expose RefreshRegistration")
	}
	connection := session.conn
	expiry := session.provider.config.RegistrationExpiry
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := refresh.RefreshRegistration(ctx); err != nil {
		t.Fatal(err)
	}
	if session.conn != connection || session.provider.config.RegistrationExpiry != expiry {
		t.Fatal("maintenance refresh changed transport or configured expiry")
	}
	if !session.Evidence().Registered || len(aka.challenges) != 1 {
		t.Fatal("refresh lost registration or forced an additional AKA")
	}
	select {
	case err := <-session.Failures():
		t.Fatalf("successful refresh published failure: %v", err)
	default:
	}
}

func TestRefreshRegistrationDeadlinePublishesFailure(t *testing.T) {
	session, _ := registrationMaintenanceFixture(t, true)
	refresh, ok := any(session).(registrationRefresher)
	if !ok {
		t.Fatal("IMS session does not expose RefreshRegistration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := refresh.RefreshRegistration(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh = %v, want caller deadline", err)
	}
	if session.Evidence().Registered {
		t.Fatal("failed maintenance refresh retained readiness")
	}
	select {
	case err := <-session.Failures():
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("failure notification = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed maintenance refresh did not reach the failure notifier")
	}
}

func TestRefreshRegistrationGateDeadlineDoesNotRevokeReady(t *testing.T) {
	session, aka := registrationMaintenanceFixture(t, false)
	refresh, ok := any(session).(registrationRefresher)
	if !ok {
		t.Fatal("IMS session does not expose RefreshRegistration")
	}
	session.registrationGate <- struct{}{}
	defer func() { <-session.registrationGate }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := refresh.RefreshRegistration(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registration gate ignored deadline: %v", err)
	}
	if !session.Evidence().Registered || len(aka.challenges) != 1 {
		t.Fatal("a request canceled before execution changed registration")
	}
	select {
	case err := <-session.Failures():
		t.Fatalf("unexecuted refresh falsely reported line failure: %v", err)
	default:
	}
}

func registrationMaintenanceFixture(t *testing.T, dropRefresh bool) (*Session, *recordingAKA) {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 65535)
		var initialExpiry string
		for n := 1; ; n++ {
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
			if n == 1 {
				initialExpiry = request.value("Expires")
			} else if request.value("Expires") != initialExpiry {
				serverDone <- errors.New("maintenance changed requested registration expiry")
				return
			}
			if n >= 3 && dropRefresh {
				continue
			}
			status, reason := 200, "OK"
			headers := []string{"Contact: " + request.value("Contact") + ";expires=600"}
			if n == 1 {
				status, reason = 401, "Unauthorized"
				headers = []string{`WWW-Authenticate: Digest realm="ims.mnc001.mcc001.3gppnetwork.org", nonce="` + base64.StdEncoding.EncodeToString(make([]byte, 32)) + `", algorithm=AKAv1-MD5, qop="auth"`}
			}
			if _, err := listener.WriteToUDP(testResponse(status, reason, request.value("Call-ID"), request.value("CSeq"), headers), remote); err != nil {
				serverDone <- err
				return
			}
		}
	}()
	aka := &recordingAKA{result: vowifi.AKAResult{RES: []byte{1, 2, 3, 4}}}
	provider, err := NewProvider(aka, Config{
		PCSCF: listener.LocalAddr().String(), LocalAddress: "127.0.0.1", Transport: "udp",
		TransactionTimeout: time.Second, SecurityMode: SecurityDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	var session *Session
	t.Cleanup(func() {
		if session != nil {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = session.Close(ctx)
		}
		_ = listener.Close()
		if err := <-serverDone; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started, err := provider.Start(ctx, vowifi.IMSRequest{
		Identity: vowifi.SIMIdentity{IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel:   evidenceTunnel{evidence: vowifi.TunnelEvidence{Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	session = started.(*Session)
	return session, aka
}
