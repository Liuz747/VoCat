package ims

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

func TestRuntimeWriteHonorsDeadlineBeforeResponseWait(t *testing.T) {
	for _, blockedByWriter := range []bool{false, true} {
		name := "socket"
		if blockedByWriter {
			name = "writer_lock"
		}
		t.Run(name, func(t *testing.T) {
			client, peer := net.Pipe() // No reader: a real Write waits indefinitely.
			defer client.Close()
			defer peer.Close()
			session := &Session{
				provider: &Provider{config: Config{TransactionTimeout: time.Second}},
				conn:     client, transport: "tcp", transactions: make(map[sipTransactionKey]chan *sipResponse),
			}
			if blockedByWriter {
				session.writeMu.Lock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := session.exchangeRuntime(ctx, []byte("MESSAGE"), sipTransactionKey{callID: name, cseq: 1, method: "MESSAGE"})
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("exchange error = %v, want deadline", err)
				}
				if blockedByWriter {
					session.writeMu.Unlock()
				}
			case <-time.After(300 * time.Millisecond):
				t.Error("runtime transaction ignored deadline before the response timer")
				_ = client.Close()
				if blockedByWriter {
					session.writeMu.Unlock()
				}
				<-done
			}
		})
	}
}

func TestCloseInterruptsBlockedRuntimeWriter(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	provider, err := NewProvider(&recordingAKA{}, Config{SecurityMode: SecurityDisabled})
	if err != nil {
		t.Fatal(err)
	}
	session, err := newSession(provider, vowifi.IMSRequest{}, identitySet{}, pcscfEndpoint{}, "tcp", client)
	if err != nil {
		t.Fatal(err)
	}
	session.runtimeStarted = true
	session.evidence.Registered = true
	session.writeMu.Lock()
	written := make(chan struct{})
	go func() {
		_, _ = client.Write([]byte("stalled SIP writer"))
		session.writeMu.Unlock()
		close(written)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- session.Close(ctx) }()
	select {
	case err := <-closed:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Error(err)
		}
	case <-time.After(time.Second):
		_ = client.Close()
		<-closed
		t.Fatal("Close waited indefinitely behind a blocked SIP writer")
	}
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt the blocked socket Write")
	}
	if session.Evidence().Registered {
		t.Fatal("closed transport retained registration readiness")
	}
}

func TestPartialTCPWriteClosesTransport(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	session := &Session{
		provider: &Provider{config: Config{TransactionTimeout: time.Second}},
		conn:     client, transport: "tcp",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	written := make(chan error, 1)
	go func() { written <- session.writeRuntime(ctx, []byte("MESSAGE sip:partial SIP/2.0\r\n")) }()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(peer, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	// Stop reading in the middle of a SIP frame until the writer times out.
	if err := <-written; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("partial write = %v, want deadline", err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("partially written TCP stream remained reusable: %v", err)
	}
}
