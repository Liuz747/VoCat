package ike

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// The first write can ignore context cancellation and wait for either an
// explicit test release or socket Close, like a transport blocked in Write.
type closeGateTransport struct {
	*fakeSessionTransport
	blockFirst bool
	entered    chan struct{}
	release    chan struct{}
	calls      atomic.Int32
}

func newCloseGateTransport(block bool) *closeGateTransport {
	return &closeGateTransport{fakeSessionTransport: newFakeSessionTransport(), blockFirst: block, entered: make(chan struct{}), release: make(chan struct{})}
}
func (transport *closeGateTransport) SendSessionPacket(ctx context.Context, packet []byte, ike bool) error {
	call := transport.calls.Add(1)
	if call == 1 && transport.blockFirst {
		close(transport.entered)
		select {
		case <-transport.release:
		case <-transport.closed:
			return net.ErrClosed
		}
	}
	return transport.fakeSessionTransport.SendSessionPacket(ctx, packet, ike)
}
func closeTestRelay(transport *closeGateTransport) *sessionRelay {
	return newSessionRelay(transport, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour, relayOptions{DPDInterval: time.Hour, DPDTimeout: time.Second, DPDAttempts: 3})
}
func waitCloseResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("relay close operation blocked")
		return nil
	}
}
func TestDeleteClosingPreventsQueuedDPD(t *testing.T) {
	transport := newCloseGateTransport(true)
	relay := closeTestRelay(transport)
	defer relay.Close()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- relay.sendIKEDelete(context.Background()) }()
	<-transport.entered // DELETE holds mu across Send.
	tickStarted := make(chan struct{})
	tickDone := make(chan error, 1)
	go func() { close(tickStarted); tickDone <- relay.dpdTick(time.Now().Add(2 * time.Hour)) }()
	<-tickStarted
	close(transport.release)
	if err := waitCloseResult(t, deleteDone); err != nil {
		t.Fatal(err)
	}
	if err := waitCloseResult(t, tickDone); err != nil {
		t.Fatal(err)
	}
	// Exercise the seam after sendIKEDelete unlocks and before CloseWithDelete
	// invokes Close; no scheduler timing is needed to make that seam observable.
	if calls := transport.calls.Load(); calls != 1 {
		t.Fatalf("DELETE followed by another IKE request: %d sends", calls)
	}
	relay.mu.Lock()
	id, pending := relay.deleteID, relay.pending
	relay.mu.Unlock()
	if id != 10 || pending != nil {
		t.Fatalf("queued DPD advanced state: ID=%d pending=%v", id, pending != nil)
	}
}
func TestCloseWithDeleteDoesNotSendSecondDelete(t *testing.T) {
	transport := newCloseGateTransport(false)
	relay := closeTestRelay(transport)
	_ = relay.CloseWithDelete(context.Background())
	_ = relay.CloseWithDelete(context.Background())
	if calls := transport.calls.Load(); calls != 1 {
		t.Fatalf("repeated close sent %d IKE requests", calls)
	}
}
func TestDeleteWithPendingDPDStopsRetransmission(t *testing.T) {
	transport := newCloseGateTransport(false)
	relay := closeTestRelay(transport)
	defer relay.Close()
	now := time.Now().Add(2 * time.Hour)
	if err := relay.dpdTick(now); err != nil {
		t.Fatal(err)
	}
	if err := relay.sendIKEDelete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := relay.dpdTick(now.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if calls := transport.calls.Load(); calls != 1 {
		t.Fatalf("closing with pending DPD sent DELETE or retry: %d sends", calls)
	}
}
func TestPlainCloseSuppressesNewDPDAndRetransmission(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "new"
		if pending {
			name = "retransmission"
		}
		t.Run(name, func(t *testing.T) {
			transport := newCloseGateTransport(false)
			relay := closeTestRelay(transport)
			now := time.Now().Add(2 * time.Hour)
			if pending {
				if err := relay.dpdTick(now); err != nil {
					t.Fatal(err)
				}
			}
			_ = relay.Close()
			before := transport.calls.Load()
			if err := relay.dpdTick(now.Add(2 * time.Second)); err != nil {
				t.Fatalf("closed relay still attempted DPD: %v", err)
			}
			if transport.calls.Load() != before {
				t.Fatal("plain Close allowed another DPD send")
			}
		})
	}
}
func TestPlainCloseUnblocksDPDOrDeleteHoldingMutexInSend(t *testing.T) {
	for _, operation := range []string{"delete", "dpd"} {
		t.Run(operation, func(t *testing.T) {
			transport := newCloseGateTransport(true)
			relay := closeTestRelay(transport)
			defer func() { close(transport.release); _ = relay.Close() }()
			sendDone := make(chan error, 1)
			go func() {
				if operation == "delete" {
					sendDone <- relay.sendIKEDelete(context.Background())
				} else {
					sendDone <- relay.dpdTick(time.Now().Add(2 * time.Hour))
				}
			}()
			<-transport.entered
			closed := make(chan error, 1)
			go func() { closed <- relay.Close() }()
			_ = waitCloseResult(t, closed)
			_ = waitCloseResult(t, sendDone)
		})
	}
}
