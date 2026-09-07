package ike

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

func livenessKeys() ikeKeys {
	return ikeKeys{SKai: bytes.Repeat([]byte{1}, 20), SKar: bytes.Repeat([]byte{2}, 20), SKei: bytes.Repeat([]byte{3}, 16), SKer: bytes.Repeat([]byte{4}, 16)}
}
func peerPacket(t *testing.T, exchange uint8, id uint32, flags uint8, p []payload) []byte {
	t.Helper()
	k := livenessKeys()
	b, e := encryptPayloads(ikeHeader{InitiatorSPI: [8]byte{1}, ResponderSPI: [8]byte{2}, Exchange: exchange, MessageID: id, Flags: flags}, p, legacyTestSuite(), k.SKer, k.SKar, nil)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestUnauthenticatedIKECannotTerminateSession(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour)
	defer r.Close()
	bad := peerPacket(t, 36, 0, 0, nil)
	bad[len(bad)-1] ^= 1
	tr.incoming <- fakeSessionPacket{data: bad, ike: true}
	tr.incoming <- fakeSessionPacket{data: peerPacket(t, exchangeInformational, 0, 0, nil), ike: true}
	select {
	case p := <-tr.sent:
		if !p.ike {
			t.Fatal("not IKE")
		}
	case <-time.After(time.Second):
		t.Fatal("forged packet killed the relay")
	}
}
func TestSessionNotifiesRelayFailureWithoutDataplaneNotifier(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour)
	defer r.Close()
	s := &Session{relay: r, evidence: vowifi.TunnelEvidence{Established: true}}
	defer s.Close(context.Background())
	failures := s.Failures()
	r.fail(errors.New("injected transport failure"))
	select {
	case e := <-failures:
		if e == nil {
			t.Fatal("nil terminal error")
		}
	case <-time.After(time.Second):
		t.Fatal("relay failure was silent")
	}
	if s.Evidence().Established {
		t.Fatal("terminal session still established")
	}
}
func TestPeerDeleteIsAcknowledgedBeforeTerminal(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour)
	defer r.Close()
	tr.incoming <- fakeSessionPacket{data: peerPacket(t, exchangeInformational, 0, 0, []payload{{Type: payloadDelete, Body: []byte{protocolIKE, 0, 0, 0}}}), ike: true}
	select {
	case p := <-tr.sent:
		k := livenessKeys()
		h, ps, e := decryptPayloads(p.data, legacyTestSuite(), k.SKei, k.SKai)
		if e != nil || h.Flags != flagInitiator|flagResponse || len(ps) != 0 {
			t.Fatalf("invalid delete ack: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("peer DELETE not acknowledged")
	}
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("DELETE not terminal")
	}
}

func TestActiveDPDBlackholeRetransmitsIdenticalRequestThenFails(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour, relayOptions{DPDInterval: 10 * time.Millisecond, DPDTimeout: 10 * time.Millisecond, DPDAttempts: 3})
	defer r.Close()
	var first []byte
	for n := 0; n < 3; n++ {
		select {
		case p := <-tr.sent:
			if !p.ike {
				t.Fatal("NAT keepalive is not DPD")
			}
			if first == nil {
				first = p.data
			} else if !bytes.Equal(first, p.data) {
				t.Fatal("DPD retransmission changed bytes")
			}
			k := livenessKeys()
			h, ps, e := decryptPayloads(p.data, legacyTestSuite(), k.SKei, k.SKai)
			if e != nil || h.MessageID != 9 || h.Flags != flagInitiator || len(ps) != 0 {
				t.Fatalf("bad DPD: %+v %v", h, e)
			}
		case <-time.After(time.Second):
			t.Fatal("missing DPD")
		}
	}
	select {
	case <-r.done:
		if !errors.Is(r.terminalError(), ErrDPDTimeout) {
			t.Fatalf("wrong failure: %v", r.terminalError())
		}
	case <-time.After(time.Second):
		t.Fatal("blackhole not detected")
	}
}
func TestActiveDPDRequiresAuthenticatedMatchingResponse(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour, relayOptions{DPDInterval: 10 * time.Millisecond, DPDTimeout: 40 * time.Millisecond, DPDAttempts: 3})
	defer r.Close()
	first := <-tr.sent
	bad := peerPacket(t, exchangeInformational, 9, flagResponse, nil)
	bad[len(bad)-1] ^= 1
	tr.incoming <- fakeSessionPacket{data: bad, ike: true}
	tr.incoming <- fakeSessionPacket{data: peerPacket(t, exchangeInformational, 8, flagResponse, nil), ike: true}
	select {
	case retry := <-tr.sent:
		if !bytes.Equal(first.data, retry.data) {
			t.Fatal("unverified response advanced request ID")
		}
	case <-time.After(time.Second):
		t.Fatal("missing retry")
	}
	tr.incoming <- fakeSessionPacket{data: peerPacket(t, exchangeInformational, 9, flagResponse, nil), ike: true}
	select {
	case next := <-tr.sent:
		k := livenessKeys()
		h, _, e := decryptPayloads(next.data, legacyTestSuite(), k.SKei, k.SKai)
		if e != nil || h.MessageID != 10 {
			t.Fatalf("request ID not advanced after verified response: %+v %v", h, e)
		}
	case <-time.After(time.Second):
		t.Fatal("no next DPD")
	}
}
func TestPeerDPDRetransmissionReturnsCachedBytes(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour)
	defer r.Close()
	request := peerPacket(t, exchangeInformational, 0, 0, nil)
	tr.incoming <- fakeSessionPacket{data: request, ike: true}
	first := <-tr.sent
	tr.incoming <- fakeSessionPacket{data: request, ike: true}
	select {
	case next := <-tr.sent:
		if !bytes.Equal(first.data, next.data) {
			t.Fatal("duplicate request re-encrypted a different response")
		}
	case <-time.After(time.Second):
		t.Fatal("missing cached response")
	}
}

func TestESPDeleteOnlyTerminatesTheCurrentChild(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour, relayOptions{ChildInboundSPI: 0x01020304, ChildOutboundSPI: 0x05060708})
	defer r.Close()
	for _, tc := range []struct {
		id       uint32
		spi      []byte
		terminal bool
	}{{0, []byte{9, 9, 9, 9}, false}, {1, []byte{5, 6, 7, 8}, true}} {
		body := append([]byte{protocolESP, 4, 0, 1}, tc.spi...)
		tr.incoming <- fakeSessionPacket{data: peerPacket(t, exchangeInformational, tc.id, 0, []payload{{Type: payloadDelete, Body: body}}), ike: true}
		select {
		case p := <-tr.sent:
			k := livenessKeys()
			_, ps, e := decryptPayloads(p.data, legacyTestSuite(), k.SKei, k.SKai)
			if e != nil {
				t.Fatal(e)
			}
			if tc.terminal {
				if len(ps) != 1 || !bytes.Equal(ps[0].Body, []byte{protocolESP, 4, 0, 1, 1, 2, 3, 4}) {
					t.Fatal("DELETE ACK did not map the opposite direction SPI")
				}
			} else if len(ps) != 0 {
				t.Fatal("unknown SPI must not delete active pair")
			}
		case <-time.After(time.Second):
			t.Fatal("missing delete ack")
		}
		if !tc.terminal {
			select {
			case <-r.done:
				t.Fatal("unknown child delete killed active session")
			default:
			}
		}
	}
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("active child deletion not terminal")
	}
}

func TestDeadSessionCloseDoesNotStartAnotherIKETransaction(t *testing.T) {
	tr := newFakeSessionTransport()
	r := newSessionRelay(tr, legacyTestSuite(), livenessKeys(), [8]byte{1}, [8]byte{2}, 9, true, time.Hour)
	r.fail(ErrPeerDeleted)
	<-r.done
	_ = r.CloseWithDelete(context.Background())
	select {
	case <-tr.sent:
		t.Fatal("sent DELETE after the peer already deleted this SA")
	default:
	}
}
