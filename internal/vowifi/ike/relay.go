package ike

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"
)

type relayOptions struct {
	DPDInterval      time.Duration
	DPDTimeout       time.Duration
	DPDAttempts      int
	ChildInboundSPI  uint32
	ChildOutboundSPI uint32
}
type dpdRequest struct {
	packet   []byte
	id       uint32
	sent     time.Time
	attempts int
}

var ErrDPDTimeout = errors.New("ike: authenticated DPD response deadline exceeded")

type sessionRelay struct {
	transport        datagramTransport
	suite            negotiatedSuite
	keys             ikeKeys
	spii             [8]byte
	spir             [8]byte
	deleteID         uint32
	childInboundSPI  uint32
	childOutboundSPI uint32
	natt             bool
	keepalive        time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	esp    chan []byte

	mu           sync.Mutex
	lastErr      error
	options      relayOptions
	nextProbe    time.Time
	pending      *dpdRequest
	peerSeen     bool
	peerID       uint32
	peerRequest  []byte
	peerResponse []byte
}

func newSessionRelay(
	transport datagramTransport,
	suite negotiatedSuite,
	keys ikeKeys,
	initiatorSPI [8]byte,
	responderSPI [8]byte,
	deleteMessageID uint32,
	natt bool,
	keepalive time.Duration,
	options ...relayOptions,
) *sessionRelay {
	if keepalive <= 0 {
		keepalive = 15 * time.Second
	}
	var o relayOptions
	if len(options) > 0 {
		o = options[0]
	}
	if o.DPDInterval <= 0 {
		o.DPDInterval = 30 * time.Second
	}
	if o.DPDTimeout <= 0 {
		o.DPDTimeout = 5 * time.Second
	}
	if o.DPDAttempts <= 0 {
		o.DPDAttempts = 3
	}
	ctx, cancel := context.WithCancel(context.Background())
	relay := &sessionRelay{
		options:          o,
		nextProbe:        time.Now().Add(o.DPDInterval),
		childInboundSPI:  o.ChildInboundSPI,
		childOutboundSPI: o.ChildOutboundSPI,
		transport:        transport,
		suite:            suite,
		keys:             keys,
		spii:             initiatorSPI,
		spir:             responderSPI,
		deleteID:         deleteMessageID,
		natt:             natt,
		keepalive:        keepalive,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		esp:              make(chan []byte, 64),
	}
	go relay.run()
	return relay
}

func (relay *sessionRelay) run() {
	defer close(relay.done)
	defer close(relay.esp)
	buffer := make([]byte, 65535)
	lastKeepalive := time.Now()
	for {
		if err := relay.ctx.Err(); err != nil {
			return
		}
		if err := relay.dpdTick(time.Now()); err != nil {
			relay.fail(err)
			return
		}
		n, isIKE, err := relay.transport.ReceiveSessionPacket(relay.ctx, buffer)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				return
			}
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				if relay.natt && time.Since(lastKeepalive) >= relay.keepalive {
					if sendErr := relay.transport.SendSessionPacket(relay.ctx, []byte{0xff}, false); sendErr != nil {
						relay.fail(sendErr)
						return
					}
					lastKeepalive = time.Now()
				}
				continue
			}
			relay.fail(err)
			return
		}
		packet := append([]byte(nil), buffer[:n]...)
		if isIKE {
			if err := relay.handleIKE(packet); err != nil {
				if errors.Is(err, errMismatchedSessionSPIs) {
					// A reconnect can reuse the same NAT mapping while the ePDG still
					// has packets queued for the previous IKE SA. Those packets are
					// unrelated to this authenticated session and must be discarded;
					// treating one as fatal tears down the newly established CHILD_SA.
					continue
				}
				relay.fail(err)
				return
			}
			continue
		}
		if len(packet) == 1 && packet[0] == 0xff {
			// Peer NAT keepalive.
			continue
		}
		if len(packet) < 8 {
			// Unauthenticated network input must not tear down the session.
			continue
		}
		select {
		case relay.esp <- packet:
		default:
			// Keep the sole socket reader available for IKE/DPD if the
			// data-plane consumer falls behind.
		case <-relay.ctx.Done():
			return
		}
	}
}

var errMismatchedSessionSPIs = errors.New("ike: session packet has mismatched SPIs")

var ErrPeerDeleted = errors.New("ike: peer deleted the active SA")
var ErrRekeyUnsupported = errors.New("ike: peer requested unsupported rekey; session recovery required")

func (relay *sessionRelay) handleIKE(packet []byte) error {
	header, _, err := parseIKEPacket(packet)
	if err != nil {
		return nil
	} // Unauthenticated input cannot end a live session.
	if header.InitiatorSPI != relay.spii || header.ResponderSPI != relay.spir {
		return errMismatchedSessionSPIs
	}
	if header.Flags&flagInitiator != 0 {
		return nil
	}
	_, payloads, err := decryptPayloads(packet, relay.suite, relay.keys.SKer, relay.keys.SKar)
	if err != nil {
		return nil
	}
	if header.Flags&flagResponse != 0 {
		relay.mu.Lock()
		if p := relay.pending; p != nil && header.Exchange == exchangeInformational && header.MessageID == p.id && len(payloads) == 0 {
			relay.pending = nil
			relay.nextProbe = time.Now().Add(relay.options.DPDInterval)
		}
		relay.mu.Unlock()
		return nil
	}
	// One receive window and one cached response; retransmissions never repeat
	// side effects or change the encrypted response bytes.
	if relay.peerSeen {
		if header.MessageID == relay.peerID {
			if bytes.Equal(packet, relay.peerRequest) {
				return relay.transport.SendSessionPacket(relay.ctx, relay.peerResponse, true)
			}
			return nil
		}
		if relay.peerID == math.MaxUint32 || header.MessageID != relay.peerID+1 {
			return nil
		}
	}
	var reply []payload
	var terminal error
	switch header.Exchange {
	case exchangeInformational:
		for _, item := range payloads {
			if item.Type != payloadDelete {
				continue
			}
			if len(item.Body) < 4 {
				return nil
			}
			protocol, size, count := item.Body[0], int(item.Body[1]), int(binary.BigEndian.Uint16(item.Body[2:4]))
			if len(item.Body) != 4+size*count {
				return nil
			}
			if protocol == protocolIKE && size == 0 && count == 0 {
				terminal = ErrPeerDeleted
			} else if protocol == protocolESP && size == 4 {
				for off := 4; off < len(item.Body); off += 4 {
					if relay.childOutboundSPI != 0 && binary.BigEndian.Uint32(item.Body[off:off+4]) == relay.childOutboundSPI {
						body := []byte{protocolESP, 4, 0, 1}
						body = binary.BigEndian.AppendUint32(body, relay.childInboundSPI)
						reply = append(reply, payload{Type: payloadDelete, Body: body})
						terminal = ErrPeerDeleted
						break
					}
				}
			}
		}
	case 36: // CREATE_CHILD_SA: explicit rejection, followed by scoped recovery.
		reply = []payload{makeNotify(35, nil)} // NO_ADDITIONAL_SAS
		terminal = ErrRekeyUnsupported
	default:
		return nil
	}
	response, err := encryptPayloads(ikeHeader{InitiatorSPI: relay.spii, ResponderSPI: relay.spir, Exchange: header.Exchange, Flags: flagInitiator | flagResponse, MessageID: header.MessageID}, reply, relay.suite, relay.keys.SKei, relay.keys.SKai, nil)
	if err != nil {
		return err
	}
	relay.peerSeen = true
	relay.peerID = header.MessageID
	relay.peerRequest = append([]byte(nil), packet...)
	relay.peerResponse = append([]byte(nil), response...)
	if err = relay.transport.SendSessionPacket(relay.ctx, response, true); err != nil {
		return err
	}
	return terminal
}

func (relay *sessionRelay) fail(err error) {
	relay.mu.Lock()
	if relay.lastErr == nil {
		relay.lastErr = err
	}
	relay.mu.Unlock()
	relay.cancel()
}

func (relay *sessionRelay) SendESP(ctx context.Context, packet []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-relay.done:
		return relay.terminalError()
	default:
	}
	return relay.transport.SendSessionPacket(ctx, packet, false)
}

func (relay *sessionRelay) ReceiveESP(ctx context.Context, buffer []byte) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case packet, ok := <-relay.esp:
		if !ok {
			return 0, relay.terminalError()
		}
		if len(packet) > len(buffer) {
			return 0, errors.New("ike: ESP receive buffer is too small")
		}
		copy(buffer, packet)
		return len(packet), nil
	}
}

func (relay *sessionRelay) terminalError() error {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if relay.lastErr != nil {
		return relay.lastErr
	}
	return net.ErrClosed
}

func (relay *sessionRelay) Close() error {
	relay.cancel()
	// ReceiveSessionPacket implementations normally observe the canceled
	// context through a short read deadline. Close the transport as an explicit
	// wake-up as well: a socket implementation that is stuck in Read must not
	// hold teardown (and the associated TUN interface) indefinitely.
	transportErr := relay.transport.Close()
	<-relay.done
	return errors.Join(relay.terminalErrorIfFailure(), transportErr)
}

func (relay *sessionRelay) CloseWithDelete(ctx context.Context) error {
	deleteErr := relay.sendIKEDelete(ctx)
	return errors.Join(deleteErr, relay.Close())
}

func (relay *sessionRelay) sendIKEDelete(ctx context.Context) error {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	// An already terminal SA cannot acknowledge another transaction.
	if relay.lastErr != nil {
		return nil
	}
	// Window size is one. Do not send a new transaction after an unanswered DPD.
	if relay.pending != nil {
		return nil
	}
	id := relay.deleteID
	if id == math.MaxUint32 {
		return errors.New("ike: message ID exhausted")
	}
	relay.deleteID++
	return sendIKESADelete(ctx, relay.transport, relay.suite, relay.keys, relay.spii, relay.spir, id)
}

func sendIKESADelete(
	ctx context.Context,
	transport datagramTransport,
	suite negotiatedSuite,
	keys ikeKeys,
	initiatorSPI [8]byte,
	responderSPI [8]byte,
	messageID uint32,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		// Teardown is often called with the operation context already canceled.
		// Give the protocol-level release a short independent chance to leave.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
	}
	request, err := encryptPayloads(ikeHeader{
		InitiatorSPI: initiatorSPI,
		ResponderSPI: responderSPI,
		Exchange:     exchangeInformational,
		Flags:        flagInitiator,
		MessageID:    messageID,
	}, []payload{{
		Type: payloadDelete,
		Body: []byte{protocolIKE, 0, 0, 0},
	}}, suite, keys.SKei, keys.SKai, nil)
	if err != nil {
		return fmt.Errorf("ike: build IKE SA delete: %w", err)
	}
	if err := transport.SendSessionPacket(ctx, request, true); err != nil {
		return fmt.Errorf("ike: send IKE SA delete: %w", err)
	}
	return nil
}

func (relay *sessionRelay) terminalErrorIfFailure() error {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	return relay.lastErr
}

var _ NATTPacketRelay = (*sessionRelay)(nil)

// dpdTick runs on the sole packet-reader goroutine, including while ESP flows.
// NAT keepalives and unverified ESP traffic are never proof of peer liveness.
func (relay *sessionRelay) dpdTick(now time.Time) error {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if relay.pending != nil {
		p := relay.pending
		if now.Sub(p.sent) < relay.options.DPDTimeout {
			return nil
		}
		if p.attempts >= relay.options.DPDAttempts {
			return ErrDPDTimeout
		}
		if err := relay.transport.SendSessionPacket(relay.ctx, p.packet, true); err != nil {
			return err
		}
		p.sent = now
		p.attempts++
		return nil
	}
	if now.Before(relay.nextProbe) {
		return nil
	}
	if relay.deleteID == math.MaxUint32 {
		return errors.New("ike: message ID exhausted")
	}
	id := relay.deleteID
	request, err := encryptPayloads(ikeHeader{InitiatorSPI: relay.spii, ResponderSPI: relay.spir, Exchange: exchangeInformational, Flags: flagInitiator, MessageID: id}, nil, relay.suite, relay.keys.SKei, relay.keys.SKai, nil)
	if err != nil {
		return err
	}
	if err = relay.transport.SendSessionPacket(relay.ctx, request, true); err != nil {
		return err
	}
	relay.deleteID++
	relay.pending = &dpdRequest{packet: request, id: id, sent: now, attempts: 1}
	return nil
}
