package multisim

import (
	"context"
	"errors"
	"strings"

	"vocat/internal/vowifi"
)

// ErrLineNotReady reports a call operation on a line whose IMS registration
// has not completed; there is no circuit-switched fallback inside a group.
var ErrLineNotReady = errors.New("multisim: the line's IMS registration is not ready")

// LineCall is one IMS call together with the line that carries it, so a
// caller can act on the call without first guessing its line.
type LineCall struct {
	Line LineIdentity
	Call vowifi.Call
}

// LineCalls lists the calls of every line of an owned group.
func (m *Manager) LineCalls(deviceID string) ([]LineCall, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return nil, ErrNotRegistered
	}
	type snapshot struct {
		orchestrator *vowifi.Orchestrator
		identity     LineIdentity
	}
	lines := make([]snapshot, 0, len(g.lines))
	for _, item := range g.lines {
		if item.orchestrator == nil {
			continue
		}
		lines = append(lines, snapshot{item.orchestrator, LineIdentity{DeviceID: deviceID, SessionID: item.sessionID, ICCID: item.profile.ICCID}})
	}
	g.actions.Add(1)
	m.mu.Unlock()
	defer g.actions.Done()

	out := make([]LineCall, 0)
	for _, line := range lines {
		calls, err := line.orchestrator.Calls()
		if err != nil {
			// A line that is not registered simply has no calls.
			continue
		}
		identity := fillLineIdentity(line.identity, line.orchestrator)
		for _, call := range calls {
			out = append(out, LineCall{Line: identity, Call: call})
		}
	}
	return out, nil
}

// DialCall places a call through one line of an owned group. The selector
// names the line by ICCID or session ID exactly like SendSMS; an empty
// selector is accepted only for a one-line group.
func (m *Manager) DialCall(ctx context.Context, deviceID, selector, number string) (vowifi.Call, LineIdentity, error) {
	orchestrator, identity, release, err := m.acquireLine(deviceID, selector, "")
	if err != nil {
		return vowifi.Call{}, LineIdentity{}, err
	}
	defer release()
	if !orchestrator.State().IMSReady {
		return vowifi.Call{}, fillLineIdentity(identity, orchestrator), ErrLineNotReady
	}
	call, err := orchestrator.DialCall(ctx, number)
	return call, fillLineIdentity(identity, orchestrator), err
}

// AnswerCall answers a ringing call. With an empty selector the line is found
// by the call ID, which is what an incoming call naturally provides.
func (m *Manager) AnswerCall(ctx context.Context, deviceID, selector, callID string) (vowifi.Call, LineIdentity, error) {
	orchestrator, identity, release, err := m.acquireLine(deviceID, selector, callID)
	if err != nil {
		return vowifi.Call{}, LineIdentity{}, err
	}
	defer release()
	call, err := orchestrator.AnswerCall(ctx, callID)
	return call, fillLineIdentity(identity, orchestrator), err
}

// HangupCall ends a call; the line is resolved like AnswerCall.
func (m *Manager) HangupCall(ctx context.Context, deviceID, selector, callID string) (LineIdentity, error) {
	orchestrator, identity, release, err := m.acquireLine(deviceID, selector, callID)
	if err != nil {
		return LineIdentity{}, err
	}
	defer release()
	return fillLineIdentity(identity, orchestrator), orchestrator.HangupCall(ctx, callID)
}

// CallMedia exposes the RTP bridge of one call; the line is resolved like
// AnswerCall.
func (m *Manager) CallMedia(ctx context.Context, deviceID, selector, callID string) (vowifi.CallMedia, LineIdentity, error) {
	orchestrator, identity, release, err := m.acquireLine(deviceID, selector, callID)
	if err != nil {
		return nil, LineIdentity{}, err
	}
	defer release()
	media, err := orchestrator.CallMedia(ctx, callID)
	return media, fillLineIdentity(identity, orchestrator), err
}

// acquireLine resolves one line of a running group and holds the group's
// action counter until release is called, mirroring SendSMS. Resolution order:
// explicit selector (ICCID or session ID), then the line that owns callID,
// then the only line of a one-line group.
func (m *Manager) acquireLine(deviceID, selector, callID string) (*vowifi.Orchestrator, LineIdentity, func(), error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, LineIdentity{}, nil, ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return nil, LineIdentity{}, nil, ErrNotRegistered
	}
	if g.state.Busy || g.state.Phase != "running" {
		m.mu.Unlock()
		return nil, LineIdentity{}, nil, ErrOperationInProgress
	}
	selector = strings.TrimSpace(selector)
	callID = strings.TrimSpace(callID)
	var item *line
	switch {
	case selector != "":
		for _, candidate := range g.lines {
			if candidate.profile.ICCID == selector || candidate.sessionID == selector {
				item = candidate
				break
			}
		}
	case callID != "":
		item = lineOwningCall(g.lines, callID)
	case len(g.lines) == 1:
		item = g.lines[0]
	default:
		m.mu.Unlock()
		return nil, LineIdentity{}, nil, ErrLineRequired
	}
	if item == nil || item.orchestrator == nil {
		m.mu.Unlock()
		return nil, LineIdentity{}, nil, ErrNotRegistered
	}
	orchestrator := item.orchestrator
	identity := LineIdentity{DeviceID: deviceID, SessionID: item.sessionID, ICCID: item.profile.ICCID}
	g.actions.Add(1)
	m.mu.Unlock()
	return orchestrator, identity, g.actions.Done, nil
}

// lineOwningCall scans the lines' in-memory call tables. Calls() is a
// registered session's own bookkeeping and never touches the card, so it is
// safe to consult while the group lock is held.
func lineOwningCall(lines []*line, callID string) *line {
	for _, candidate := range lines {
		if candidate.orchestrator == nil {
			continue
		}
		calls, err := candidate.orchestrator.Calls()
		if err != nil {
			continue
		}
		for _, call := range calls {
			if call.ID == callID {
				return candidate
			}
		}
	}
	return nil
}

func fillLineIdentity(identity LineIdentity, orchestrator *vowifi.Orchestrator) LineIdentity {
	state := orchestrator.State()
	identity.IMSI = state.IMSI
	identity.PhoneNumber = state.PhoneNumber
	return identity
}
