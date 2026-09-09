package multisim

import (
	"context"
	"sync"
	"time"
)

// Card liveness for a group-owned reader.
//
// Receiving SMS does not need the card: once the tunnel and the IMS security
// association are keyed, every line runs without touching it. A card that dies
// while a group owns it therefore stays invisible until some line needs an
// authentication, which on this network is up to eight hours away, and then
// every line sharing that card fails at once and cannot come back. Meanwhile
// the ordinary device snapshot poller deliberately skips group-owned modems so
// it cannot fight the broker for the reader.
//
// The broker is the one component that legitimately holds the reader, so it is
// where card liveness belongs: record it for free on every transaction it
// already performs, and when the card has gone untouched for a while, spend one
// cheap read to find out whether it is still there.

// CardProbeInterval is how stale a verification may get before the broker
// spends a read on the card. Ten minutes bounds the discovery of a dead card
// without adding meaningful traffic: one command per modem per interval,
// against a reader that is otherwise idle for hours.
const CardProbeInterval = 10 * time.Minute

// CardHealth is what the broker knows about the physical card right now.
type CardHealth struct {
	// VerifiedAt is when the card last answered. Zero means it has not
	// answered since this broker started, which is not the same as absent.
	VerifiedAt time.Time
	// LastError is why the most recent attempt failed, empty when the last
	// attempt succeeded. It is a classified string, never a raw backend error.
	LastError string
	// ActiveICCID is the profile the card reported as active at VerifiedAt.
	ActiveICCID string
}

type cardHealthState struct {
	mu          sync.Mutex
	verifiedAt  time.Time
	lastError   string
	activeICCID string
}

func (state *cardHealthState) succeed(iccid string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.verifiedAt = time.Now().UTC()
	state.lastError = ""
	if iccid != "" {
		state.activeICCID = iccid
	}
}

func (state *cardHealthState) fail(class string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.lastError = class
}

func (state *cardHealthState) snapshot() CardHealth {
	state.mu.Lock()
	defer state.mu.Unlock()
	return CardHealth{VerifiedAt: state.verifiedAt, LastError: state.lastError, ActiveICCID: state.activeICCID}
}

// CardHealth reports what the broker last observed about the physical card.
func (b *AuthBroker) CardHealth() CardHealth {
	return b.card.snapshot()
}

// ProbeCard reads the active profile if nothing else has touched the card
// recently. It takes the same transaction token as an authentication, so it can
// never overlap a profile switch; a caller that arrives while it runs waits for
// one short read.
//
// It reports whether it actually probed, so callers can log the interesting
// case without logging the far more common "someone else already verified it".
func (b *AuthBroker) ProbeCard(ctx context.Context, staleAfter time.Duration) (bool, error) {
	if staleAfter <= 0 {
		staleAfter = CardProbeInterval
	}
	if health := b.card.snapshot(); !health.VerifiedAt.IsZero() &&
		time.Since(health.VerifiedAt) < staleAfter {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-b.transaction:
	}
	defer func() { b.transaction <- struct{}{} }()

	// Re-check under the token: an authentication may have verified the card
	// while this call was queued, which is a better answer than our own read.
	if health := b.card.snapshot(); !health.VerifiedAt.IsZero() &&
		time.Since(health.VerifiedAt) < staleAfter {
		return false, nil
	}
	active, err := b.options.Backend.ActiveICCID(ctx)
	if err != nil {
		b.card.fail(safeErrorClass(err))
		return true, safeBackendError("probe card", err)
	}
	b.card.succeed(active)
	return true, nil
}

// WatchCard probes the card whenever it has gone unverified for longer than
// the interval, until the context ends. Failures are logged, never fatal: a
// probe is an observation, and a line that is happily receiving SMS must not be
// disturbed because the reader stopped answering.
func (b *AuthBroker) WatchCard(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = CardProbeInterval
	}
	// Check well inside the interval so a card that dies just after a
	// transaction is still found within roughly one interval.
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	previousError := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		probed, err := b.ProbeCard(probeCtx, interval)
		cancel()
		if !probed {
			continue
		}
		if err != nil {
			if previousError == "" {
				b.options.Logger.Warn("multisim card did not answer",
					"device_id", b.options.DeviceID, "error_class", safeErrorClass(err))
			}
			previousError = safeErrorClass(err)
			continue
		}
		if previousError != "" {
			b.options.Logger.Info("multisim card answered again",
				"device_id", b.options.DeviceID)
			previousError = ""
		}
	}
}
