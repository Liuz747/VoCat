package multisim

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"vocat/internal/vowifi"
)

// AuthBroker owns one cancelable transaction lock for a physical reader. All
// profile adapters sharing that reader must come from this broker. It never
// holds the backend's APDU lock while invoking backend methods.
type AuthBroker struct {
	options     BrokerOptions
	transaction chan struct{}
	// Accessed only while holding transaction. Once learned, IMSI cannot change
	// during this broker's lifetime, including through another profile adapter.
	imsis map[string]string
	// Service-centre addresses by ICCID, learned while the profile is switched
	// in. Read outside the transaction by ReadSMSCenter, hence the mutex.
	smscMu sync.Mutex
	smscs  map[string]string
	// card records when the physical card last answered. Receiving SMS needs
	// no card, so without this a dead reader stays invisible until the next
	// authentication, hours away, and then takes every line on it down at once.
	card cardHealthState
}

type ProfileAdapter struct {
	broker  *AuthBroker
	profile Profile
}

var _ vowifi.SIMIdentityReader = (*ProfileAdapter)(nil)
var _ vowifi.PreferredAKAProvider = (*ProfileAdapter)(nil)
var _ vowifi.SMSCenterReader = (*ProfileAdapter)(nil)

var errSMSCenterUnsupported = errors.New("multisim: reader does not expose the SMS service-centre address")

func NewAuthBroker(options BrokerOptions) (*AuthBroker, error) {
	if options.DeviceID == "" || options.Backend == nil {
		return nil, errors.New("multisim: physical device and authentication backend are required")
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 2 * time.Minute
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	b := &AuthBroker{options: options, transaction: make(chan struct{}, 1), imsis: make(map[string]string), smscs: make(map[string]string)}
	b.transaction <- struct{}{}
	return b, nil
}

func (b *AuthBroker) ForProfile(profile Profile) (*ProfileAdapter, error) {
	if err := profile.validate(); err != nil {
		return nil, err
	}
	return &ProfileAdapter{broker: b, profile: profile}, nil
}

func (a *ProfileAdapter) ReadIdentity(ctx context.Context, _ string) (identity vowifi.SIMIdentity, err error) {
	err = a.withProfile(ctx, nil, nil, func(_ context.Context, live vowifi.SIMIdentity) error { identity = live; return nil })
	if err != nil {
		return vowifi.SIMIdentity{}, err
	}
	return identity, nil
}

// ReadSMSCenter answers with this profile's own service-centre address. The
// address is read from the card while the profile is switched in (normally
// as a side effect of the identity read that precedes it) and cached for the
// broker's lifetime, so later submissions never switch the card just for it.
func (a *ProfileAdapter) ReadSMSCenter(ctx context.Context, _ string) (string, error) {
	if smsc, ok := a.broker.cachedSMSCenter(a.profile.ICCID); ok {
		return smsc, nil
	}
	if _, ok := a.broker.options.Backend.(vowifi.SMSCenterReader); !ok {
		return "", errSMSCenterUnsupported
	}
	var readErr error
	err := a.withProfile(ctx, nil, nil, func(ctx context.Context, _ vowifi.SIMIdentity) error {
		readErr = a.broker.learnSMSCenter(ctx, a.profile.ICCID)
		return nil
	})
	if err != nil {
		return "", err
	}
	if readErr != nil {
		return "", readErr
	}
	smsc, _ := a.broker.cachedSMSCenter(a.profile.ICCID)
	return smsc, nil
}

func (b *AuthBroker) cachedSMSCenter(iccid string) (string, bool) {
	b.smscMu.Lock()
	defer b.smscMu.Unlock()
	smsc, ok := b.smscs[iccid]
	return smsc, ok
}

// learnSMSCenter must run with the profile switched in and the transaction
// held. A failure is not cached: the next acquisition simply tries again.
func (b *AuthBroker) learnSMSCenter(ctx context.Context, iccid string) error {
	reader, ok := b.options.Backend.(vowifi.SMSCenterReader)
	if !ok {
		return errSMSCenterUnsupported
	}
	smsc, err := reader.ReadSMSCenter(ctx, b.options.DeviceID)
	if err != nil {
		b.options.Logger.Debug("multisim service-centre read failed", "profile_suffix", profileSuffix(iccid), "error_class", safeErrorClass(err))
		return safeBackendError("read SMS service centre", err)
	}
	if smsc == "" {
		return errors.New("multisim: card reported no SMS service-centre address")
	}
	b.smscMu.Lock()
	b.smscs[iccid] = smsc
	b.smscMu.Unlock()
	return nil
}

func (a *ProfileAdapter) CheckReady(ctx context.Context, expected vowifi.SIMIdentity) (evidence vowifi.AKAEvidence, err error) {
	err = a.withProfile(ctx, &expected, nil, func(ctx context.Context, live vowifi.SIMIdentity) error {
		var err error
		evidence, err = a.broker.options.Backend.CheckReady(ctx, live)
		if err != nil {
			return safeBackendError("AKA readiness check", err)
		}
		if !evidence.Ready {
			return errors.New("multisim: AKA application is not ready")
		}
		return nil
	})
	if err != nil {
		return vowifi.AKAEvidence{}, err
	}
	return evidence, nil
}

func (a *ProfileAdapter) Authenticate(ctx context.Context, identity vowifi.SIMIdentity, challenge vowifi.AKAChallenge) (vowifi.AKAResult, error) {
	return a.AuthenticateWithPreference(ctx, identity, challenge, "")
}

func (a *ProfileAdapter) AuthenticateWithPreference(ctx context.Context, expected vowifi.SIMIdentity, challenge vowifi.AKAChallenge, preference string) (result vowifi.AKAResult, err error) {
	started := time.Now()
	var waited time.Duration
	acquired := false
	logger := a.broker.options.Logger.With("profile_suffix", profileSuffix(a.profile.ICCID))
	logger.Info("multisim AKA queued")
	defer func() {
		elapsed := time.Since(started)
		if !acquired {
			waited = elapsed
		}
		if err != nil {
			logger.Warn("multisim AKA failed", "wait_ms", waited.Milliseconds(), "total_ms", elapsed.Milliseconds(), "error_class", safeErrorClass(err))
		} else {
			logger.Info("multisim AKA completed", "wait_ms", waited.Milliseconds(), "total_ms", elapsed.Milliseconds())
		}
	}()
	err = a.withProfile(ctx, &expected, func() {
		acquired = true
		waited = time.Since(started)
		logger.Info("multisim AKA reader acquired", "wait_ms", waited.Milliseconds())
	}, func(ctx context.Context, live vowifi.SIMIdentity) error {
		evidence, err := a.broker.options.Backend.CheckReady(ctx, live)
		if err != nil {
			return safeBackendError("AKA readiness check", err)
		}
		if !evidence.Ready {
			return errors.New("multisim: AKA application is not ready")
		}
		result, err = a.broker.options.Backend.AuthenticateWithPreference(ctx, live, challenge, preference)
		if err != nil {
			return safeBackendError("AKA authentication", err)
		}
		return nil
	})
	if err != nil {
		return vowifi.AKAResult{}, err
	}
	return result, nil
}

func (a *ProfileAdapter) withProfile(ctx context.Context, expected *vowifi.SIMIdentity, acquired func(), operation func(context.Context, vowifi.SIMIdentity) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, a.broker.options.RequestTimeout)
	defer cancel()
	if expected != nil && (expected.ICCID != a.profile.ICCID || !validIMSI(expected.IMSI)) {
		return ErrIdentityMismatch
	}
	// Check both before and after acquisition: cancellation must win even when
	// the token becomes available at the same instant as the deadline.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.broker.transaction:
	}
	defer func() { a.broker.transaction <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if acquired != nil {
		acquired()
	}
	backend := a.broker.options.Backend
	active, err := backend.ActiveICCID(ctx)
	if err != nil {
		return safeBackendError("read active profile", err)
	}
	if active != a.profile.ICCID {
		logger := a.broker.options.Logger.With("profile_suffix", profileSuffix(a.profile.ICCID))
		logger.Info("multisim profile switch requested")
		if err := backend.SwitchProfile(ctx, a.profile); err != nil {
			logger.Warn("multisim profile switch failed", "error_class", safeErrorClass(err))
			return safeBackendError("profile switch", err)
		}
		active, err = backend.ActiveICCID(ctx)
		if err != nil {
			return safeBackendError("confirm active profile", err)
		}
		if active != a.profile.ICCID {
			return ErrIdentityMismatch
		}
		logger.Info("multisim profile switch verified")
	}
	live, err := backend.ReadIdentity(ctx, a.broker.options.DeviceID)
	if err != nil {
		return safeBackendError("read SIM identity", err)
	}
	if live.ICCID != a.profile.ICCID || !validIMSI(live.IMSI) {
		return ErrIdentityMismatch
	}
	pinned := a.broker.imsis[a.profile.ICCID]
	if (pinned != "" && pinned != live.IMSI) || (expected != nil && expected.IMSI != live.IMSI) {
		return ErrIdentityMismatch
	}
	a.broker.imsis[a.profile.ICCID] = live.IMSI
	// The card just answered with the identity we expected: that is the
	// strongest possible liveness evidence and it costs nothing to record.
	a.broker.card.succeed(active)
	if err := ctx.Err(); err != nil {
		return err
	}
	// The card is switched in and verified: learn its service-centre address
	// now so SMS submission later needs no switch of its own.
	if _, cached := a.broker.cachedSMSCenter(a.profile.ICCID); !cached {
		if _, ok := backend.(vowifi.SMSCenterReader); ok {
			_ = a.broker.learnSMSCenter(ctx, a.profile.ICCID)
		}
	}
	if err := operation(ctx, live); err != nil {
		return err
	}
	return ctx.Err()
}

func profileSuffix(iccid string) string {
	if len(iccid) > 4 {
		return iccid[len(iccid)-4:]
	}
	return "****"
}

func safeErrorClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, ErrIdentityMismatch), errors.Is(err, vowifi.ErrInvalidIdentity):
		return "identity_mismatch"
	case errors.Is(err, vowifi.ErrEAPAuthenticationRejected):
		return "authentication_rejected"
	default:
		return "backend_failure"
	}
}

func validIMSI(imsi string) bool { return len(imsi) >= 10 && len(imsi) <= 18 && digits(imsi) }

// Do not wrap arbitrary backend errors: APDU and AT implementations can embed
// command text, including authentication material. Preserve only safe classes.
func safeBackendError(operation string, err error) error {
	for _, safe := range []error{context.Canceled, context.DeadlineExceeded, ErrIdentityMismatch, vowifi.ErrInvalidIdentity, vowifi.ErrEAPAuthenticationRejected} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return errors.New("multisim: " + operation + " failed")
}
