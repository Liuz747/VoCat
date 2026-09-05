package vowifi

import (
	"context"
	"testing"
	"time"
)

// blockingIMSProvider never registers, like a P-CSCF that answers the first
// REGISTER and then goes silent.
type blockingIMSProvider struct{}

func (blockingIMSProvider) Start(ctx context.Context, _ IMSRequest) (IMSSession, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Regression: a silent registrar used to consume the caller's whole operation
// budget (measured 120 s of no logs on the public host), while the runtime's
// own retry registered in about three seconds. Bound the registration instead.
func TestEnableBoundsIMSRegistrationSoTheRuntimeCanRetrySooner(t *testing.T) {
	environment := newFakeEnvironment()
	orchestrator, err := New(Dependencies{
		SIM:    fakeSIM{environment},
		AKA:    fakeAKA{environment},
		Radio:  fakeRadio{environment},
		Proxy:  fakeProxy{environment},
		Tunnel: fakeTunnelProvider{environment},
		IMS:    blockingIMSProvider{},
		Phones: fakePhones{environment},
	}, Options{
		DeviceID:               "EC20",
		CleanupTimeout:         time.Second,
		IMSRegistrationTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	started := time.Now()
	state, enableErr := orchestrator.Enable(context.Background())
	elapsed := time.Since(started)
	if enableErr == nil {
		t.Fatal("Enable() error = nil, want the stalled IMS registration reported")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Enable() took %v; a silent registrar must not hold the operation budget", elapsed)
	}
	if state.Phase != PhaseFailed {
		t.Fatalf("state after a stalled registration = %+v", state)
	}
}

func TestIMSRegistrationTimeoutDefaultsWhenUnset(t *testing.T) {
	environment := newFakeEnvironment()
	orchestrator := newTestOrchestrator(t, environment, false)
	if got := orchestrator.options.IMSRegistrationTimeout; got != defaultIMSRegistrationTimeout {
		t.Fatalf("IMSRegistrationTimeout = %v, want the %v default", got, defaultIMSRegistrationTimeout)
	}
}
