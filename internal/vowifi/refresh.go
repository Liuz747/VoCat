package vowifi

import (
	"context"
	"errors"
)

// IMSRegistrationRefresher renews the current registration without creating a
// new tunnel. It is an explicit maintenance action, not a profile-rotation job.
type IMSRegistrationRefresher interface {
	RefreshRegistration(context.Context) error
}

func (orchestrator *Orchestrator) RefreshRegistration(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	orchestrator.mu.Lock()
	resources := orchestrator.resources
	ready := orchestrator.state.IMSReady
	orchestrator.mu.Unlock()
	if !ready || resources == nil || resources.ims == nil {
		return ErrIMSNotRegistered
	}
	refresher, ok := resources.ims.(IMSRegistrationRefresher)
	if !ok {
		return errors.New("vowifi: current IMS provider does not support explicit renewal")
	}
	// The provider serializes registration and cancellation against Close.
	// Do not hold the orchestrator state lock across network or SIM work.
	return refresher.RefreshRegistration(ctx)
}
