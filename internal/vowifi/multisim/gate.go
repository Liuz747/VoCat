package multisim

import "context"

// StartupGate admits a bounded number of line setup sequences at a time. All
// lines of one group share one physical reader, so letting every line start
// at once only makes each AKA exchange wait behind the others' profile
// switches until the network side gives up on it.
type StartupGate struct{ slots chan struct{} }

// NewStartupGate allows up to size concurrent setups; size < 1 means one.
func NewStartupGate(size int) *StartupGate {
	if size < 1 {
		size = 1
	}
	return &StartupGate{slots: make(chan struct{}, size)}
}

// Acquire blocks until a slot is free or ctx ends. The release function is
// idempotent so a caller may defer it and also release early.
func (g *StartupGate) Acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case g.slots <- struct{}{}:
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		<-g.slots
	}, nil
}

// Size reports the configured concurrency.
func (g *StartupGate) Size() int { return cap(g.slots) }
