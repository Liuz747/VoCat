package multisim

import (
	"context"
	"testing"
	"time"
)

func TestStartupGateBoundsConcurrencyAndHonoursContext(t *testing.T) {
	gate := NewStartupGate(2)
	if gate.Size() != 2 {
		t.Fatalf("size = %d", gate.Size())
	}
	releaseA, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	releaseB, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := gate.Acquire(ctx); err == nil {
		t.Fatal("third acquisition succeeded while the gate was full")
	}
	releaseA()
	releaseA() // idempotent
	releaseC, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	releaseB()
	releaseC()
	if NewStartupGate(0).Size() != 1 {
		t.Fatal("zero size did not clamp to one")
	}
}
