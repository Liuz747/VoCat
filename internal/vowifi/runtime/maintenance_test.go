package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestMaintenanceRejectsReconnectWithoutChangingDesiredState(t *testing.T) {
	m := New(Options{})
	defer m.Close(context.Background())
	if err := m.Register(testOrchestrator(t, "reader")); err != nil {
		t.Fatal(err)
	}
	if err := m.BeginMaintenance("reader"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestReconnect("reader"); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("maintenance reconnect accepted: %v", err)
	}
	m.mu.Lock()
	desired := m.entries["reader"].desiredEnabled
	m.mu.Unlock()
	if desired {
		t.Fatal("rejected request changed desired single-line state")
	}
	state, err := m.State("reader")
	if err != nil {
		t.Fatal(err)
	}
	if state.Enabled || state.Active {
		t.Fatal("maintenance spawned a competing session")
	}
}
