package device

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"vocat/internal/modem"
)

func TestProfileSwitchTimerLogsEachStageWithDurations(t *testing.T) {
	var buffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffer, nil))
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	timer := newProfileSwitchTimer(logger, "ec20", "8901240527185778332", clock)
	now = now.Add(300 * time.Millisecond)
	timer.stage("deregister")
	now = now.Add(20 * time.Second)
	timer.stage("refresh_wait")
	timer.finish(nil)

	output := buffer.String()
	for _, want := range []string{
		`stage=deregister`, `stage_ms=300`, `total_ms=300`,
		`stage=refresh_wait`, `stage_ms=20000`, `total_ms=20300`,
		`msg="eSIM profile switch finished"`, `ok=true`, `device_id=ec20`, `iccid=8901240527185778332`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing %q in switch timing log:\n%s", want, output)
		}
	}
	// A nil logger must be safe: the timer is used on code paths shared with tests.
	newProfileSwitchTimer(nil, "ec20", "x", clock).stage("noop")
	_ = context.Background()
}

func TestWaitForSIMReadyAfterSwitchPollsCPINUntilReady(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: "AT+CPIN?", err: errors.New("+CME ERROR: 13")},
		{command: "AT+CPIN?", response: okResponse("+CPIN: NOT READY")},
		{command: "AT+CPIN?", response: okResponse("+CPIN: READY")},
		{command: "AT+QINISTAT", response: okResponse("+QINISTAT: 7")},
	}}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.waitForSIMReadyAfterSwitch(ctx, id, 0); err != nil {
		t.Fatalf("waitForSIMReadyAfterSwitch: %v", err)
	}
	client.assertDone(t)
}

func TestWaitForSIMReadyAfterSwitchGivesUpAtDeadline(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: "AT+CPIN?", err: errors.New("+CME ERROR: 13")},
		{command: "AT+CPIN?", err: errors.New("+CME ERROR: 13")},
		{command: "AT+CPIN?", err: errors.New("+CME ERROR: 13")},
	}}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := manager.waitForSIMReadyAfterSwitch(ctx, id, 60*time.Millisecond); err == nil {
		t.Fatal("expected a deadline error when the SIM never becomes ready")
	}
}

func TestParseQINISTATPhonebookReady(t *testing.T) {
	for _, test := range []struct {
		response  modem.Response
		wantReady bool
		wantKnown bool
	}{
		{okResponse("+QINISTAT: 7"), true, true},
		{okResponse("+QINISTAT: 3"), false, true},
		{okResponse("+QINISTAT: 0"), false, true},
		{okResponse("+QINISTAT: 5"), true, true},
		{okResponse("OK"), false, false},
	} {
		ready, known := qinistatPhonebookReady(test.response)
		if ready != test.wantReady || known != test.wantKnown {
			t.Fatalf("qinistatPhonebookReady(%v) = (%v, %v), want (%v, %v)", test.response.Lines, ready, known, test.wantReady, test.wantKnown)
		}
	}
}

func TestWaitForSIMReadyAfterSwitchAlsoWaitsForPhonebookInit(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: "AT+CPIN?", response: okResponse("+CPIN: READY")},
		{command: "AT+QINISTAT", response: okResponse("+QINISTAT: 3")},
		{command: "AT+QINISTAT", response: okResponse("+QINISTAT: 7")},
	}}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.waitForSIMReadyAfterSwitch(ctx, id, 0); err != nil {
		t.Fatalf("waitForSIMReadyAfterSwitch: %v", err)
	}
	client.assertDone(t)
}

func TestWaitForSIMReadyAfterSwitchToleratesModemsWithoutQINISTAT(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: "AT+CPIN?", response: okResponse("+CPIN: READY")},
		{command: "AT+QINISTAT", err: errors.New("ERROR")},
	}}
	manager, id := newStartedTestManager(t, client)
	if err := manager.waitForSIMReadyAfterSwitch(context.Background(), id, 0); err != nil {
		t.Fatalf("waitForSIMReadyAfterSwitch: %v", err)
	}
	client.assertDone(t)
}

func TestPhoneReadsAreHeldAfterProfileSwitch(t *testing.T) {
	manager, id := newStartedTestManager(t, &transcriptClient{})
	if manager.phoneReadsHeld(id) {
		t.Fatal("phone reads held before any switch")
	}
	manager.holdPhoneReads(id, time.Minute)
	if !manager.phoneReadsHeld(id) {
		t.Fatal("phone reads not held right after a switch")
	}
	manager.holdPhoneReads(id, -time.Second)
	if manager.phoneReadsHeld(id) {
		t.Fatal("expired hold still active")
	}
}
