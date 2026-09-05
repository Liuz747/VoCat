package device

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"vocat/internal/modem"
)

// profileSwitchTimer logs one line per stage of an eSIM profile switch with
// the stage and cumulative durations, so the slow part of a switch can be
// read straight from the journal instead of inferred from code timeouts.
type profileSwitchTimer struct {
	logger *slog.Logger
	id     string
	iccid  string
	now    func() time.Time
	start  time.Time
	last   time.Time
}

func newProfileSwitchTimer(logger *slog.Logger, id, iccid string, now func() time.Time) *profileSwitchTimer {
	if now == nil {
		now = time.Now
	}
	start := now()
	return &profileSwitchTimer{logger: logger, id: id, iccid: iccid, now: now, start: start, last: start}
}

func (timer *profileSwitchTimer) stage(name string) {
	if timer == nil {
		return
	}
	current := timer.now()
	stageMS := current.Sub(timer.last).Milliseconds()
	totalMS := current.Sub(timer.start).Milliseconds()
	timer.last = current
	if timer.logger == nil {
		return
	}
	timer.logger.Info("eSIM profile switch stage",
		"device_id", timer.id, "iccid", timer.iccid, "stage", name, "stage_ms", stageMS, "total_ms", totalMS)
}

func (timer *profileSwitchTimer) finish(err error) {
	if timer == nil || timer.logger == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	timer.logger.Info("eSIM profile switch finished",
		"device_id", timer.id, "iccid", timer.iccid, "ok", err == nil, "error", errText,
		"total_ms", timer.now().Sub(timer.start).Milliseconds())
}

// waitForSIMReadyAfterSwitch polls AT+CPIN? until the modem reports READY.
// Right after an EnableProfile with REFRESH the eUICC is being re-initialised;
// taking a full snapshot in that window trips readSnapshot's hot-swap branch
// (an extra CFUN=0/4 cycle) and then a QMI ICCID read that blocks for the
// whole long timeout (measured 45 s). Waiting for READY first costs the real
// SIM initialisation time only.
func (manager *Manager) waitForSIMReadyAfterSwitch(ctx context.Context, id string, interval time.Duration) error {
	for {
		response, err := manager.ExecuteAT(ctx, id, "AT+CPIN?")
		if err == nil && cpinReportsReady(response) {
			break
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("wait for SIM ready after profile switch: %w (last error: %v)", ctx.Err(), err)
			}
			return fmt.Errorf("wait for SIM ready after profile switch: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
	// CPIN READY comes well before the phonebook/SMS initialisation finishes.
	// Quectel exposes that via AT+QINISTAT (bit 4 = phonebook ready); while it
	// is unset, CNUM/CPBS/CPBR do not answer and each costs a full command
	// timeout in the snapshot. Modems without QINISTAT skip this wait.
	for {
		response, err := manager.ExecuteAT(ctx, id, "AT+QINISTAT")
		if err != nil {
			return nil
		}
		ready, known := qinistatPhonebookReady(response)
		if !known || ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for phonebook init after profile switch: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// qinistatPhonebookReady parses "+QINISTAT: <n>" (Quectel): bit 1 = CPIN
// ready, bit 2 = SMS init done, bit 4 = phonebook init done.
func qinistatPhonebookReady(response modem.Response) (ready, known bool) {
	value := strings.TrimSpace(valueAfterPrefix(response, "+QINISTAT:"))
	if value == "" {
		return false, false
	}
	status, err := strconv.Atoi(value)
	if err != nil {
		return false, false
	}
	return status&4 != 0, true
}

// cpinReportsReady is stricter than parseCPIN: Quectel firmware answers
// "+CPIN: NOT READY" while the SIM stack initialises, which parseCPIN's
// substring match would accept as READY.
func cpinReportsReady(response modem.Response) bool {
	value := strings.ToUpper(strings.TrimSpace(valueAfterPrefix(response, "+CPIN:")))
	return strings.Contains(value, "READY") && !strings.Contains(value, "NOT READY")
}

// phoneReadHoldAfterSwitch is how long CNUM/phonebook/EF_MSISDN reads stay
// suppressed after an eSIM profile switch. Issued too early, AT+CNUM stalls
// the EC20's AT interpreter for ~45 s (every following command times out),
// even though CPIN and QINISTAT already report ready. The number is supplied
// by the IMS registration anyway.
const phoneReadHoldAfterSwitch = 2 * time.Minute

func (manager *Manager) holdPhoneReads(id string, duration time.Duration) {
	manager.phoneHoldMu.Lock()
	defer manager.phoneHoldMu.Unlock()
	if manager.phoneHold == nil {
		manager.phoneHold = make(map[string]time.Time)
	}
	manager.phoneHold[id] = time.Now().Add(duration)
}

func (manager *Manager) phoneReadsHeld(id string) bool {
	manager.phoneHoldMu.Lock()
	defer manager.phoneHoldMu.Unlock()
	until, ok := manager.phoneHold[id]
	return ok && time.Now().Before(until)
}
