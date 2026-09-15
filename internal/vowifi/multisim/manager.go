package multisim

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"vocat/internal/vowifi"
	vowifiruntime "vocat/internal/vowifi/runtime"
)

// Manager owns group configuration operations. Each group's runtime.Manager
// owns the independent per-subscription retry and failure-notification loops.
type Manager struct {
	options Options
	mu      sync.Mutex
	closed  bool
	groups  map[string]*group
	wg      sync.WaitGroup
}

type group struct {
	config        Config
	state         GroupState
	owned         bool
	ctx           context.Context
	cancel        context.CancelFunc
	next          *Config
	stopRequested bool
	// reconciling serialises incremental edits of a running group without
	// marking the group busy, so SMS and line operations keep working.
	reconciling  bool
	wake         chan struct{}
	runtime      *vowifiruntime.Manager
	lines        []*line
	initializers sync.WaitGroup
	actions      sync.WaitGroup
}

type line struct {
	profile        Profile
	sessionID      string
	orchestrator   *vowifi.Orchestrator
	lastError      string
	retryWake      chan struct{}
	refreshPending bool
	// cancel stops this line's initialization without touching the group;
	// initialized is closed once initializeLine has returned.
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan struct{}
}

func newLine(cycleCtx context.Context, deviceID string, profile Profile) *line {
	ctx, cancel := context.WithCancel(cycleCtx)
	return &line{profile: profile, sessionID: LineID(deviceID, profile.ICCID), retryWake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, initialized: make(chan struct{})}
}

func New(options Options) *Manager {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = 2 * time.Minute
	}
	if options.CleanupTimeout <= 0 {
		options.CleanupTimeout = 30 * time.Second
	}
	if options.PrepareRetryInitial <= 0 {
		options.PrepareRetryInitial = 5 * time.Second
	}
	if options.PrepareRetryMaximum <= 0 {
		options.PrepareRetryMaximum = 60 * time.Second
	}
	if options.PrepareRetryMaximum < options.PrepareRetryInitial {
		options.PrepareRetryMaximum = options.PrepareRetryInitial
	}
	return &Manager{options: options, groups: make(map[string]*group)}
}

// Apply accepts desired configuration independently of the request lifetime.
// Disabling always cancels an in-flight start. Another enabled configuration
// is accepted only after the previous configuration operation has finished.
func (m *Manager) Apply(ctx context.Context, config Config) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err := config.Validate(); err != nil {
		return err
	}
	config = cloneConfig(config)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	g := m.groups[config.DeviceID]
	if !config.Enabled {
		if g == nil {
			m.groups[config.DeviceID] = &group{config: config, state: GroupState{DeviceID: config.DeviceID, Phase: "idle", Lines: []LineState{}, UpdatedAt: time.Now().UTC()}}
			return nil
		}
		g.next = nil
		g.stopRequested = true
		g.state.Enabled = false
		g.state.UpdatedAt = time.Now().UTC()
		if g.owned {
			if g.cancel != nil {
				g.cancel()
			}
			signal(g.wake)
		}
		return nil
	}
	if m.options.Factory == nil {
		return errors.New("multisim: orchestrator factory is required")
	}
	if g != nil && g.owned {
		if g.state.Busy || g.reconciling {
			return ErrOperationInProgress
		}
		if !g.stopRequested && g.state.Phase == "running" {
			if reflect.DeepEqual(g.config, config) {
				return nil
			}
			if g.config.Enabled && g.config.DeviceID == config.DeviceID {
				// Same reader, same enabled group: edit the line set in place
				// instead of tearing every line down and rebuilding the group.
				g.reconciling = true
				m.mu.Unlock()
				err := m.reconcile(ctx, g, config)
				m.mu.Lock()
				g.reconciling = false
				return err
			}
		}
		g.next = &config
		g.stopRequested = true
		g.state.Enabled = true
		g.state.Busy = true
		g.state.Phase = "stopping"
		g.state.UpdatedAt = time.Now().UTC()
		if g.cancel != nil {
			g.cancel()
		}
		signal(g.wake)
		return nil
	}
	cycleCtx, cancel := context.WithCancel(context.Background())
	g = &group{config: config, owned: true, ctx: cycleCtx, cancel: cancel, wake: make(chan struct{}, 1), state: GroupState{DeviceID: config.DeviceID, Enabled: true, Busy: true, Phase: "preparing", Lines: []LineState{}, UpdatedAt: time.Now().UTC()}}
	m.groups[config.DeviceID] = g
	m.wg.Add(1)
	go m.run(g, cycleCtx)
	return nil
}

func (m *Manager) Owns(deviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[deviceID]
	return g != nil && g.owned
}

// State reads only in-memory snapshots. It must never Ensure a runtime or
// consult a physical reader, even for a device with no registered group.
func (m *Manager) State(deviceID string) GroupState {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[deviceID]
	if g == nil {
		return GroupState{DeviceID: deviceID, Phase: "idle", Lines: []LineState{}}
	}
	state := g.state
	state.Lines = make([]LineState, 0, len(g.lines))
	for _, item := range g.lines {
		snapshot := vowifi.State{DeviceID: item.sessionID, ICCID: item.profile.ICCID, Phase: vowifi.PhaseIdle}
		if item.orchestrator != nil {
			snapshot = item.orchestrator.State()
		} else if item.lastError != "" {
			snapshot.Phase = vowifi.PhaseFailed
			snapshot.LastError = item.lastError
			snapshot.LastErrorClass = "initialization"
		}
		state.Lines = append(state.Lines, LineState{ICCID: item.profile.ICCID, Name: item.profile.Name, SessionID: item.sessionID, State: snapshot})
		if snapshot.UpdatedAt.After(state.UpdatedAt) {
			state.UpdatedAt = snapshot.UpdatedAt
		}
	}
	if m.options.CardHealth != nil {
		health := m.options.CardHealth(deviceID)
		state.CardVerifiedAt = health.VerifiedAt
		state.CardError = health.LastError
		state.CardActiveICCID = health.ActiveICCID
	}
	return state
}

func (m *Manager) Reconnect(deviceID, iccid string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	if g.state.Busy || g.state.Phase != "running" {
		m.mu.Unlock()
		return ErrOperationInProgress
	}
	var item *line
	for _, candidate := range g.lines {
		if candidate.profile.ICCID == iccid {
			item = candidate
			break
		}
	}
	if item == nil {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	if item.orchestrator == nil {
		signal(item.retryWake)
		m.mu.Unlock()
		return nil
	}
	// runtime operations do not call back into this manager. Hold the group lock
	// until the reconnect is queued so disabling cannot overtake this request.
	_, err := g.runtime.RequestReconnect(item.sessionID)
	m.mu.Unlock()
	return err
}

// Disconnect cleanly tears down one line's IMS and IKE sessions while keeping
// the multi-SIM group and its sibling lines running. A later Reconnect restores
// the line's desired state and establishes it again.
func (m *Manager) Disconnect(deviceID, iccid string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	if g.state.Busy || g.state.Phase != "running" {
		m.mu.Unlock()
		return ErrOperationInProgress
	}
	var item *line
	for _, candidate := range g.lines {
		if candidate.profile.ICCID == iccid {
			item = candidate
			break
		}
	}
	if item == nil || item.orchestrator == nil {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	_, err := g.runtime.RequestEnabled(item.sessionID, false)
	m.mu.Unlock()
	if err == nil {
		m.options.Logger.Info("multisim disconnect_requested", "device_id", deviceID, "session_id", item.sessionID, "profile_suffix", profileSuffix(item.profile.ICCID))
	}
	return err
}

func (m *Manager) run(g *group, cycleCtx context.Context) {
	defer m.wg.Done()
	var prepareRetry time.Duration
	for {
		m.mu.Lock()
		config := cloneConfig(g.config)
		m.mu.Unlock()
		startErr := m.start(g, cycleCtx, config)
		if startErr == nil {
			prepareRetry = 0
		}
		if startErr == nil && cycleCtx.Err() == nil {
			m.setPhase(g, "running", false, "")
			<-cycleCtx.Done()
		}
		m.setPhase(g, "stopping", true, "")
		m.mu.Lock()
		runtime := g.runtime
		m.mu.Unlock()
		if runtime != nil {
			// Close's cancellation stops all independent retry loops. The completion
			// channel proves workers and session teardown finished; a timer alone does
			// not permit restoring the physical card or releasing group ownership.
			done := make(chan error, 1)
			go func() {
				err := runtime.Close(context.Background())
				// A factory may be finishing after cancellation. Its worker must
				// close any late resources before the physical reader is restored.
				g.initializers.Wait()
				g.actions.Wait()
				// Orchestrator Close contains slow providers; only this barrier
				// establishes that they cannot touch the physical reader again.
				err = errors.Join(err, m.waitLineCleanup(g))
				done <- err
			}()
			timer := time.NewTimer(m.options.CleanupTimeout)
			var closeErr error
			select {
			case closeErr = <-done:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-timer.C:
				m.setPhase(g, "cleanup_failed", false, "session cleanup exceeded its deadline; reader remains owned")
				closeErr = <-done
			}
			if closeErr != nil && !errors.Is(closeErr, vowifi.ErrCleanupIncomplete) {
				// With a non-cancelled Close context runtime has waited for all its
				// workers. Unexpected errors still require explicit retry of teardown.
				for closeErr != nil {
					m.setPhase(g, "cleanup_failed", false, "session cleanup failed; reader remains owned")
					if !m.waitRetry(g) {
						return
					}
					closeErr = m.closeLines(g)
				}
			}
		}
		for {
			m.setPhase(g, "restoring", true, "")
			restoreCtx, cancel := context.WithTimeout(context.Background(), m.options.CleanupTimeout)
			var restoreErr error
			if m.options.Restore != nil {
				restoreErr = m.options.Restore(restoreCtx, config)
			}
			// A backend returning success after its deadline has not established a
			// timely cleanup; retain ownership until a fresh restoration confirms it.
			if restoreErr == nil {
				restoreErr = restoreCtx.Err()
			}
			cancel()
			if restoreErr == nil {
				break
			}
			m.setPhase(g, "restore_failed", false, "physical profile/radio restoration failed; reader remains owned")
			if !m.waitRetry(g) {
				return
			}
		}
		m.mu.Lock()
		if startErr != nil && g.next == nil && !g.stopRequested && !m.closed &&
			m.options.PrepareRetryable != nil && m.options.PrepareRetryable(startErr) {
			// After a restart the reader may simply not be discovered yet. The
			// preparation has already been restored above, so waiting keeps only
			// the desired state; disabling, re-saving, or closing ends the wait.
			prepareRetry = m.nextPrepareRetry(prepareRetry)
			g.state.Phase = "waiting_device"
			g.state.Busy = false
			g.state.LastError = "reader not available yet; preparation will retry"
			g.state.UpdatedAt = time.Now().UTC()
			m.mu.Unlock()
			m.options.Logger.Info("multisim group reader not available; retrying preparation",
				"device_id", config.DeviceID, "retry_in", prepareRetry, "error", startErr)
			m.waitPrepareRetry(g, prepareRetry)
			m.mu.Lock()
			if g.next == nil && !g.stopRequested && !m.closed {
				g.cancel()
				g.runtime = nil
				g.lines = nil
				cycleCtx, g.cancel = context.WithCancel(context.Background())
				g.ctx = cycleCtx
				g.state = GroupState{DeviceID: config.DeviceID, Enabled: true, Busy: true, Phase: "preparing", Lines: []LineState{}, UpdatedAt: time.Now().UTC()}
				m.mu.Unlock()
				continue
			}
		}
		if g.next != nil && !m.closed {
			next := cloneConfig(*g.next)
			g.next = nil
			g.config = next
			g.stopRequested = false
			g.runtime = nil
			g.lines = nil
			cycleCtx, g.cancel = context.WithCancel(context.Background())
			g.ctx = cycleCtx
			g.state = GroupState{DeviceID: next.DeviceID, Enabled: true, Busy: true, Phase: "preparing", Lines: []LineState{}, UpdatedAt: time.Now().UTC()}
			m.mu.Unlock()
			continue
		}
		g.owned = false
		g.state.Busy = false
		g.state.Phase = "idle"
		g.state.LastError = ""
		if startErr != nil && !g.stopRequested && !m.closed {
			g.state.Phase = "failed"
			g.state.LastError = "group preparation failed"
			m.options.Logger.Error("multisim group preparation failed; no automatic retry, re-save the configuration to start again",
				"device_id", g.config.DeviceID, "error", startErr)
		} else {
			g.state.Enabled = false
			m.options.Logger.Info("multisim group released the device", "device_id", g.config.DeviceID)
		}
		g.state.UpdatedAt = time.Now().UTC()
		m.mu.Unlock()
		return
	}
}

func (m *Manager) start(g *group, cycleCtx context.Context, config Config) error {
	ctx, cancel := context.WithTimeout(cycleCtx, m.options.OperationTimeout)
	defer cancel()
	if m.options.Prepare != nil {
		if err := m.options.Prepare(ctx, config); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime := vowifiruntime.New(vowifiruntime.Options{Logger: m.options.Logger, OperationTimeout: m.options.OperationTimeout, RetryInitial: m.options.RetryInitial, RetryMaximum: m.options.RetryMaximum})
	m.mu.Lock()
	g.runtime = runtime
	for _, profile := range config.Profiles {
		g.lines = append(g.lines, newLine(cycleCtx, config.DeviceID, profile))
	}
	items := append([]*line(nil), g.lines...)
	g.initializers.Add(len(items))
	m.mu.Unlock()
	for _, item := range items {
		go m.initializeLine(g, config, runtime, item)
	}
	return ctx.Err()
}

// profileDiff splits a desired profile list against the running one. A changed
// AID is a removal plus an addition; a changed name is applied in place.
func profileDiff(current, desired []Profile) (added, removed, renamed []Profile) {
	running := make(map[string]Profile, len(current))
	for _, profile := range current {
		running[profile.ICCID] = profile
	}
	wanted := make(map[string]bool, len(desired))
	for _, profile := range desired {
		wanted[profile.ICCID] = true
		previous, ok := running[profile.ICCID]
		switch {
		case !ok:
			added = append(added, profile)
		case !strings.EqualFold(previous.AID, profile.AID):
			removed = append(removed, previous)
			added = append(added, profile)
		case previous.Name != profile.Name:
			renamed = append(renamed, profile)
		}
	}
	for _, profile := range current {
		if !wanted[profile.ICCID] {
			removed = append(removed, profile)
		}
	}
	return added, removed, renamed
}

// reconcile applies a changed profile list to a running group. The caller
// holds g.reconciling and has released m.mu; Verify may need the reader.
func (m *Manager) reconcile(ctx context.Context, g *group, config Config) error {
	m.mu.Lock()
	added, removed, renamed := profileDiff(g.config.Profiles, config.Profiles)
	m.mu.Unlock()
	if len(added) > 0 && m.options.Verify != nil {
		if err := m.options.Verify(ctx, config, added); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if g.stopRequested || g.state.Phase != "running" || g.runtime == nil {
		return ErrOperationInProgress
	}
	g.config = config
	g.state.UpdatedAt = time.Now().UTC()
	for _, profile := range renamed {
		for _, item := range g.lines {
			if item.profile.ICCID == profile.ICCID {
				item.profile.Name = profile.Name
			}
		}
	}
	for _, profile := range removed {
		for index, item := range g.lines {
			if item.profile.ICCID != profile.ICCID {
				continue
			}
			g.lines = append(g.lines[:index], g.lines[index+1:]...)
			g.actions.Add(1)
			go m.removeLine(g, item)
			break
		}
	}
	runtime := g.runtime
	for _, profile := range added {
		item := newLine(g.ctx, config.DeviceID, profile)
		g.lines = append(g.lines, item)
		g.initializers.Add(1)
		go m.initializeLine(g, config, runtime, item)
	}
	m.options.Logger.Info("multisim group reconciled in place",
		"device_id", config.DeviceID, "added", len(added), "removed", len(removed), "renamed", len(renamed), "lines", len(g.lines))
	return nil
}

// removeLine stops one line's initialization, then closes and forgets its
// session. Siblings are never touched; the group stays running.
func (m *Manager) removeLine(g *group, item *line) {
	defer g.actions.Done()
	item.cancel()
	<-item.initialized
	m.mu.Lock()
	orchestrator := item.orchestrator
	runtime := g.runtime
	deviceID := g.config.DeviceID
	m.mu.Unlock()
	attrs := []any{"device_id", deviceID, "session_id", item.sessionID,
		"profile_suffix", profileSuffix(item.profile.ICCID), "profile_name", item.profile.Name}
	if orchestrator == nil || runtime == nil {
		m.options.Logger.Info("multisim line removed before it was created", attrs...)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.options.CleanupTimeout)
	defer cancel()
	if err := runtime.Remove(ctx, item.sessionID); err != nil && !errors.Is(err, vowifiruntime.ErrNotRegistered) && !errors.Is(err, vowifiruntime.ErrClosed) {
		m.options.Logger.Warn("multisim line removal left cleanup incomplete", append(attrs, "error", err)...)
		return
	}
	m.options.Logger.Info("multisim line removed", attrs...)
}

// Construction can fail independently (for example a temporary database read).
// Retry only construction here; once registered, runtime.Manager owns all
// session retries and failure notifications for this line.
func (m *Manager) initializeLine(g *group, config Config, runtime *vowifiruntime.Manager, item *line) {
	defer g.initializers.Done()
	defer close(item.initialized)
	cycleCtx := item.ctx
	retry := m.options.RetryInitial
	if retry <= 0 {
		retry = 2 * time.Second
	}
	maximum := m.options.RetryMaximum
	if maximum <= 0 {
		maximum = 30 * time.Second
	}
	if maximum < retry {
		maximum = retry
	}
	for {
		if cycleCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(cycleCtx, m.options.OperationTimeout)
		orchestrator, err := m.options.Factory(ctx, config, item.profile, item.sessionID)
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		if err == nil && orchestrator == nil {
			err = errors.New("nil orchestrator")
		}
		if err == nil && orchestrator.State().DeviceID != item.sessionID {
			err = errors.New("factory returned wrong session ID")
		}
		if err == nil {
			err = runtime.Register(orchestrator)
		}
		if err != nil {
			if orchestrator != nil {
				_ = orchestrator.Close(context.Background())
				_ = orchestrator.WaitCleanup(context.Background())
			}
			if cycleCtx.Err() != nil {
				return
			}
			m.mu.Lock()
			item.lastError = "line initialization failed"
			m.mu.Unlock()
			m.options.Logger.Warn("multisim line initialization failed; retrying",
				"device_id", config.DeviceID, "session_id", item.sessionID,
				"profile_suffix", profileSuffix(item.profile.ICCID), "profile_name", item.profile.Name,
				"retry_in", retry, "error", err)
		} else {
			m.mu.Lock()
			item.orchestrator = orchestrator
			item.lastError = ""
			m.mu.Unlock()
			m.options.Logger.Info("multisim line created",
				"device_id", config.DeviceID, "session_id", item.sessionID,
				"profile_suffix", profileSuffix(item.profile.ICCID), "profile_name", item.profile.Name)
			if cycleCtx.Err() != nil {
				return
			}
			// Close may race after this check, but runtime's own closed gate prevents
			// any newly queued operation from outliving its shutdown.
			if _, err := runtime.RequestEnabled(item.sessionID, true); err != nil {
				m.mu.Lock()
				item.lastError = "line enable could not be queued"
				m.mu.Unlock()
			}
			return
		}
		timer := time.NewTimer(retry)
		select {
		case <-cycleCtx.Done():
			timer.Stop()
			return
		case <-item.retryWake:
			timer.Stop()
		case <-timer.C:
		}
		if retry > maximum/2 {
			retry = maximum
		} else {
			retry *= 2
		}
	}
}

func (m *Manager) setPhase(g *group, phase string, busy bool, lastError string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g.state.Phase != phase || lastError != "" {
		attrs := []any{"device_id", g.config.DeviceID, "phase_from", g.state.Phase, "phase_to", phase, "busy", busy}
		if lastError != "" {
			attrs = append(attrs, "error", lastError)
			m.options.Logger.Warn("multisim group phase changed", attrs...)
		} else {
			m.options.Logger.Info("multisim group phase changed", attrs...)
		}
	}
	g.state.Phase = phase
	g.state.Busy = busy
	g.state.LastError = lastError
	g.state.UpdatedAt = time.Now().UTC()
}

func (m *Manager) nextPrepareRetry(previous time.Duration) time.Duration {
	if previous <= 0 {
		return m.options.PrepareRetryInitial
	}
	if previous > m.options.PrepareRetryMaximum/2 {
		return m.options.PrepareRetryMaximum
	}
	return previous * 2
}

// waitPrepareRetry returns when the backoff elapses or the group's desired
// state changes. A stale wake left by an earlier request does not end it.
func (m *Manager) waitPrepareRetry(g *group, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case <-g.wake:
			m.mu.Lock()
			changed := g.next != nil || g.stopRequested || m.closed
			m.mu.Unlock()
			if changed {
				return
			}
		}
	}
}

func (m *Manager) waitRetry(g *group) bool {
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return false
	}
	<-g.wake
	m.mu.Lock()
	closed = m.closed
	m.mu.Unlock()
	return !closed
}

func (m *Manager) closeLines(g *group) error {
	m.mu.Lock()
	items := append([]*line(nil), g.lines...)
	m.mu.Unlock()
	var errs []error
	for _, item := range items {
		if item.orchestrator != nil {
			if err := item.orchestrator.Close(context.Background()); err != nil && !errors.Is(err, vowifi.ErrCleanupIncomplete) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errors.Join(errs...), m.waitLineCleanup(g))
}

func (m *Manager) waitLineCleanup(g *group) error {
	m.mu.Lock()
	var orchestrators []*vowifi.Orchestrator
	for _, item := range g.lines {
		if item.orchestrator != nil {
			orchestrators = append(orchestrators, item.orchestrator)
		}
	}
	m.mu.Unlock()
	var errs []error
	for _, orchestrator := range orchestrators {
		if err := orchestrator.WaitCleanup(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	m.closed = true
	for _, g := range m.groups {
		if !g.owned {
			continue
		}
		g.next = nil
		g.stopRequested = true
		g.state.Enabled = false
		if g.cancel != nil {
			g.cancel()
		}
		signal(g.wake)
	}
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.groups {
		if g.owned {
			return errors.New("multisim: shutdown left a reader owned after incomplete restoration")
		}
	}
	return nil
}

func cloneConfig(config Config) Config {
	config.Profiles = append([]Profile(nil), config.Profiles...)
	return config
}
func signal(channel chan struct{}) {
	if channel != nil {
		select {
		case channel <- struct{}{}:
		default:
		}
	}
}

// Refresh requests a protected re-registration on the existing IMS session.
// It does not reconnect the tunnel or alter network-provided expiry timers.
func (m *Manager) Refresh(deviceID, iccid string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	if g.state.Busy || g.state.Phase != "running" {
		m.mu.Unlock()
		return ErrOperationInProgress
	}
	var item *line
	for _, candidate := range g.lines {
		if candidate.profile.ICCID == iccid {
			item = candidate
			break
		}
	}
	if item == nil || item.orchestrator == nil {
		m.mu.Unlock()
		return ErrNotRegistered
	}
	if item.refreshPending {
		m.mu.Unlock()
		return ErrOperationInProgress
	}
	if !item.orchestrator.State().IMSReady {
		m.mu.Unlock()
		return vowifi.ErrIMSNotRegistered
	}
	item.refreshPending = true
	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	g.actions.Add(1)
	orchestrator := item.orchestrator
	m.mu.Unlock()
	logger := m.options.Logger.With("session_id", item.sessionID, "profile_suffix", profileSuffix(item.profile.ICCID))
	logger.Info("multisim refresh_requested")
	go func() {
		defer g.actions.Done()
		defer cancel()
		started := time.Now()
		err := orchestrator.RefreshRegistration(ctx)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			logger.Warn("multisim refresh_failed", "total_ms", time.Since(started).Milliseconds(), "error_class", safeErrorClass(err))
		} else {
			logger.Info("multisim refresh_finished", "total_ms", time.Since(started).Milliseconds())
		}
		m.mu.Lock()
		item.refreshPending = false
		m.mu.Unlock()
	}()
	return nil
}

// SendSMS submits one SMS through the line selected by ICCID or session ID.
// An empty selector is accepted only when the group has exactly one line. The
// group's own restoration waits for the submission, as it does for Refresh.
func (m *Manager) SendSMS(ctx context.Context, deviceID, selector string, request vowifi.SMSSubmitRequest) (vowifi.SMSSubmitResult, LineIdentity, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return vowifi.SMSSubmitResult{}, LineIdentity{}, ErrClosed
	}
	g := m.groups[deviceID]
	if g == nil || !g.owned || !g.state.Enabled {
		m.mu.Unlock()
		return vowifi.SMSSubmitResult{}, LineIdentity{}, ErrNotRegistered
	}
	if g.state.Busy || g.state.Phase != "running" {
		m.mu.Unlock()
		return vowifi.SMSSubmitResult{}, LineIdentity{}, ErrOperationInProgress
	}
	selector = strings.TrimSpace(selector)
	var item *line
	if selector == "" {
		if len(g.lines) != 1 {
			m.mu.Unlock()
			return vowifi.SMSSubmitResult{}, LineIdentity{}, ErrLineRequired
		}
		item = g.lines[0]
	} else {
		for _, candidate := range g.lines {
			if candidate.profile.ICCID == selector || candidate.sessionID == selector {
				item = candidate
				break
			}
		}
	}
	if item == nil || item.orchestrator == nil {
		m.mu.Unlock()
		return vowifi.SMSSubmitResult{}, LineIdentity{}, ErrNotRegistered
	}
	orchestrator := item.orchestrator
	identity := LineIdentity{DeviceID: deviceID, SessionID: item.sessionID, ICCID: item.profile.ICCID}
	g.actions.Add(1)
	m.mu.Unlock()
	defer g.actions.Done()
	result, err := orchestrator.SendSMS(ctx, request)
	state := orchestrator.State()
	identity.IMSI = state.IMSI
	identity.PhoneNumber = state.PhoneNumber
	return result, identity, err
}
