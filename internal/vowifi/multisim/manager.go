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
	wake          chan struct{}
	runtime       *vowifiruntime.Manager
	lines         []*line
	initializers  sync.WaitGroup
	actions       sync.WaitGroup
}

type line struct {
	profile        Profile
	sessionID      string
	orchestrator   *vowifi.Orchestrator
	lastError      string
	retryWake      chan struct{}
	refreshPending bool
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
		if g.state.Busy {
			return ErrOperationInProgress
		}
		if !g.stopRequested && g.state.Phase == "running" && reflect.DeepEqual(g.config, config) {
			return nil
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

func (m *Manager) run(g *group, cycleCtx context.Context) {
	defer m.wg.Done()
	for {
		m.mu.Lock()
		config := cloneConfig(g.config)
		m.mu.Unlock()
		startErr := m.start(g, cycleCtx, config)
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
		g.lines = append(g.lines, &line{profile: profile, sessionID: LineID(config.DeviceID, profile.ICCID), retryWake: make(chan struct{}, 1)})
	}
	items := append([]*line(nil), g.lines...)
	g.initializers.Add(len(items))
	m.mu.Unlock()
	for _, item := range items {
		go m.initializeLine(g, cycleCtx, config, runtime, item)
	}
	return ctx.Err()
}

// Construction can fail independently (for example a temporary database read).
// Retry only construction here; once registered, runtime.Manager owns all
// session retries and failure notifications for this line.
func (m *Manager) initializeLine(g *group, cycleCtx context.Context, config Config, runtime *vowifiruntime.Manager, item *line) {
	defer g.initializers.Done()
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
