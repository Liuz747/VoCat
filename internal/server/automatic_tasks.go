package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"vocat/internal/device"
	"vocat/internal/exportproxy"
	"vocat/internal/loghub"
	"vocat/internal/store"
)

const (
	automaticTaskPollInterval = 5 * time.Second
	automaticTaskMaxRuntime   = 8 * time.Minute
	// Profile rotation dwell bounds: the time a profile stays online after its
	// VoWiFi tunnel is ready before the next profile is enabled.
	automaticRotationMinDwellSeconds = 2
	// automaticDevicePresenceWait covers the moment a modem is re-enumerating
	// after an eUICC REFRESH.
	automaticDevicePresenceWait      = 20 * time.Second
	automaticRotationMaxDwellSeconds = 86400
)

type automaticTaskRotationProfile struct {
	ICCID string `json:"iccid"`
	AID   string `json:"aid,omitempty"`
	// Name is the profile nickname captured when the task was saved (this
	// batch stores the phone number there); display only.
	Name string `json:"name,omitempty"`
}

type automaticTaskPayload struct {
	Phone           string                         `json:"phone,omitempty"`
	Message         string                         `json:"message,omitempty"`
	DurationSeconds int                            `json:"duration_seconds,omitempty"`
	Profiles        []automaticTaskRotationProfile `json:"profiles,omitempty"`
}

// rotationRunState pins the target profile for every attempt of one run: if
// attempt 1 switched the card but failed later (for example the VoWiFi wait
// timed out), attempt 2 must finish the same target instead of skipping a
// profile.
type rotationRunState struct {
	target *automaticTaskRotationProfile
}

type rotationRunContextKey struct{}

func withRotationRun(ctx context.Context, state *rotationRunState) context.Context {
	return context.WithValue(ctx, rotationRunContextKey{}, state)
}

func rotationRunFromContext(ctx context.Context) *rotationRunState {
	state, _ := ctx.Value(rotationRunContextKey{}).(*rotationRunState)
	return state
}

// nextRotationProfile picks the profile after the one currently enabled on
// the eUICC, wrapping around; an unknown or empty current ICCID restarts the
// cycle at the first entry.
func nextRotationProfile(profiles []automaticTaskRotationProfile, currentICCID string) (automaticTaskRotationProfile, error) {
	if len(profiles) == 0 {
		return automaticTaskRotationProfile{}, errors.New("rotation task has no profiles")
	}
	current := strings.TrimSpace(currentICCID)
	for index, profile := range profiles {
		if current != "" && strings.EqualFold(strings.TrimSpace(profile.ICCID), current) {
			return profiles[(index+1)%len(profiles)], nil
		}
	}
	return profiles[0], nil
}

func validateAutomaticTaskRotation(profiles []automaticTaskRotationProfile, intervalSeconds int) error {
	if len(profiles) < 2 {
		return errors.New("profile rotation needs at least two eSIM profiles")
	}
	seen := make(map[string]bool, len(profiles))
	for _, profile := range profiles {
		iccid := strings.TrimSpace(profile.ICCID)
		if iccid == "" {
			return errors.New("profile rotation entries need an ICCID")
		}
		if seen[strings.ToUpper(iccid)] {
			return errors.New("profile rotation lists the same ICCID twice")
		}
		seen[strings.ToUpper(iccid)] = true
	}
	if intervalSeconds < automaticRotationMinDwellSeconds || intervalSeconds > automaticRotationMaxDwellSeconds {
		return fmt.Errorf("profile rotation dwell must be %d-%d seconds", automaticRotationMinDwellSeconds, automaticRotationMaxDwellSeconds)
	}
	return nil
}

type automaticTaskExecutionError struct {
	err       error
	retryable bool
}

func (value automaticTaskExecutionError) Error() string { return value.err.Error() }
func (value automaticTaskExecutionError) Unwrap() error { return value.err }

type automaticTaskProgress func(string)

type automaticTaskEnvironmentSnapshot struct {
	config store.Device
	policy store.CardPolicy
}

type automaticTaskScheduler struct {
	server *Server
	ctx    context.Context
	mu     sync.Mutex
	queues map[string]chan store.AutomaticTaskRun
}

func (s *Server) StartAutomaticTasks(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	scheduler := &automaticTaskScheduler{server: s, ctx: ctx, queues: make(map[string]chan store.AutomaticTaskRun)}
	s.automaticTasks = scheduler
	queued, err := s.store.RecoverAutomaticTaskRuns(ctx, time.Now().UTC())
	if err != nil {
		s.logger.Warn("recover automatic tasks", "error", err)
	} else {
		for _, run := range queued {
			scheduler.enqueue(run)
		}
	}
	go scheduler.run()
}

func (scheduler *automaticTaskScheduler) run() {
	ticker := time.NewTicker(automaticTaskPollInterval)
	defer ticker.Stop()
	scheduler.claim()
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case <-ticker.C:
			scheduler.claim()
		}
	}
}

func (scheduler *automaticTaskScheduler) claim() {
	var runs []store.AutomaticTaskRun
	var err error
	if scheduler.server.developerActive(scheduler.ctx) {
		runs, err = scheduler.server.store.ClaimDueAutomaticTasks(scheduler.ctx, time.Now().UTC(), 50)
	} else {
		runs, err = scheduler.server.store.ClaimDueAvailableAutomaticTasks(scheduler.ctx, time.Now().UTC(), 50)
	}
	if err != nil {
		scheduler.server.logger.Warn("claim automatic tasks", "error", err)
		return
	}
	for _, run := range runs {
		scheduler.enqueue(run)
	}
}

func (scheduler *automaticTaskScheduler) enqueue(run store.AutomaticTaskRun) {
	deviceID := strings.TrimSpace(run.DeviceID)
	scheduler.mu.Lock()
	queue := scheduler.queues[deviceID]
	if queue == nil {
		queue = make(chan store.AutomaticTaskRun, 100)
		scheduler.queues[deviceID] = queue
		go scheduler.worker(deviceID, queue)
	}
	scheduler.mu.Unlock()
	select {
	case queue <- run:
	case <-scheduler.ctx.Done():
	}
}

func (scheduler *automaticTaskScheduler) worker(deviceID string, queue <-chan store.AutomaticTaskRun) {
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case run := <-queue:
			scheduler.execute(run)
		}
	}
}

func (scheduler *automaticTaskScheduler) execute(run store.AutomaticTaskRun) {
	task, err := scheduler.server.store.AutomaticTask(scheduler.ctx, run.TaskID)
	if err != nil {
		run.Status, run.Error, run.FinishedAt = "failed", err.Error(), time.Now().UTC()
		_ = scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run)
		return
	}
	if err := validateAutomaticTaskAvailability(scheduler.server.developerActive(scheduler.ctx), task.TaskType, task.Environment); err != nil {
		run.Status, run.Error, run.FinishedAt = "failed", err.Error(), time.Now().UTC()
		_ = scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run)
		return
	}
	run.Status, run.StartedAt = "running", time.Now().UTC()
	_ = scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run)
	var output string
	runState := &rotationRunState{}
	for attempt := 1; attempt <= task.RetryCount+1; attempt++ {
		run.Attempts = attempt
		run.Output = fmt.Sprintf("第 %d 次尝试：正在检查设备和 eSIM Profile", attempt)
		_ = scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run)
		progress := func(message string) {
			run.Output = fmt.Sprintf("第 %d 次尝试：%s", attempt, message)
			_ = scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run)
		}
		operationContext, cancel := context.WithTimeout(scheduler.ctx, automaticTaskMaxRuntime)
		output, err = scheduler.server.executeAutomaticTask(withRotationRun(operationContext, runState), task, progress)
		cancel()
		if err == nil {
			break
		}
		var executionError automaticTaskExecutionError
		if errors.As(err, &executionError) && !executionError.retryable {
			break
		}
		if attempt <= task.RetryCount {
			// A device error may contain the full AT command, including APN
			// credentials. The persisted run retains a user-facing outcome; logs
			// contain only non-sensitive execution metadata.
			scheduler.server.logger.Warn("automatic task attempt failed", "task_id", task.ID, "device_id", task.DeviceID, "attempt", attempt, "error", loghub.RedactString(err.Error()))
			select {
			case <-scheduler.ctx.Done():
				break
			case <-time.After(time.Duration(attempt*5) * time.Second):
			}
		}
	}
	run.FinishedAt = time.Now().UTC()
	if err == nil {
		run.Status, run.Output, run.Error = "success", output, ""
	} else {
		run.Status, run.Error = "failed", err.Error()
	}
	if updateErr := scheduler.server.store.UpdateAutomaticTaskRun(context.Background(), run); updateErr != nil {
		scheduler.server.logger.Warn("finish automatic task run", "run_id", run.ID, "error", updateErr)
	}
	if task.TaskType == "profile_rotation" && task.IntervalSeconds > 0 {
		// The dwell counts from the moment this run finished (tunnel ready or
		// failed), not from the previous slot, so back-to-back switches keep a
		// predictable online window per profile.
		next := run.FinishedAt.Add(time.Duration(task.IntervalSeconds) * time.Second)
		if rescheduleErr := scheduler.server.store.RescheduleAutomaticTask(context.Background(), task.ID, next); rescheduleErr != nil {
			scheduler.server.logger.Warn("reschedule rotation task", "task_id", task.ID, "error", rescheduleErr)
		}
	}
	if task.Notify {
		go scheduler.server.notifyAutomaticTask(context.Background(), task, run)
	}
}

func (s *Server) executeAutomaticTask(ctx context.Context, task store.AutomaticTask, progress automaticTaskProgress) (output string, err error) {
	if err := validateAutomaticTaskAvailability(s.developerActive(ctx), task.TaskType, task.Environment); err != nil {
		return "", automaticTaskExecutionError{err: err, retryable: false}
	}
	progress("正在检查设备和 eSIM Profile")
	var payload automaticTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return "", automaticTaskExecutionError{err: fmt.Errorf("decode task payload: %w", err), retryable: false}
	}
	var rotationTarget automaticTaskRotationProfile
	if task.TaskType == "profile_rotation" {
		target, err := s.resolveRotationTarget(ctx, task, payload)
		if err != nil {
			return "", err
		}
		rotationTarget = target
		task.ProfileICCID, task.ProfileAID = target.ICCID, target.AID
	}
	switchStarted := time.Now()
	config, entry, physicalID, err := s.ensureAutomaticTaskProfile(ctx, task, progress)
	if err != nil {
		return "", err
	}
	switchDuration := time.Since(switchStarted)
	iccid := strings.TrimSpace(task.ProfileICCID)
	policy, policyErr := s.store.CardPolicy(ctx, iccid)
	if errors.Is(policyErr, store.ErrNotFound) {
		policy = defaultCardPolicy(iccid)
	} else if policyErr != nil {
		return "", fmt.Errorf("read saved card policy: %w", policyErr)
	}
	snapshot := automaticTaskEnvironmentSnapshot{config: config, policy: policy}
	actionCompleted := false
	defer func() {
		if task.TaskType == "profile_rotation" {
			// Rotation's end state is "this profile stays online over VoWiFi";
			// restoring the pre-task policy would tear the fresh tunnel down.
			return
		}
		progress("正在恢复该 Profile 原先保存的卡策略")
		if restoreErr := s.restoreAutomaticTaskEnvironment(physicalID, snapshot); restoreErr != nil {
			if err == nil && actionCompleted {
				output = ""
				err = automaticTaskExecutionError{err: fmt.Errorf("task completed but card policy restoration failed: %w", restoreErr), retryable: false}
			} else if err == nil {
				err = fmt.Errorf("restore card policy: %w", restoreErr)
			} else {
				err = fmt.Errorf("%w; card policy restoration also failed: %v", err, restoreErr)
			}
		}
	}()
	tunnelStarted := time.Now()
	if err := s.prepareAutomaticTaskEnvironment(ctx, &config, entry, physicalID, task, progress); err != nil {
		return "", err
	}
	tunnelDuration := time.Since(tunnelStarted)
	switch task.TaskType {
	case "profile_rotation":
		// The switch and the VoWiFi readiness wait above are the whole job:
		// once the tunnel is registered the carrier delivers queued SMS.
		output = fmt.Sprintf("已切换到 %s（…%s），VoWiFi 隧道已就绪（%s）", firstNonEmpty(strings.TrimSpace(rotationTarget.Name), rotationTarget.ICCID), lastDigits(rotationTarget.ICCID, 4), formatRotationTiming(switchDuration, tunnelDuration))
		s.logger.Info("automatic profile rotation timing", "device_id", task.DeviceID, "iccid", rotationTarget.ICCID,
			"switch_ms", switchDuration.Milliseconds(), "tunnel_ms", tunnelDuration.Milliseconds())
		err = nil
	case "sms":
		progress("正在发送短信")
		output, err = s.executeAutomaticSMS(ctx, task, payload)
	case "call":
		progress("正在发起通话")
		output, err = s.executeAutomaticCall(ctx, task, payload)
	case "public_ip":
		progress("蜂窝数据已连接，正在查询漫游公网 IP")
		output, err = s.executeAutomaticPublicIP(ctx, config, task.ProfileICCID)
	default:
		return "", fmt.Errorf("unsupported automatic task type %q", task.TaskType)
	}
	actionCompleted = err == nil
	return output, err
}

// resolveRotationTarget reads the ICCID currently enabled on the device and
// picks the next entry of the task's rotation list.
func (s *Server) resolveRotationTarget(ctx context.Context, task store.AutomaticTask, payload automaticTaskPayload) (automaticTaskRotationProfile, error) {
	if err := validateAutomaticTaskRotation(payload.Profiles, task.IntervalSeconds); err != nil {
		return automaticTaskRotationProfile{}, automaticTaskExecutionError{err: err, retryable: false}
	}
	runState := rotationRunFromContext(ctx)
	if runState != nil && runState.target != nil {
		return *runState.target, nil
	}
	config, err := s.store.Device(ctx, task.DeviceID)
	if err != nil {
		return automaticTaskRotationProfile{}, fmt.Errorf("read device: %w", err)
	}
	entry, physicalID, err := s.waitForPhysicalDevice(ctx, config, automaticDevicePresenceWait)
	if err != nil {
		return automaticTaskRotationProfile{}, err
	}
	// The cached snapshot can lag behind a switch made by a failed earlier run;
	// read the ICCID the modem reports now.
	currentICCID := entry.Snapshot.ICCID
	if snapshot, refreshErr := s.devices.Refresh(ctx, physicalID); refreshErr == nil && strings.TrimSpace(snapshot.ICCID) != "" {
		currentICCID = snapshot.ICCID
	}
	target, err := nextRotationProfile(payload.Profiles, currentICCID)
	if err != nil {
		return automaticTaskRotationProfile{}, err
	}
	if runState != nil {
		pinned := target
		runState.target = &pinned
	}
	return target, nil
}

// formatRotationTiming renders the two halves of a rotation run for the run
// output: the eUICC/modem switch and the VoWiFi tunnel + IMS registration.
func formatRotationTiming(switchDuration, tunnelDuration time.Duration) string {
	return fmt.Sprintf("换档 %.1f s · 隧道 %.1f s", switchDuration.Seconds(), tunnelDuration.Seconds())
}

func lastDigits(value string, count int) string {
	value = strings.TrimSpace(value)
	if len(value) <= count {
		return value
	}
	return value[len(value)-count:]
}

// waitForPhysicalDevice rides out the short window in which the modem is not
// enumerated: an eUICC REFRESH (and the SIM re-initialisation behind it) makes
// the device disappear from the manager's list for a moment, which is not the
// same as a device that is really gone.
func (s *Server) waitForPhysicalDevice(ctx context.Context, config store.Device, timeout time.Duration) (device.Device, string, error) {
	deadline := time.Now().Add(timeout)
	for {
		entry, physicalID, present := s.physicalForConfig(config)
		if present && entry.Snapshot != nil {
			return entry, physicalID, nil
		}
		if !time.Now().Before(deadline) {
			return device.Device{}, "", errors.New("configured device is offline")
		}
		select {
		case <-ctx.Done():
			return device.Device{}, "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Server) ensureAutomaticTaskProfile(ctx context.Context, task store.AutomaticTask, progress automaticTaskProgress) (store.Device, device.Device, string, error) {
	config, err := s.store.Device(ctx, task.DeviceID)
	if err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("read device: %w", err)
	}
	if err := validateAutomaticTaskDeviceCapabilities(config, task.TaskType, task.Environment); err != nil {
		return store.Device{}, device.Device{}, "", err
	}
	entry, physicalID, err := s.waitForPhysicalDevice(ctx, config, automaticDevicePresenceWait)
	if err != nil {
		return store.Device{}, device.Device{}, "", err
	}
	if strings.EqualFold(strings.TrimSpace(entry.Snapshot.ICCID), strings.TrimSpace(task.ProfileICCID)) {
		return config, entry, physicalID, nil
	}
	progress("正在切换到任务指定的 eSIM Profile")
	// Mirror the manual switch API: keep the SM/ME scanner's subscription
	// identity stable, pause VoWiFi reconciliation, and stop the live runtime
	// so the old profile de-registers cleanly. A switch without that
	// de-REGISTER leaves a stale IMS binding and the carrier then parks MT SMS
	// on its retry timer (minutes) instead of pushing them on the next
	// REGISTER (measured <0.4 s), see DEVICE-REPORT §25.6.
	s.smsSyncMu.Lock()
	defer s.smsSyncMu.Unlock()
	desiredData := config.NetworkEnabled && !config.VoWiFiEnabled
	dataRuntime := s.cellularDataRuntime()
	s.clearPublicIP(config.ID)
	dataRuntime.invalidateWithMaintenancePhase(config.ID, desiredData, "recovering", "", "profile_switching")
	switchCompleted := false
	defer func() {
		if !switchCompleted {
			dataRuntime.invalidate(config.ID, desiredData, "failed", "automatic eSIM profile switch did not complete")
		}
	}()
	if maintenance, ok := s.vowifi.(VoWiFiMaintenanceController); ok {
		if err := maintenance.BeginMaintenance(config.ID); err != nil {
			return store.Device{}, device.Device{}, "", fmt.Errorf("prepare VoWiFi for profile switch: %w", err)
		}
		defer maintenance.EndMaintenance(config.ID)
	}
	stageStarted := time.Now()
	stage := func(name string) {
		s.logger.Info("automatic profile switch stage", "device_id", config.ID, "stage", name, "stage_ms", time.Since(stageStarted).Milliseconds())
		stageStarted = time.Now()
	}
	progress("正在注销当前 Profile 的 VoWiFi 注册")
	if err := s.quiesceVoWiFiForProfileSwitch(ctx, config.ID); err != nil {
		return store.Device{}, device.Device{}, "", err
	}
	stage("quiesce_vowifi")
	if _, err := s.devices.SetFlight(ctx, physicalID, true); err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("enter airplane mode before profile switch: %w", err)
	}
	stage("set_flight")
	progress("正在启用下一个 eSIM Profile")
	if err := s.devices.ESIMSwitchProfile(ctx, physicalID, task.ProfileICCID, task.ProfileAID); err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("switch eSIM profile: %w", err)
	}
	stage("esim_switch_profile")
	if _, err := s.devices.SetFlight(ctx, physicalID, true); err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("keep airplane mode after profile switch: %w", err)
	}
	stage("set_flight_again")
	entry, physicalID, err = s.waitForPhysicalDevice(ctx, config, automaticDevicePresenceWait)
	if err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("device did not recover after profile switch: %w", err)
	}
	snapshot, err := s.devices.Refresh(ctx, physicalID)
	if err != nil {
		return store.Device{}, device.Device{}, "", fmt.Errorf("verify switched profile: %w", err)
	}
	stage("snapshot_refresh")
	if !strings.EqualFold(strings.TrimSpace(snapshot.ICCID), strings.TrimSpace(task.ProfileICCID)) {
		return store.Device{}, device.Device{}, "", fmt.Errorf("profile verification failed: current ICCID is %s", firstNonEmpty(snapshot.ICCID, "unavailable"))
	}
	dataRuntime.invalidate(config.ID, false, "disabled", "")
	switchCompleted = true
	entry.Snapshot = &snapshot
	return config, entry, physicalID, nil
}

func (s *Server) prepareAutomaticTaskEnvironment(ctx context.Context, config *store.Device, entry device.Device, physicalID string, task store.AutomaticTask, progress automaticTaskProgress) error {
	iccid := strings.TrimSpace(task.ProfileICCID)
	if task.Environment == "vowifi" {
		progress("正在准备 VoWiFi 执行环境")
		if task.TaskType == "public_ip" {
			return errors.New("public IP tasks cannot run over VoWiFi")
		}
		desiredData := config.NetworkEnabled && !config.VoWiFiEnabled
		dataRuntime := s.cellularDataRuntime()
		s.clearPublicIP(config.ID)
		dataRuntime.invalidateWithMaintenancePhase(config.ID, desiredData, "recovering", "", "vowifi")
		if _, err := s.devices.SetFlight(ctx, physicalID, true); err != nil {
			return fmt.Errorf("enable airplane mode for VoWiFi: %w", err)
		}
		config.VoWiFiEnabled, config.NetworkEnabled = true, false
		if err := s.store.UpsertDevice(ctx, *config); err != nil {
			return err
		}
		dataRuntime.invalidate(config.ID, false, "disabled", "")
		policy, policyErr := s.store.CardPolicy(ctx, iccid)
		if errors.Is(policyErr, store.ErrNotFound) {
			policy = defaultCardPolicy(iccid)
			policyErr = nil
		}
		if policyErr != nil {
			return policyErr
		}
		policy.NetworkEnabled = false
		policy.VoWiFiEnabled = true
		policy.AirplaneEnabled = true
		policy.Source = "automatic_task"
		if err := s.store.UpsertCardPolicy(ctx, policy); err != nil {
			return err
		}
		if s.vowifi == nil {
			return errors.New("VoWiFi runtime is unavailable")
		}
		state, stateErr := s.vowifi.State(config.ID)
		stateMatchesCard := state.ICCID == "" || strings.EqualFold(strings.TrimSpace(state.ICCID), iccid)
		if stateErr == nil && stateMatchesCard && state.IMSReady && (task.TaskType != "sms" || state.SMSReady) {
			return nil
		}
		if stateErr == nil && state.Enabled {
			_, stateErr = s.vowifi.RequestReconnect(config.ID)
		} else {
			_, stateErr = s.vowifi.RequestEnabled(config.ID, true)
		}
		if stateErr != nil {
			return fmt.Errorf("start VoWiFi: %w", stateErr)
		}
		return s.waitAutomaticVoWiFi(ctx, config.ID, iccid, task.TaskType == "sms" || task.TaskType == "profile_rotation")
	}
	if s.vowifi != nil {
		if state, stateErr := s.vowifi.State(config.ID); stateErr == nil && (state.Enabled || state.Active) {
			if _, stateErr = s.vowifi.RequestEnabled(config.ID, false); stateErr != nil {
				return fmt.Errorf("stop VoWiFi: %w", stateErr)
			}
			if err := s.waitAutomaticVoWiFiStopped(ctx, config.ID); err != nil {
				return err
			}
		}
	}
	progress("正在开启蜂窝无线并启用自动选网")
	config.VoWiFiEnabled = false
	config.NetworkEnabled = task.TaskType == "public_ip"
	if err := s.store.UpsertDevice(ctx, *config); err != nil {
		return err
	}
	policy, policyErr := s.store.CardPolicy(ctx, iccid)
	if errors.Is(policyErr, store.ErrNotFound) {
		policy = defaultCardPolicy(iccid)
		policy.APN = config.APN
	} else if policyErr != nil {
		return policyErr
	}
	policy.NetworkEnabled = config.NetworkEnabled
	policy.VoWiFiEnabled = false
	policy.AirplaneEnabled = false
	policy.Source = "automatic_task"
	if err := s.store.UpsertCardPolicy(ctx, policy); err != nil {
		return err
	}
	if task.TaskType != "public_ip" {
		if _, err := s.applyCellularData(ctx, config.ID, physicalID, s.cardNetworkRequest(ctx, physicalID, *config, policy, false)); err != nil {
			s.logger.Warn("automatic task could not stop unused cellular data", "device_id", config.ID)
		}
	}
	if _, err := s.devices.SetFlight(ctx, physicalID, false); err != nil {
		return fmt.Errorf("enable cellular radio: %w", err)
	}
	if _, err := s.devices.SetOperatorSelection(ctx, physicalID, true, "", nil); err != nil {
		return fmt.Errorf("enable automatic network selection: %w", err)
	}
	if _, err := s.devices.ReRegisterOperator(ctx, physicalID); err != nil {
		return fmt.Errorf("re-register cellular network: %w", err)
	}
	progress("正在搜索并注册蜂窝网络（漫游注册可能需要数分钟）")
	if err := s.waitAutomaticCellular(ctx, physicalID, task.TaskType == "public_ip"); err != nil {
		return err
	}
	if task.TaskType == "public_ip" {
		progress("已注册蜂窝网络，正在建立数据连接")
		if _, err := s.applyCellularData(ctx, config.ID, physicalID, s.cardNetworkRequest(ctx, physicalID, *config, policy, true)); err != nil {
			return fmt.Errorf("start roaming data: %w", err)
		}
	}
	return nil
}

func (s *Server) waitAutomaticVoWiFi(ctx context.Context, deviceID, iccid string, requireSMS bool) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		state, err := s.vowifi.State(deviceID)
		if err == nil && state.IMSReady && (!requireSMS || state.SMSReady) && (state.ICCID == "" || strings.EqualFold(state.ICCID, iccid)) {
			return nil
		}
		// A "cleanup_warning" is the previous profile's best-effort
		// de-registration failing; the runtime is idle and our enable request
		// is still queued, so it must not be mistaken for a failed start.
		if err == nil && state.LastError != "" && state.LastErrorClass != "cleanup_warning" && !state.Active && !state.Enabled {
			return errors.New(state.LastError)
		}
		select {
		case <-ctx.Done():
			if err == nil && state.LastError != "" {
				return fmt.Errorf("wait for VoWiFi readiness: %s", state.LastError)
			}
			return fmt.Errorf("wait for VoWiFi readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Server) waitAutomaticVoWiFiStopped(ctx context.Context, deviceID string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		state, err := s.vowifi.State(deviceID)
		if err != nil || (!state.Active && !state.Enabled) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for VoWiFi shutdown: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Server) waitAutomaticCellular(ctx context.Context, physicalID string, requirePacketAttach bool) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	stableSamples := 0
	for {
		snapshot, err := s.devices.Refresh(ctx, physicalID)
		registered := err == nil && (snapshot.RegistrationStatus == 1 || snapshot.RegistrationStatus == 5)
		if registered && (!requirePacketAttach || snapshot.PSAttached) {
			stableSamples++
			if stableSamples >= 2 {
				return nil
			}
		} else {
			stableSamples = 0
		}
		if err == nil && snapshot.RegistrationStatus == 3 {
			return errors.New("cellular network registration was denied")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for cellular registration: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Server) executeAutomaticSMS(ctx context.Context, task store.AutomaticTask, payload automaticTaskPayload) (string, error) {
	body, _ := json.Marshal(map[string]any{"device_id": task.DeviceID, "phone": payload.Phone, "message": payload.Message})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/sms/send", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	s.handleSMSSend(recorder, request)
	if recorder.Code < 200 || recorder.Code >= 300 {
		failure := fmt.Errorf("send SMS failed (HTTP %d): %s", recorder.Code, compactAutomaticResponse(recorder.Body.Bytes()))
		// Once any part reached the modem/IMS transaction, retrying the whole
		// message could deliver a duplicate. Preparation failures remain safe to
		// retry according to the configured count.
		return "", automaticTaskExecutionError{err: failure, retryable: automaticSMSRetrySafe(recorder.Body.Bytes())}
	}
	return "短信已提交到 " + payload.Phone, nil
}

func automaticSMSRetrySafe(body []byte) bool {
	var payload struct {
		Data struct {
			PartsAttempted int   `json:"parts_attempted"`
			PartsAccepted  int   `json:"parts_accepted"`
			RetrySafe      *bool `json:"retry_safe"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	if payload.Data.RetrySafe != nil {
		return *payload.Data.RetrySafe
	}
	return payload.Data.PartsAttempted == 0 && payload.Data.PartsAccepted == 0
}

func (s *Server) executeAutomaticCall(ctx context.Context, task store.AutomaticTask, payload automaticTaskPayload) (string, error) {
	config, err := s.store.Device(ctx, task.DeviceID)
	if err != nil {
		return "", err
	}
	_, physicalID, present := s.physicalForConfig(config)
	if !present {
		return "", errors.New("configured device is offline")
	}
	body, _ := json.Marshal(map[string]any{"number": payload.Phone, "duration_seconds": payload.DurationSeconds})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/devices/calls/dial", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	s.handleCallAction(recorder, request, config, physicalID, "dial")
	if recorder.Code < 200 || recorder.Code >= 300 {
		return "", fmt.Errorf("dial failed (HTTP %d): %s", recorder.Code, compactAutomaticResponse(recorder.Body.Bytes()))
	}
	return fmt.Sprintf("已拨打 %s，将在 %d 秒后自动挂断", payload.Phone, payload.DurationSeconds), nil
}

func (s *Server) executeAutomaticPublicIP(ctx context.Context, config store.Device, iccid string) (string, error) {
	if strings.TrimSpace(config.Interface) == "" {
		return "", errors.New("device has no cellular network interface")
	}
	info, err := exportproxy.LookupPublicIP(ctx, config.Interface)
	if err != nil {
		return "", fmt.Errorf("detect roaming public IP: %w", err)
	}
	s.savePublicIP(config.ID, iccid, info)
	return strings.TrimSpace(fmt.Sprintf("公网 IP %s · %s %s", info.IP, info.CountryCode, info.Region)), nil
}

func (s *Server) restoreAutomaticTaskEnvironment(physicalID string, snapshot automaticTaskEnvironmentSnapshot) error {
	cleanupContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config, policy := snapshot.config, snapshot.policy
	desiredNetwork := policy.NetworkEnabled && !policy.VoWiFiEnabled && !policy.AirplaneEnabled
	config.APN = policy.APN
	config.NetworkEnabled = desiredNetwork
	config.VoWiFiEnabled = policy.VoWiFiEnabled
	var restoreErrors []error
	if err := s.store.UpsertCardPolicy(cleanupContext, policy); err != nil {
		restoreErrors = append(restoreErrors, fmt.Errorf("persist card policy: %w", err))
	}
	if err := s.store.UpsertDevice(cleanupContext, config); err != nil {
		restoreErrors = append(restoreErrors, fmt.Errorf("persist device policy: %w", err))
	}
	if config.DeviceType == store.DeviceTypeUSBSIMReader {
		if s.vowifi == nil {
			return errors.Join(append(restoreErrors, errors.New("VoWiFi runtime is unavailable"))...)
		}
		state, stateErr := s.vowifi.State(config.ID)
		if policy.VoWiFiEnabled {
			if stateErr == nil && state.Enabled {
				_, stateErr = s.vowifi.RequestReconnect(config.ID)
			} else {
				_, stateErr = s.vowifi.RequestEnabled(config.ID, true)
			}
		} else if stateErr == nil && (state.Enabled || state.Active) {
			_, stateErr = s.vowifi.RequestEnabled(config.ID, false)
		}
		if stateErr != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("restore reader VoWiFi: %w", stateErr))
		}
		return errors.Join(restoreErrors...)
	}

	if policy.VoWiFiEnabled {
		if _, err := s.applyCellularData(cleanupContext, config.ID, physicalID, s.cardNetworkRequest(cleanupContext, physicalID, config, policy, false)); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("stop cellular data: %w", err))
		}
		if _, err := s.devices.SetFlight(cleanupContext, physicalID, true); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("restore airplane mode: %w", err))
		}
		if s.vowifi == nil {
			restoreErrors = append(restoreErrors, errors.New("VoWiFi runtime is unavailable"))
		} else if state, stateErr := s.vowifi.State(config.ID); stateErr == nil && state.Enabled {
			if _, err := s.vowifi.RequestReconnect(config.ID); err != nil {
				restoreErrors = append(restoreErrors, fmt.Errorf("restore VoWiFi: %w", err))
			}
		} else if _, err := s.vowifi.RequestEnabled(config.ID, true); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("restore VoWiFi: %w", err))
		}
		return errors.Join(restoreErrors...)
	}
	if s.vowifi != nil {
		if state, stateErr := s.vowifi.State(config.ID); stateErr == nil && (state.Enabled || state.Active) {
			if _, err := s.vowifi.RequestEnabled(config.ID, false); err != nil {
				restoreErrors = append(restoreErrors, fmt.Errorf("stop VoWiFi: %w", err))
			}
		}
	}
	if policy.AirplaneEnabled {
		if _, err := s.applyCellularData(cleanupContext, config.ID, physicalID, s.cardNetworkRequest(cleanupContext, physicalID, config, policy, false)); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("stop cellular data: %w", err))
		}
		if _, err := s.devices.SetFlight(cleanupContext, physicalID, true); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("restore airplane mode: %w", err))
		}
		return errors.Join(restoreErrors...)
	}
	if !desiredNetwork {
		if _, err := s.applyCellularData(cleanupContext, config.ID, physicalID, s.cardNetworkRequest(cleanupContext, physicalID, config, policy, false)); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("stop cellular data: %w", err))
		}
	}
	if _, err := s.devices.SetFlight(cleanupContext, physicalID, false); err != nil {
		restoreErrors = append(restoreErrors, fmt.Errorf("restore cellular radio: %w", err))
	}
	if desiredNetwork {
		if _, err := s.applyCellularData(cleanupContext, config.ID, physicalID, s.cardNetworkRequest(cleanupContext, physicalID, config, policy, true)); err != nil {
			restoreErrors = append(restoreErrors, fmt.Errorf("restore cellular data: %w", err))
		}
	}
	return errors.Join(restoreErrors...)
}

func (s *Server) cardNetworkRequest(
	ctx context.Context,
	physicalID string,
	config store.Device,
	policy store.CardPolicy,
	enabled bool,
) device.NetworkRequest {
	request := device.NetworkRequest{
		Enabled: enabled, APN: policy.APN, IPVersion: policy.IPVersion, Backend: config.DeviceBackend,
	}
	if request.IPVersion == "" {
		request.IPVersion = "IPV4V6"
	}
	profile, err := s.store.CardAPNProfileByAPN(ctx, policy.ICCID, policy.APN, policy.IPVersion)
	if err != nil {
		return request
	}
	request.Username = profile.Username
	request.Password = profile.Password
	request.Authentication = profile.AuthType
	if entry, getErr := s.devices.Get(physicalID); getErr == nil && entry.Snapshot != nil &&
		entry.Snapshot.RegistrationStatus == 5 && profile.RoamingIPVersion != "" {
		request.IPVersion = profile.RoamingIPVersion
	}
	return request
}

func compactAutomaticResponse(body []byte) string {
	var payload map[string]any
	if json.Unmarshal(body, &payload) == nil {
		if apiErr, ok := payload["error"].(map[string]any); ok {
			return firstNonEmpty(fmt.Sprint(apiErr["message"]), fmt.Sprint(apiErr["code"]), "request failed")
		}
	}
	return strings.TrimSpace(string(body))
}

func (s *Server) routeAutomaticTasksAPI(w http.ResponseWriter, r *http.Request, cleanPath string) bool {
	segments := splitAPIPath(cleanPath)
	if len(segments) == 0 || segments[0] != "automatic-tasks" {
		return false
	}
	if len(segments) == 1 {
		s.handleAutomaticTasks(w, r)
		return true
	}
	if len(segments) == 2 && segments[1] == "runs" {
		s.handleAutomaticTaskRuns(w, r)
		return true
	}
	id, err := strconv.ParseInt(segments[1], 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_task_id", "automatic task ID is invalid")
		return true
	}
	if len(segments) == 2 {
		s.handleAutomaticTask(w, r, id)
		return true
	}
	if len(segments) == 3 && segments[2] == "run" {
		s.handleAutomaticTaskRunNow(w, r, id)
		return true
	}
	writeError(w, http.StatusNotFound, "not_found", "automatic task endpoint not found")
	return true
}

func (s *Server) handleAutomaticTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		tasks, err := s.store.ListAutomaticTasks(r.Context())
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if !s.developerActive(r.Context()) {
			visible := tasks[:0]
			for _, task := range tasks {
				if validateAutomaticTaskAvailability(false, task.TaskType, task.Environment) == nil {
					visible = append(visible, task)
				}
			}
			tasks = visible
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"tasks": tasks}})
	case http.MethodPost:
		task, err := s.decodeAutomaticTask(r, 0)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_automatic_task", err.Error())
			return
		}
		saved, err := s.store.SaveAutomaticTask(r.Context(), task)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"data": saved})
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *Server) handleAutomaticTask(w http.ResponseWriter, r *http.Request, id int64) {
	switch r.Method {
	case http.MethodPut:
		task, err := s.decodeAutomaticTask(r, id)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_automatic_task", err.Error())
			return
		}
		saved, err := s.store.SaveAutomaticTask(r.Context(), task)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": saved})
	case http.MethodDelete:
		if err := s.store.DeleteAutomaticTask(r.Context(), id); err != nil {
			s.writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"deleted": true}})
	default:
		w.Header().Set("Allow", "PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *Server) handleAutomaticTaskRuns(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	query := r.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))
	offset, _ := strconv.Atoi(query.Get("offset"))
	var runs []store.AutomaticTaskRun
	var total int
	var err error
	if s.developerActive(r.Context()) {
		runs, total, err = s.store.ListAutomaticTaskRunsPaginated(r.Context(), limit, offset)
	} else {
		runs, total, err = s.store.ListAvailableAutomaticTaskRunsPaginated(r.Context(), limit, offset)
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"runs": runs, "total": total}})
}

func (s *Server) handleAutomaticTaskRunNow(w http.ResponseWriter, r *http.Request, id int64) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if s.automaticTasks == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler_unavailable", "automatic task scheduler is unavailable")
		return
	}
	task, err := s.store.AutomaticTask(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	config, err := s.store.Device(r.Context(), task.DeviceID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := validateAutomaticTaskDeviceCapabilities(config, task.TaskType, task.Environment); err != nil {
		writeError(w, http.StatusConflict, "wifi_calling_only_device", err.Error())
		return
	}
	if err := validateAutomaticTaskAvailability(s.developerActive(r.Context()), task.TaskType, task.Environment); err != nil {
		writeError(w, http.StatusNotFound, "task_unavailable", err.Error())
		return
	}
	run, err := s.store.QueueAutomaticTaskNow(r.Context(), task)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.automaticTasks.enqueue(run)
	writeJSON(w, http.StatusAccepted, map[string]any{"data": run})
}

func (s *Server) decodeAutomaticTask(r *http.Request, id int64) (store.AutomaticTask, error) {
	var request struct {
		Name            string               `json:"name"`
		Enabled         bool                 `json:"enabled"`
		DeviceID        string               `json:"device_id"`
		ProfileICCID    string               `json:"profile_iccid"`
		ProfileAID      string               `json:"profile_aid"`
		TaskType        string               `json:"task_type"`
		Environment     string               `json:"environment"`
		IntervalDays    int                  `json:"interval_days"`
		IntervalSeconds int                  `json:"interval_seconds"`
		StartDate       string               `json:"start_date"`
		EndDate         string               `json:"end_date"`
		EndTime         string               `json:"end_time"`
		RunTime         string               `json:"run_time"`
		Timezone        string               `json:"timezone"`
		RetryCount      int                  `json:"retry_count"`
		Notify          bool                 `json:"notify"`
		Payload         automaticTaskPayload `json:"payload"`
	}
	if err := s.decodeJSON(nilResponseWriter{}, r, &request); err != nil {
		return store.AutomaticTask{}, err
	}
	request.Name, request.DeviceID = strings.TrimSpace(request.Name), strings.TrimSpace(request.DeviceID)
	request.ProfileICCID, request.ProfileAID = strings.TrimSpace(request.ProfileICCID), strings.TrimSpace(request.ProfileAID)
	request.TaskType, request.Environment = strings.ToLower(strings.TrimSpace(request.TaskType)), strings.ToLower(strings.TrimSpace(request.Environment))
	if request.Name == "" || request.DeviceID == "" || request.ProfileICCID == "" {
		return store.AutomaticTask{}, errors.New("name, device, and eSIM profile are required")
	}
	selectedDevice, err := s.store.Device(r.Context(), request.DeviceID)
	if err != nil {
		return store.AutomaticTask{}, errors.New("selected device does not exist")
	}
	if request.Environment != "vowifi" && request.Environment != "cellular" {
		return store.AutomaticTask{}, errors.New("environment must be vowifi or cellular")
	}
	if request.TaskType != "sms" && request.TaskType != "call" && request.TaskType != "public_ip" && request.TaskType != "profile_rotation" {
		return store.AutomaticTask{}, errors.New("unsupported task type")
	}
	if request.TaskType == "profile_rotation" {
		if request.Environment != "vowifi" {
			return store.AutomaticTask{}, errors.New("profile rotation tasks must use the VoWiFi environment")
		}
		if err := validateAutomaticTaskRotation(request.Payload.Profiles, request.IntervalSeconds); err != nil {
			return store.AutomaticTask{}, err
		}
		for index := range request.Payload.Profiles {
			request.Payload.Profiles[index].ICCID = strings.TrimSpace(request.Payload.Profiles[index].ICCID)
			request.Payload.Profiles[index].AID = strings.TrimSpace(request.Payload.Profiles[index].AID)
			request.Payload.Profiles[index].Name = strings.TrimSpace(request.Payload.Profiles[index].Name)
		}
		// Rotation never keys off the day schedule; keep a valid placeholder.
		if request.IntervalDays < 1 {
			request.IntervalDays = 1
		}
	} else {
		if request.IntervalSeconds != 0 {
			return store.AutomaticTask{}, errors.New("interval_seconds is only valid for profile rotation tasks")
		}
		request.Payload.Profiles = nil
	}
	if request.TaskType == "public_ip" && request.Environment != "cellular" {
		return store.AutomaticTask{}, errors.New("public IP tasks must use cellular direct mode")
	}
	if err := validateAutomaticTaskAvailability(s.developerActive(r.Context()), request.TaskType, request.Environment); err != nil {
		return store.AutomaticTask{}, err
	}
	if err := validateAutomaticTaskDeviceCapabilities(selectedDevice, request.TaskType, request.Environment); err != nil {
		return store.AutomaticTask{}, err
	}
	if request.IntervalDays < 1 || request.IntervalDays > 365 || request.RetryCount < 0 || request.RetryCount > 10 {
		return store.AutomaticTask{}, errors.New("interval_days must be 1-365 and retry_count must be 0-10")
	}
	if request.TaskType == "sms" {
		if !validDialNumber(request.Payload.Phone) || strings.TrimSpace(request.Payload.Message) == "" {
			return store.AutomaticTask{}, errors.New("SMS phone and message are required")
		}
	}
	if request.TaskType == "call" && (!validDialNumber(request.Payload.Phone) || request.Payload.DurationSeconds < 1 || request.Payload.DurationSeconds > 600) {
		return store.AutomaticTask{}, errors.New("call phone is required and automatic hang-up must be 1-600 seconds")
	}
	request.Timezone = strings.TrimSpace(request.Timezone)
	if request.Timezone == "" {
		request.Timezone = time.Local.String()
	}
	location, err := time.LoadLocation(request.Timezone)
	if err != nil {
		return store.AutomaticTask{}, errors.New("timezone must be a valid IANA time zone")
	}
	nextRun, err := nextAutomaticRun(request.StartDate, request.RunTime, request.IntervalDays, time.Now().In(location))
	if err != nil {
		return store.AutomaticTask{}, err
	}
	var endAt time.Time
	request.EndDate, request.EndTime = strings.TrimSpace(request.EndDate), strings.TrimSpace(request.EndTime)
	if request.EndDate != "" || request.EndTime != "" {
		if request.TaskType != "profile_rotation" {
			return store.AutomaticTask{}, errors.New("end_date/end_time are only valid for profile rotation tasks")
		}
		endAt, err = time.ParseInLocation("2006-01-02 15:04", request.EndDate+" "+request.EndTime, location)
		if err != nil {
			return store.AutomaticTask{}, errors.New("end_date and end_time must both be set using YYYY-MM-DD and HH:MM")
		}
		start, _ := time.ParseInLocation("2006-01-02 15:04", strings.TrimSpace(request.StartDate)+" "+strings.TrimSpace(request.RunTime), location)
		if !endAt.After(start) {
			return store.AutomaticTask{}, errors.New("end time must be after the start time")
		}
	}
	payload, _ := json.Marshal(request.Payload)
	task := store.AutomaticTask{ID: id, Name: request.Name, Enabled: request.Enabled, DeviceID: request.DeviceID,
		ProfileICCID: request.ProfileICCID, ProfileAID: request.ProfileAID, TaskType: request.TaskType,
		Environment: request.Environment, IntervalDays: request.IntervalDays, IntervalSeconds: request.IntervalSeconds, EndAt: endAt.UTC(), StartDate: request.StartDate,
		RunTime: request.RunTime, Timezone: request.Timezone, Payload: payload, RetryCount: request.RetryCount, Notify: request.Notify, NextRunAt: nextRun.UTC()}
	if id != 0 {
		if previous, previousErr := s.store.AutomaticTask(r.Context(), id); previousErr == nil {
			task.CreatedAt, task.LastRunAt, task.LastStatus, task.LastError = previous.CreatedAt, previous.LastRunAt, previous.LastStatus, previous.LastError
		}
	}
	return task, nil
}

func validateAutomaticTaskAvailability(available bool, taskType, environment string) error {
	if !available && (taskType == "public_ip" || environment == "cellular") {
		return errors.New("unsupported task type or environment")
	}
	return nil
}

func validateAutomaticTaskDeviceCapabilities(config store.Device, taskType, environment string) error {
	if config.DeviceType != store.DeviceTypeUSBSIMReader {
		return nil
	}
	if environment != "vowifi" {
		return errors.New("USB SIM reader tasks must use the VoWiFi environment")
	}
	if taskType != "sms" && taskType != "call" {
		return errors.New("USB SIM readers support only VoWiFi SMS and call tasks")
	}
	return nil
}

func nextAutomaticRun(date, clock string, intervalDays int, now time.Time) (time.Time, error) {
	location := now.Location()
	start, err := time.ParseInLocation("2006-01-02 15:04", strings.TrimSpace(date)+" "+strings.TrimSpace(clock), location)
	if err != nil {
		return time.Time{}, errors.New("start_date and run_time must use YYYY-MM-DD and HH:MM")
	}
	for start.Before(now) {
		start = start.AddDate(0, 0, intervalDays)
	}
	return start, nil
}

// nilResponseWriter is used only because decodeJSON's size/error contract is
// shared with HTTP handlers; decode errors are returned to the real handler.
type nilResponseWriter struct{}

func (nilResponseWriter) Header() http.Header        { return make(http.Header) }
func (nilResponseWriter) Write([]byte) (int, error)  { return 0, nil }
func (nilResponseWriter) WriteHeader(statusCode int) {}
