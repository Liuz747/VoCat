package server

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"vocat/internal/device"
	"vocat/internal/store"
	"vocat/internal/vowifi/multisim"
)

var errMultiSIMActive = errors.New("请先停止此设备的多隧道，再执行设备操作")

type MultiSIMController interface {
	Apply(context.Context, multisim.Config) error
	Owns(string) bool
	State(string) multisim.GroupState
	Reconnect(string, string) error
}

func runtimeMultiSIMConfig(v store.MultiSIMConfig) multisim.Config {
	out := multisim.Config{DeviceID: v.DeviceID, Enabled: v.Enabled, Profiles: make([]multisim.Profile, 0, len(v.Profiles))}
	for _, p := range v.Profiles {
		out.Profiles = append(out.Profiles, multisim.Profile{ICCID: p.ICCID, AID: p.AID, Name: p.Name})
	}
	return out
}

// Serialize API mutations for one physical device without blocking other
// modems. The runtime's own transaction lock continues to own SIM exchanges.
func (s *Server) lockMultiSIMDevice(ctx context.Context, id string) (func(), error) {
	value, _ := s.multisimDeviceLocks.LoadOrStore(id, &sync.Mutex{})
	return lockServerMutex(ctx, value.(*sync.Mutex))
}

func lockServerMutex(ctx context.Context, mu *sync.Mutex) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if mu.TryLock() {
			return mu.Unlock, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (s *Server) multiSIMOwned(ctx context.Context, id string) bool {
	if s.multisim != nil && s.multisim.Owns(id) {
		return true
	}
	if s.store == nil {
		return false
	}
	cfg, err := s.store.MultiSIMConfig(ctx, id)
	return (err == nil && cfg.Enabled) || (err != nil && !errors.Is(err, store.ErrNotFound))
}
func (s *Server) multiSIMCardOwned(ctx context.Context, iccid string) bool {
	if s.store == nil {
		return false
	}
	configs, err := s.store.ListMultiSIMConfigs(ctx)
	if err != nil {
		return true
	}
	for _, cfg := range configs {
		if !cfg.Enabled && (s.multisim == nil || !s.multisim.Owns(cfg.DeviceID)) {
			continue
		}
		for _, p := range cfg.Profiles {
			if p.ICCID == iccid {
				return true
			}
		}
	}
	return false
}
func writeMultiSIMConflict(w http.ResponseWriter) {
	writeError(w, http.StatusConflict, "multisim_active", "此设备正在运行或停止多隧道。请先停止多隧道，再执行读卡或设备操作。")
}

func (s *Server) multiSIMResponse(ctx context.Context, device store.Device) map[string]any {
	cfg, err := s.store.MultiSIMConfig(ctx, device.ID)
	if err != nil {
		cfg = store.MultiSIMConfig{DeviceID: device.ID, Profiles: []store.MultiSIMProfile{}}
	}
	state := multisim.GroupState{DeviceID: device.ID, Phase: "disabled", Lines: []multisim.LineState{}}
	owned := false
	if s.multisim != nil {
		state = s.multisim.State(device.ID)
		owned = s.multisim.Owns(device.ID)
	}
	if state.DeviceID == "" {
		state.DeviceID = device.ID
	}
	if state.Lines == nil {
		state.Lines = []multisim.LineState{}
	}
	if state.Phase == "" {
		if cfg.Enabled {
			state.Phase = "pending"
		} else {
			state.Phase = "disabled"
		}
	}
	active := ""
	if entry, _, present := s.physicalForConfig(device); present && entry.Snapshot != nil {
		active = entry.Snapshot.ICCID
	}
	return map[string]any{"config": cfg, "state": state, "owned": owned, "active_profile_iccid": active, "available": s.multisim != nil}
}

// multiSIMSummary is the compact projection attached to device summaries so the
// device list, dashboard and overview can show a running group without one
// extra request per device. It returns nil when the device has no enabled or
// owned group, so legacy single-line devices are unaffected.
func (s *Server) multiSIMSummary(ctx context.Context, deviceID string) map[string]any {
	if s.store == nil {
		return nil
	}
	cfg, err := s.store.MultiSIMConfig(ctx, deviceID)
	if err != nil {
		return nil
	}
	owned := s.multisim != nil && s.multisim.Owns(deviceID)
	if !cfg.Enabled && !owned {
		return nil
	}
	state := multisim.GroupState{Phase: "pending", Lines: []multisim.LineState{}}
	if s.multisim != nil {
		if live := s.multisim.State(deviceID); live.Phase != "" {
			state = live
		}
	}
	ready := 0
	lines := make([]map[string]any, 0, len(state.Lines))
	for _, line := range state.Lines {
		if line.State.SMSReady {
			ready++
		}
		suffix := line.ICCID
		if len(suffix) > 6 {
			suffix = suffix[len(suffix)-6:]
		}
		lines = append(lines, map[string]any{
			"iccid_suffix":     suffix,
			"name":             line.Name,
			"session_id":       line.SessionID,
			"phone_number":     line.State.PhoneNumber,
			"phase":            string(line.State.Phase),
			"sms_ready":        line.State.SMSReady,
			"tunnel_ready":     line.State.TunnelReady,
			"ims_ready":        line.State.IMSReady,
			"proxy_id":         line.State.ProxyID,
			"attempt":          line.State.Attempt,
			"last_reason":      line.State.LastReason,
			"last_error_class": line.State.LastErrorClass,
			"updated_at":       line.State.UpdatedAt,
		})
	}
	return map[string]any{
		"enabled":     cfg.Enabled,
		"owned":       owned,
		"phase":       state.Phase,
		"busy":        state.Busy,
		"last_error":  state.LastError,
		"lines_total": len(cfg.Profiles),
		"lines_ready": ready,
		"lines":       lines,
		"updated_at":  state.UpdatedAt,
	}
}

func (s *Server) applyMultiSIMConfig(ctx context.Context, cfg store.MultiSIMConfig) (store.MultiSIMConfig, error) {
	previous, readErr := s.store.MultiSIMConfig(ctx, cfg.DeviceID)
	if readErr != nil && !errors.Is(readErr, store.ErrNotFound) {
		return cfg, readErr
	}
	saved, err := s.store.SaveMultiSIMConfig(ctx, cfg)
	if err != nil {
		return saved, err
	}
	if err = s.multisim.Apply(ctx, runtimeMultiSIMConfig(saved)); err == nil {
		return saved, nil
	}
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rollbackErr error
	if errors.Is(readErr, store.ErrNotFound) {
		rollbackErr = s.store.DeleteMultiSIMConfig(rollbackCtx, cfg.DeviceID)
	} else {
		_, rollbackErr = s.store.SaveMultiSIMConfig(rollbackCtx, previous)
	}
	if rollbackErr != nil {
		return saved, errors.New("运行配置未被接受，且保存配置回滚失败，请检查设备状态")
	}
	return previous, err
}

func (s *Server) handleMultiSIM(w http.ResponseWriter, r *http.Request, device store.Device, tail []string) bool {
	if len(tail) == 0 && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"data": s.multiSIMResponse(r.Context(), device)})
		return true
	}
	if s.multisim == nil {
		writeError(w, http.StatusServiceUnavailable, "multisim_unavailable", "多隧道运行服务不可用")
		return true
	}
	unlock, err := s.lockMultiSIMDevice(r.Context(), device.ID)
	if err != nil {
		writeMultiSIMConflict(w)
		return true
	}
	defer unlock()
	if len(tail) == 3 && tail[0] == "lines" && (tail[2] == "reconnect" || tail[2] == "refresh") {
		if !requireMethod(w, r, http.MethodPost) {
			return true
		}
		cfg, err := s.store.MultiSIMConfig(r.Context(), device.ID)
		if err != nil {
			s.writeStoreError(w, err)
			return true
		}
		found := false
		for _, p := range cfg.Profiles {
			if p.ICCID == tail[1] {
				found = true
				break
			}
		}
		if !cfg.Enabled || !found {
			writeError(w, http.StatusConflict, "multisim_line_unavailable", "该号码没有启用多隧道")
			return true
		}
		var operationErr error
		if tail[2] == "refresh" {
			refresher, ok := s.multisim.(interface{ Refresh(string, string) error })
			if !ok {
				writeError(w, http.StatusServiceUnavailable, "multisim_refresh_unavailable", "线路续约暂不可用")
				return true
			}
			operationErr = refresher.Refresh(device.ID, tail[1])
		} else {
			operationErr = s.multisim.Reconnect(device.ID, tail[1])
		}
		if operationErr != nil {
			writeError(w, http.StatusConflict, "multisim_"+tail[2]+"_failed", operationErr.Error())
			return true
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"data": s.multiSIMResponse(r.Context(), device)})
		return true
	}
	if len(tail) != 0 {
		writeError(w, http.StatusNotFound, "not_found", "multi-SIM endpoint not found")
		return true
	}
	if !requireMethod(w, r, http.MethodPut) {
		return true
	}
	var request multisim.Config
	if err := s.decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_multisim", err.Error())
		return true
	}
	if request.DeviceID != "" && request.DeviceID != device.ID {
		writeError(w, http.StatusBadRequest, "invalid_multisim", "设备身份不匹配")
		return true
	}
	request.DeviceID = device.ID
	// A stop request must retain the card ownership list until cleanup ends.
	// Omitting profiles also preserves the saved selection after a full stop.
	if !request.Enabled && (request.Profiles == nil || s.multiSIMOwned(r.Context(), device.ID)) {
		previous, readErr := s.store.MultiSIMConfig(r.Context(), device.ID)
		if readErr != nil && !errors.Is(readErr, store.ErrNotFound) {
			s.writeStoreError(w, readErr)
			return true
		}
		if readErr == nil {
			request.Profiles = runtimeMultiSIMConfig(previous).Profiles
		}
	}
	if request.Enabled && device.DeviceType != store.DeviceTypePCIeEC20EC25 {
		writeError(w, http.StatusConflict, "multisim_unsupported", "首版多隧道仅支持 EC20/EC25 AT 设备")
		return true
	}
	if len(request.Profiles) > 8 || (request.Enabled && len(request.Profiles) < 2) {
		writeError(w, http.StatusBadRequest, "invalid_multisim", "请选择 2–8 个 eSIM Profile")
		return true
	}
	seen := map[string]bool{}
	for i := range request.Profiles {
		p := &request.Profiles[i]
		p.ICCID = strings.TrimSpace(p.ICCID)
		p.AID = strings.ToUpper(strings.TrimSpace(p.AID))
		p.Name = strings.TrimSpace(p.Name)
		if !validICCID(p.ICCID) || seen[p.ICCID] {
			writeError(w, http.StatusBadRequest, "invalid_multisim", "Profile ICCID 无效或重复")
			return true
		}
		seen[p.ICCID] = true
		if _, err := hex.DecodeString(p.AID); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_multisim", "Profile AID 必须是十六进制")
			return true
		}
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_multisim", err.Error())
		return true
	}
	cfg := store.MultiSIMConfig{DeviceID: device.ID, Enabled: request.Enabled, Profiles: make([]store.MultiSIMProfile, 0, len(request.Profiles))}
	for _, p := range request.Profiles {
		cfg.Profiles = append(cfg.Profiles, store.MultiSIMProfile{ICCID: p.ICCID, AID: p.AID, Name: p.Name})
	}
	unlockBindings, gateErr := s.lockMultiSIMBindings(r.Context())
	if gateErr != nil {
		writeMultiSIMConflict(w)
		return true
	}
	defer unlockBindings()
	if _, err := s.applyMultiSIMConfig(r.Context(), cfg); err != nil {
		if errors.Is(err, store.ErrMultiSIMConflict) {
			writeError(w, http.StatusConflict, "automatic_tasks_active", "请先停用此设备的自动任务，并等待已排队或执行中的任务结束。")
			return true
		}
		writeError(w, http.StatusConflict, "multisim_apply_failed", err.Error())
		return true
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"data": s.multiSIMResponse(r.Context(), device)})
	return true
}

func (s *Server) stopMultiSIMBeforeDelete(ctx context.Context, id string) error {
	if !s.multiSIMOwned(ctx, id) {
		return nil
	}
	if s.multisim == nil {
		return errors.New("multi-SIM runtime unavailable")
	}
	cfg, err := s.store.MultiSIMConfig(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		cfg = store.MultiSIMConfig{DeviceID: id}
	} else if err != nil {
		return err
	}
	cfg.Enabled = false
	if _, err = s.applyMultiSIMConfig(ctx, cfg); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for s.multisim.Owns(id) {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return waitCtx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func (s *Server) lockMultiSIMUnowned(ctx context.Context, id string) (func(), error) {
	unlock, err := s.lockMultiSIMDevice(ctx, id)
	if err != nil {
		return nil, err
	}
	if s.multiSIMOwned(ctx, id) {
		unlock()
		return nil, errMultiSIMActive
	}
	return unlock, nil
}

// This short gate orders binding writes against ownership acquisition. Never
// retain it across modem RF/AT commands. Unlock is idempotent for early release.
func (s *Server) lockMultiSIMBindings(ctx context.Context) (func(), error) {
	unlock, err := lockServerMutex(ctx, &s.multisimBindingsMu)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(unlock) }, nil
}

// Called while holding the bindings gate. Check every physical candidate,
// rather than the display mapper's single best match: stale/wrong IMEI metadata
// must not clear ownership of a selected USB path or AT/control node.
func (s *Server) multiSIMBindingConflict(ctx context.Context, candidate store.Device) bool {
	if s.devices == nil {
		return false
	}
	configs, err := s.store.ListDevices(ctx)
	if err != nil {
		return true
	}
	entries := s.devices.List()
	for _, config := range configs {
		if config.ID == candidate.ID || !s.multiSIMOwned(ctx, config.ID) {
			continue
		}
		for _, entry := range entries {
			if entry.Discovered && multiSIMPhysicalCandidateIntersects(entry, config) && multiSIMPhysicalCandidateIntersects(entry, candidate) {
				return true
			}
		}
	}
	return false
}

// Deliberately conservative for exclusion: any selected physical path or node
// counts, even when another identity field disagrees. This cannot be used to
// choose a modem for normal operations.
func multiSIMPhysicalCandidateIntersects(entry device.Device, config store.Device) bool {
	if entry.ID == config.ID {
		return true
	}
	if config.ModemIMEI != "" && entry.Snapshot != nil && strings.EqualFold(strings.TrimSpace(config.ModemIMEI), strings.TrimSpace(entry.Snapshot.IMEI)) {
		return true
	}
	if config.USBPath != "" && config.USBPath == entry.Candidate.USBPath {
		return true
	}
	for _, selected := range []string{config.ATPort, config.ControlDevice} {
		if selected == "" {
			continue
		}
		for _, node := range []string{entry.Candidate.ATPort.Path, entry.Candidate.ATPort.OpenPath(), entry.Candidate.QMIControl, entry.Candidate.ReaderName} {
			if node != "" && selected == node {
				return true
			}
		}
	}
	return false
}
