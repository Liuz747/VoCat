package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"vocat/internal/device"
	"vocat/internal/store"
	"vocat/internal/vowifi/multisim"
)

// MultiSIMCardAccess lends the reader of a running multi-tunnel group to eUICC
// maintenance. Every operation holds the transaction token that the group's
// AKA exchanges and profile switches take, so card work never interleaves with
// a line's switch-then-authenticate sequence.
type MultiSIMCardAccess interface {
	WithOwnedReader(ctx context.Context, deviceID string, operation func(ctx context.Context, physicalID string) error) error
	// DeleteOwnedProfile first switches the card away from iccid and moves the
	// group's restore target off it when either points at it, then runs
	// deleteProfile, all within one transaction.
	DeleteOwnedProfile(ctx context.Context, deviceID, iccid string, fallback []multisim.Profile, deleteProfile func(ctx context.Context, physicalID string) error) error
}

// ownedReaderError means the group's reader could not be lent, as opposed to a
// failure reported by the card operation itself.
type ownedReaderError struct{ err error }

func (e *ownedReaderError) Error() string { return e.err.Error() }
func (e *ownedReaderError) Unwrap() error { return e.err }

// ownedESIMMaintenance reports whether an eSIM request may run while a group
// owns the reader. Switching and disabling stay blocked: the group decides
// which profile is enabled.
func ownedESIMMaintenance(method string, tail []string) bool {
	if len(tail) == 0 || tail[0] != "esim" {
		return false
	}
	rest := tail[1:]
	switch {
	case len(rest) == 0:
		return method == http.MethodGet
	case rest[0] == "profiles" && len(rest) == 1:
		return method == http.MethodGet
	case rest[0] == "profiles" && len(rest) == 2:
		return method == http.MethodDelete || method == http.MethodPatch
	case rest[0] == "notifications" && len(rest) == 1:
		return method == http.MethodGet
	case rest[0] == "actions" && len(rest) == 2 && rest[1] == "download":
		return method == http.MethodGet
	}
	return false
}

func (s *Server) ownedCardReader(deviceID string) bool {
	return s.multisimCards != nil && s.multisim != nil && s.multisim.Owns(deviceID)
}

// withESIMReader runs a card operation on the running group's reader when one
// owns the device, and directly on the physical modem otherwise.
func (s *Server) withESIMReader(ctx context.Context, configuredID, physicalID string, operation func(context.Context, string) error) error {
	if !s.ownedCardReader(configuredID) {
		return operation(ctx, physicalID)
	}
	var operationErr error
	err := s.multisimCards.WithOwnedReader(ctx, configuredID, func(ctx context.Context, physical string) error {
		operationErr = operation(ctx, physical)
		return operationErr
	})
	if err != nil && operationErr == nil {
		return &ownedReaderError{err: err}
	}
	return err
}

func (s *Server) writeESIMError(w http.ResponseWriter, err error) {
	var readerErr *ownedReaderError
	if errors.As(err, &readerErr) {
		s.logger.Warn("multi-SIM reader unavailable for eSIM maintenance", "error", readerErr.err)
		writeMultiSIMConflict(w)
		return
	}
	s.writeDeviceError(w, err)
}

// deleteOwnedESIMProfile deletes a profile from a card whose group is running.
// The line leaves the group first so its IMS registration is torn down while
// the card still holds the profile; a failed deletion puts the line back.
func (s *Server) deleteOwnedESIMProfile(w http.ResponseWriter, r *http.Request, configuredID, iccid, aidHex string) {
	ctx := r.Context()
	cfg, err := s.store.MultiSIMConfig(ctx, configuredID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	remaining := make([]store.MultiSIMProfile, 0, len(cfg.Profiles))
	inGroup := false
	for _, profile := range cfg.Profiles {
		if profile.ICCID == iccid {
			inGroup = true
			continue
		}
		remaining = append(remaining, profile)
	}
	if len(remaining) == 0 {
		writeError(w, http.StatusConflict, "multisim_last_profile", "这是多隧道里最后一个号码。请先停止多隧道，再删除它。")
		return
	}
	if inGroup {
		next := cfg
		next.Profiles = remaining
		if _, err := s.applyMultiSIMConfig(ctx, next); err != nil {
			writeError(w, http.StatusConflict, "multisim_busy", "多隧道暂时无法移除这条线路，请稍后再试："+err.Error())
			return
		}
	}
	fallback := runtimeMultiSIMConfig(store.MultiSIMConfig{DeviceID: configuredID, Profiles: remaining}).Profiles
	var result *device.EsimDeleteResult
	var deleteErr error
	err = s.multisimCards.DeleteOwnedProfile(ctx, configuredID, iccid, fallback, func(ctx context.Context, physicalID string) error {
		result, deleteErr = s.devices.ESIMDeleteProfile(ctx, physicalID, iccid, aidHex)
		return deleteErr
	})
	if err != nil {
		if inGroup {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			if _, restoreErr := s.applyMultiSIMConfig(restoreCtx, cfg); restoreErr != nil {
				s.logger.Warn("restore multi-SIM line after failed profile deletion", "device_id", configuredID, "error", restoreErr)
			}
			cancel()
		}
		if deleteErr == nil {
			err = &ownedReaderError{err: err}
		}
		s.writeESIMError(w, err)
		return
	}
	data := map[string]any{
		"status":           "deleted",
		"iccid":            iccid,
		"multisim_removed": inGroup,
	}
	if result != nil {
		data["spaceDelta"] = map[string]any{"direction": "reclaimed", "bytes": result.SpaceDelta}
		if result.Warning != "" {
			data["warning"] = result.Warning
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

// joinDownloadedProfile adds a profile downloaded onto a running group's card
// as a new line. It reports whether the line was added and a warning for the
// operator when the profile was written but could not join.
func (s *Server) joinDownloadedProfile(ctx context.Context, deviceID string, result *device.EsimDownloadResult, after device.EsimInfo, afterErr error) (bool, string) {
	const unconfirmed = "Profile 已写入，但没能确认新号码，未自动加入多隧道；请刷新后在多隧道面板里重新保存。"
	iccid := ""
	if result != nil {
		iccid = strings.TrimSpace(result.ICCID)
	}
	if afterErr != nil || iccid == "" {
		return false, unconfirmed
	}
	var found *device.EsimProfile
	for i := range after.Profiles {
		if after.Profiles[i].ICCID == iccid {
			found = &after.Profiles[i]
			break
		}
	}
	if found == nil {
		return false, unconfirmed
	}
	cfg, err := s.store.MultiSIMConfig(ctx, deviceID)
	if err != nil || !cfg.Enabled {
		return false, ""
	}
	for _, existing := range cfg.Profiles {
		if existing.ICCID == iccid {
			return true, ""
		}
	}
	name := firstNonEmpty(firstNonEmpty(found.Nickname, found.Name), found.ServiceProvider)
	cfg.Profiles = append(cfg.Profiles, store.MultiSIMProfile{ICCID: iccid, AID: after.AID, Name: name})
	if _, err := s.applyMultiSIMConfig(ctx, cfg); err != nil {
		return false, "Profile 已写入，但加入多隧道失败：" + err.Error()
	}
	return true, ""
}
