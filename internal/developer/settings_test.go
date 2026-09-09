package developer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"vocat/internal/exportproxy"
	"vocat/internal/httpsmode"
	"vocat/internal/store"
)

func TestResetExperimentalRestoresDefaults(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "vocat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := SetDeviceLimit(ctx, database, 8); err != nil {
		t.Fatal(err)
	}
	if err := SetSMSHourlyLimit(ctx, database, 18); err != nil {
		t.Fatal(err)
	}
	enabled, _ := json.Marshal(map[string]bool{"enabled": true})
	if err := database.UpsertAppSetting(ctx, store.AppSetting{Key: httpsmode.SettingKey, Value: enabled}); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertDevice(ctx, store.Device{ID: "modem-1", Name: "modem-1", NetworkEnabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertCardPolicy(ctx, store.CardPolicy{ICCID: "8901000000000000001", NetworkEnabled: true, IPVersion: "IPV4V6"}); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertAppSetting(ctx, store.AppSetting{Key: exportproxy.SettingKey, Value: json.RawMessage(`[]`)}); err != nil {
		t.Fatal(err)
	}
	if err := ResetExperimental(ctx, database); err != nil {
		t.Fatal(err)
	}
	if limit := DeviceLimit(ctx, database, true); limit != DefaultDeviceLimit {
		t.Fatalf("device limit = %d, want %d", limit, DefaultDeviceLimit)
	}
	if limit := SMSHourlyLimit(ctx, database); limit != DefaultSMSHourlyLimit {
		t.Fatalf("SMS hourly limit = %d, want %d", limit, DefaultSMSHourlyLimit)
	}
	setting, err := database.AppSetting(ctx, httpsmode.SettingKey)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.Unmarshal(setting.Value, &document); err != nil || document.Enabled {
		t.Fatalf("HTTPS setting = %s, error = %v", setting.Value, err)
	}
	device, err := database.Device(ctx, "modem-1")
	if err != nil || device.NetworkEnabled {
		t.Fatalf("device roaming data was not disabled: %+v, %v", device, err)
	}
	policy, err := database.CardPolicy(ctx, "8901000000000000001")
	if err != nil || policy.NetworkEnabled {
		t.Fatalf("card roaming policy was not disabled: %+v, %v", policy, err)
	}
	if _, err := database.AppSetting(ctx, exportproxy.SettingKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("export proxy configurations were not deleted: %v", err)
	}
}

func TestSetDeviceLimitValidatesRange(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "vocat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if SetDeviceLimit(ctx, database, -1) == nil {
		t.Fatal("negative device limit was accepted")
	}
	// There is no upper bound on modules: the user runs fleets of EC20s
	// behind hubs, and any ceiling here would be an artificial wall.
	if err := SetDeviceLimit(ctx, database, 500); err != nil {
		t.Fatalf("large device limit was rejected: %v", err)
	}
	if got := DeviceLimit(ctx, database, true); got != 500 {
		t.Fatalf("device limit = %d, want 500", got)
	}
	// 0 means unlimited and is a valid explicit setting.
	if err := SetDeviceLimit(ctx, database, 0); err != nil {
		t.Fatalf("unlimited (0) device limit was rejected: %v", err)
	}
	if got := DeviceLimit(ctx, database, true); got != 0 {
		t.Fatalf("device limit = %d, want 0 (unlimited)", got)
	}
}

func TestDeviceLimitIsUnlimitedByDefaultRegardlessOfDeveloperMode(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "vocat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, developerEnabled := range []bool{false, true} {
		if got := DeviceLimit(ctx, database, developerEnabled); got != 0 {
			t.Fatalf("developer=%v: default device limit = %d, want 0 (unlimited)", developerEnabled, got)
		}
	}
	if DefaultDeviceLimit != 0 {
		t.Fatalf("DefaultDeviceLimit = %d, want 0 (unlimited)", DefaultDeviceLimit)
	}
}

func TestSetSMSHourlyLimitValidatesRange(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "vocat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if SetSMSHourlyLimit(ctx, database, 0) == nil || SetSMSHourlyLimit(ctx, database, MaxSMSHourlyLimit+1) == nil {
		t.Fatal("out-of-range SMS hourly limit was accepted")
	}
	if err := SetSMSHourlyLimit(ctx, database, 15); err != nil {
		t.Fatal(err)
	}
	if got := SMSHourlyLimit(ctx, database); got != 15 {
		t.Fatalf("SMS hourly limit = %d, want 15", got)
	}
}

func TestStoredLimitsAboveHardMaximumAreClamped(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "vocat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for key, limit := range map[string]int{
		DeviceLimitSettingKey: 99,
		SMSHourlyLimitKey:     99,
	} {
		value, _ := json.Marshal(map[string]int{"limit": limit})
		if err := database.UpsertAppSetting(ctx, store.AppSetting{Key: key, Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	if got := DeviceLimit(ctx, database, true); got != 99 {
		t.Fatalf("device limit = %d, want 99 (module count is not clamped)", got)
	}
	if got := SMSHourlyLimit(ctx, database); got != MaxSMSHourlyLimit {
		t.Fatalf("SMS hourly limit = %d, want %d", got, MaxSMSHourlyLimit)
	}
}
