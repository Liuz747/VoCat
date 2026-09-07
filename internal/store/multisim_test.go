package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestMultiSIMConfigPersistsAndDoesNotChangeDevicePolicy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "multi.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mustSaveDevice(t, db, "ec20", "EC20")
	before, _ := db.Device(ctx, "ec20")
	want := MultiSIMConfig{DeviceID: "ec20", Enabled: true, Profiles: []MultiSIMProfile{{ICCID: "1111111111111111111", AID: "A000", Name: "A"}, {ICCID: "2222222222222222222", AID: "A000", Name: "B"}}}
	if _, err := db.SaveMultiSIMConfig(ctx, want); err != nil {
		t.Fatal(err)
	}
	after, _ := db.Device(ctx, "ec20")
	if before.VoWiFiEnabled != after.VoWiFiEnabled {
		t.Fatal("single-line policy changed")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.MultiSIMConfig(ctx, "ec20")
	if err != nil || !got.Enabled || len(got.Profiles) != 2 || got.Profiles[1].Name != "B" {
		t.Fatalf("lost config: %+v %v", got, err)
	}
	all, err := db.ListMultiSIMConfigs(ctx)
	if err != nil || len(all) != 1 {
		t.Fatal("list failed", err)
	}
	if err := db.DeleteDevice(ctx, "ec20"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MultiSIMConfig(ctx, "ec20"); !errors.Is(err, ErrNotFound) {
		t.Fatal("configuration outlived device", err)
	}
}

func TestMultiSIMAndAutomaticTasksAreMutuallyExclusive(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t, filepath.Join(t.TempDir(), "multi.db"))
	mustSaveDevice(t, db, "ec20", "EC20")
	cfg := MultiSIMConfig{DeviceID: "ec20", Enabled: true, Profiles: []MultiSIMProfile{{ICCID: "1111111111111111111"}, {ICCID: "2222222222222222222"}}}
	task, err := db.SaveAutomaticTask(ctx, AutomaticTask{Name: "rotation", Enabled: true, DeviceID: "ec20", ProfileICCID: "1111111111111111111", TaskType: "profile_rotation", Environment: "vowifi", IntervalDays: 1, IntervalSeconds: 60, StartDate: "2026-09-07", RunTime: "12:00", Timezone: "UTC", NextRunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveMultiSIMConfig(ctx, cfg); !errors.Is(err, ErrMultiSIMConflict) {
		t.Fatal("enabled task accepted", err)
	}
	task.Enabled = false
	task, err = db.SaveAutomaticTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	run, err := db.QueueAutomaticTaskNow(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveMultiSIMConfig(ctx, cfg); !errors.Is(err, ErrMultiSIMConflict) {
		t.Fatal("queued task accepted", err)
	}
	run.Status = "failed"
	run.FinishedAt = time.Now()
	if err := db.UpdateAutomaticTaskRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveMultiSIMConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	task.Enabled = true
	if _, err := db.SaveAutomaticTask(ctx, task); err == nil {
		t.Fatal("task enabled during multisim")
	}
	task.Enabled = false
	if _, err := db.QueueAutomaticTaskNow(ctx, task); err == nil {
		t.Fatal("task queued during multisim")
	}
}
