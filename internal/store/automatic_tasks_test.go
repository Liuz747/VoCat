package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAutomaticTasksAreClaimedInDeviceQueueOrderAndAdvanceSchedule(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-tasks.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	now := time.Now().UTC().Truncate(time.Second)
	for index := 0; index < 2; index++ {
		payload, _ := json.Marshal(map[string]any{"phone": "10086", "message": "test"})
		if _, err := database.SaveAutomaticTask(ctx, AutomaticTask{
			Name: "task", Enabled: true, DeviceID: "ec20", ProfileICCID: "8944100000000000000",
			TaskType: "sms", Environment: "vowifi", IntervalDays: 2,
			StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai", Payload: payload,
			NextRunAt: now.Add(time.Duration(index-2) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := database.ClaimDueAutomaticTasks(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].DeviceID != "ec20" || runs[1].DeviceID != "ec20" || runs[0].TaskID >= runs[1].TaskID {
		t.Fatalf("claimed runs = %+v", runs)
	}
	tasks, err := database.ListAutomaticTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if !task.NextRunAt.After(now) {
			t.Fatalf("task %d next run was not advanced: %v", task.ID, task.NextRunAt)
		}
	}
	second, err := database.ClaimDueAutomaticTasks(ctx, now, 10)
	if err != nil || len(second) != 0 {
		t.Fatalf("same schedule claimed twice: %+v, %v", second, err)
	}
}

func TestDeletingAutomaticTaskRemovesRunHistory(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-delete.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "task", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "call", Environment: "cellular", IntervalDays: 1,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai", Payload: []byte(`{"phone":"10086","duration_seconds":10}`),
		NextRunAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.QueueAutomaticTaskNow(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := database.DeleteAutomaticTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	runs, err := database.ListAutomaticTaskRuns(ctx, 10)
	if err != nil || len(runs) != 0 {
		t.Fatalf("orphan runs = %+v, %v", runs, err)
	}
}

func TestListAutomaticTaskRunsPaginated(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-runs-page.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "task", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "call", Environment: "cellular", IntervalDays: 1,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai", Payload: []byte(`{"phone":"10086","duration_seconds":10}`),
		NextRunAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 5; index++ {
		if _, err := database.QueueAutomaticTaskNow(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	first, total, err := database.ListAutomaticTaskRunsPaginated(ctx, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(first) != 2 {
		t.Fatalf("first page: total = %d, runs = %+v", total, first)
	}
	if first[0].ID <= first[1].ID {
		t.Fatalf("runs not newest-first: %+v", first)
	}

	last, total, err := database.ListAutomaticTaskRunsPaginated(ctx, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(last) != 1 {
		t.Fatalf("last page: total = %d, runs = %+v", total, last)
	}

	// Out-of-range paging inputs are clamped to defaults, not errors.
	all, total, err := database.ListAutomaticTaskRunsPaginated(ctx, 0, -5)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(all) != 5 {
		t.Fatalf("clamped page: total = %d, runs = %+v", total, all)
	}
}

func TestAvailableAutomaticTasksExcludeRestrictedTaskAndRunHistory(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-availability.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	now := time.Now().UTC().Truncate(time.Second)
	save := func(name, taskType, environment string) AutomaticTask {
		t.Helper()
		task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
			Name: name, Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
			TaskType: taskType, Environment: environment, IntervalDays: 1,
			StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai",
			Payload: []byte(`{"phone":"10086","message":"test"}`), NextRunAt: now.Add(-time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		return task
	}
	visible := save("visible", "sms", "vowifi")
	hidden := save("hidden", "public_ip", "cellular")
	for _, task := range []AutomaticTask{visible, hidden} {
		if _, err := database.QueueAutomaticTaskNow(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	runs, total, err := database.ListAvailableAutomaticTaskRunsPaginated(ctx, 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(runs) != 1 || runs[0].TaskID != visible.ID {
		t.Fatalf("available history total=%d runs=%+v", total, runs)
	}
	claimed, err := database.ClaimDueAvailableAutomaticTasks(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].TaskID != visible.ID {
		t.Fatalf("available claims = %+v", claimed)
	}
	storedHidden, err := database.AutomaticTask(ctx, hidden.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedHidden.NextRunAt.After(now) {
		t.Fatalf("restricted task schedule advanced unexpectedly: %v", storedHidden.NextRunAt)
	}
}

func TestRecoverAutomaticTaskRunsFailsRunningAndReturnsQueued(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-recovery.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "task", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "call", Environment: "cellular", IntervalDays: 1,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai", Payload: []byte(`{"phone":"10086","duration_seconds":10}`),
		NextRunAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := database.QueueAutomaticTaskNow(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	running.Status = "running"
	running.StartedAt = time.Now().UTC().Add(-time.Minute)
	running.Attempts = 1
	if err := database.UpdateAutomaticTaskRun(ctx, running); err != nil {
		t.Fatal(err)
	}
	queued, err := database.QueueAutomaticTaskNow(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	recoveredAt := time.Now().UTC().Truncate(time.Second)
	recovered, err := database.RecoverAutomaticTaskRuns(ctx, recoveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].ID != queued.ID || recovered[0].Status != "queued" {
		t.Fatalf("recovered queued runs = %+v", recovered)
	}
	runs, err := database.ListAutomaticTaskRuns(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	foundRunning := false
	for _, run := range runs {
		if run.ID == running.ID {
			foundRunning = true
			if run.Status != "failed" || run.FinishedAt.IsZero() || !strings.Contains(run.Error, "service restarted") {
				t.Fatalf("recovered running run = %+v", run)
			}
		}
	}
	if !foundRunning {
		t.Fatal("running run was not found after recovery")
	}
	recoveredTask, err := database.AutomaticTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredTask.LastStatus != "failed" || !strings.Contains(recoveredTask.LastError, "service restarted") {
		t.Fatalf("recovered task status = %+v", recoveredTask)
	}
}

func TestMigration24KeepsAutomaticTaskHistoryAndAcceptsProfileRotation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "automatic-task-rotation-migration.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= 23; version++ {
		for _, statement := range migrationStatements(version) {
			if _, err := raw.ExecContext(ctx, statement); err != nil {
				t.Fatalf("create v%d schema: %v", version, err)
			}
		}
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO devices (id, name, created_at, updated_at) VALUES ('ec20', 'EC20', 100, 100);
		INSERT INTO automatic_tasks (
			id, name, enabled, device_id, profile_iccid, task_type, environment, interval_days,
			start_date, run_time, payload_json, next_run_at, created_at, updated_at
		) VALUES (7, 'daily', 1, 'ec20', '8944100000000000001', 'sms', 'vowifi', 2,
			'2026-08-10', '12:00', '{"phone":"10086","message":"hi"}', 200, 100, 100);
		INSERT INTO automatic_task_runs (
			task_id, device_id, scheduled_at, status, attempts, output, created_at, updated_at
		) VALUES (7, 'ec20', 200, 'success', 1, 'done', 200, 200);
		PRAGMA user_version = 23;
	`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database := openTestStore(t, path)
	legacy, err := database.AutomaticTask(ctx, 7)
	if err != nil || legacy.Name != "daily" || legacy.IntervalDays != 2 || legacy.IntervalSeconds != 0 {
		t.Fatalf("legacy task after migration = %+v, %v", legacy, err)
	}
	runs, total, err := database.ListAutomaticTaskRunsPaginated(ctx, 10, 0)
	if err != nil || total != 1 || len(runs) != 1 || runs[0].TaskID != 7 || runs[0].Output != "done" {
		t.Fatalf("run history after migration = %+v (total %d), %v", runs, total, err)
	}
	rotation, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "rotate", Enabled: true, DeviceID: "ec20", ProfileICCID: "8944100000000000001",
		TaskType: "profile_rotation", Environment: "vowifi", IntervalDays: 1, IntervalSeconds: 900,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai",
		Payload:   []byte(`{"profiles":[{"iccid":"8944100000000000001"},{"iccid":"8944100000000000002"}]}`),
		NextRunAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("save profile_rotation task: %v", err)
	}
	if rotation.TaskType != "profile_rotation" || rotation.IntervalSeconds != 900 {
		t.Fatalf("saved rotation task = %+v", rotation)
	}
	if err := database.DeleteAutomaticTask(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if _, total, err := database.ListAutomaticTaskRunsPaginated(ctx, 10, 0); err != nil || total != 0 {
		t.Fatalf("run history did not cascade after migration: total=%d, %v", total, err)
	}
}

func TestClaimAdvancesSecondScheduledTasksByIntervalSeconds(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-minutes.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	now := time.Now().UTC().Truncate(time.Second)
	due := now.Add(-40 * time.Minute)
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "rotate", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "profile_rotation", Environment: "vowifi", IntervalDays: 1, IntervalSeconds: 900,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai",
		Payload: []byte(`{"profiles":[{"iccid":"one"},{"iccid":"two"}]}`), NextRunAt: due,
	})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := database.ClaimDueAutomaticTasks(ctx, now, 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("claimed = %+v, %v", runs, err)
	}
	stored, err := database.AutomaticTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	// due + 15m, +30m are still in the past; +45m is the first future slot.
	if want := due.Add(45 * time.Minute); !stored.NextRunAt.Equal(want) {
		t.Fatalf("next run = %v, want %v", stored.NextRunAt, want)
	}
}

func TestClaimSkipsTasksWithInFlightRuns(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-inflight.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	now := time.Now().UTC().Truncate(time.Second)
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "rotate", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "profile_rotation", Environment: "vowifi", IntervalDays: 1, IntervalSeconds: 30,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai",
		Payload: []byte(`{"profiles":[{"iccid":"one"},{"iccid":"two"}]}`), NextRunAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := database.ClaimDueAutomaticTasks(ctx, now, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	// The first run is still queued; the slot is due again but must not pile up.
	again, err := database.ClaimDueAutomaticTasks(ctx, now.Add(5*time.Minute), 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("claim with queued run = %+v, %v", again, err)
	}
	first[0].Status, first[0].FinishedAt = "success", now
	if err := database.UpdateAutomaticTaskRun(ctx, first[0]); err != nil {
		t.Fatal(err)
	}
	if err := database.RescheduleAutomaticTask(ctx, task.ID, now.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	stored, err := database.AutomaticTask(ctx, task.ID)
	if err != nil || !stored.NextRunAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("rescheduled next run = %v, %v", stored.NextRunAt, err)
	}
	after, err := database.ClaimDueAutomaticTasks(ctx, now.Add(time.Minute), 10)
	if err != nil || len(after) != 1 {
		t.Fatalf("claim after completion = %+v, %v", after, err)
	}
}

func TestRotationTaskDisablesItselfAfterEndAt(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, filepath.Join(t.TempDir(), "automatic-task-end.db"))
	mustSaveDevice(t, database, "ec20", "EC20")
	now := time.Now().UTC().Truncate(time.Second)
	task, err := database.SaveAutomaticTask(ctx, AutomaticTask{
		Name: "rotate", Enabled: true, DeviceID: "ec20", ProfileICCID: "one",
		TaskType: "profile_rotation", Environment: "vowifi", IntervalDays: 1, IntervalSeconds: 30,
		StartDate: "2026-08-10", RunTime: "12:00", Timezone: "Asia/Shanghai",
		Payload:   []byte(`{"profiles":[{"iccid":"one"},{"iccid":"two"}]}`),
		NextRunAt: now.Add(-time.Minute), EndAt: now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !task.EndAt.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("persisted end_at = %v", task.EndAt)
	}
	before, err := database.ClaimDueAutomaticTasks(ctx, now, 10)
	if err != nil || len(before) != 1 {
		t.Fatalf("claim before end = %+v, %v", before, err)
	}
	before[0].Status, before[0].FinishedAt = "success", now
	if err := database.UpdateAutomaticTaskRun(ctx, before[0]); err != nil {
		t.Fatal(err)
	}
	if err := database.RescheduleAutomaticTask(ctx, task.ID, now.Add(11*time.Minute)); err != nil {
		t.Fatal(err)
	}
	after, err := database.ClaimDueAutomaticTasks(ctx, now.Add(12*time.Minute), 10)
	if err != nil || len(after) != 0 {
		t.Fatalf("claim after end created runs: %+v, %v", after, err)
	}
	stored, err := database.AutomaticTask(ctx, task.ID)
	if err != nil || stored.Enabled {
		t.Fatalf("task past end_at still enabled: %+v, %v", stored, err)
	}
}
