package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeMQTTAcknowledgedTaskCanReplayUntilDedupeExpires(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	payload := json.RawMessage(`{"id":"retained-task","seq":1,"state":"succeeded","result":null,"error":null}`)
	_, _, err = db.RecordTerminalNodeMQTTTask(ctx, NodeMQTTTask{ID: "retained-task", Node: "test-node", Action: "node.describe", RequestHash: "hash", RequestJSON: json.RawMessage(`{}`)}, "succeeded", json.RawMessage(`null`), payload)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.AckNodeMQTTOutbox(ctx, "test-node", "task", "retained-task", 1); err != nil {
		t.Fatal(err)
	}
	if err = db.CleanupNodeMQTT(ctx, "test-node", time.Now().Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "retained-task"); err != nil {
		t.Fatal(err)
	}
	items, err := db.PendingNodeMQTTOutbox(ctx, "test-node", time.Now().Add(time.Second), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || string(items[0].Payload) != string(payload) {
		t.Fatalf("duplicate task lost its exact reply after ACK cleanup: %+v", items)
	}
	if err = db.AckNodeMQTTOutbox(ctx, "test-node", "task", "retained-task", 1); err != nil {
		t.Fatal(err)
	}
	if err = db.CleanupNodeMQTT(ctx, "test-node", time.Now().Add(31*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.NodeMQTTTask(ctx, "retained-task"); err != ErrNotFound {
		t.Fatalf("expired dedupe task retained: %v", err)
	}
	var count int
	if err = db.db.QueryRow(`SELECT COUNT(*) FROM node_mqtt_outbox WHERE business_id='retained-task'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired reply count=%d err=%v", count, err)
	}
}

func TestNodeMQTTUnackedSurvivesRestartCleanupAndWrongAck(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"id":"durable-event","event":"node.hello","data":{}}`)
	if err = db.EnqueueNodeMQTTEvent(ctx, "test-node", "durable-event", payload); err != nil {
		t.Fatal(err)
	}
	if err = db.MarkNodeMQTTOutboxAttempt(ctx, "event", "durable-event", 0, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ack := range []struct {
		node, kind, id string
		seq            int
	}{{"other-node", "event", "durable-event", 0}, {"test-node", "task", "durable-event", 0}, {"test-node", "event", "durable-event", 1}, {"test-node", "event", "unknown", 0}} {
		if err = db.AckNodeMQTTOutbox(ctx, ack.node, ack.kind, ack.id, ack.seq); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.CleanupNodeMQTT(ctx, "test-node", time.Now().Add(365*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = db.WakeNodeMQTTOutbox(ctx, "test-node"); err != nil {
		t.Fatal(err)
	}
	items, err := db.PendingNodeMQTTOutbox(ctx, "test-node", time.Now().Add(time.Second), 100)
	if err != nil || len(items) != 1 || string(items[0].Payload) != string(payload) || items[0].Attempts != 1 {
		t.Fatalf("unacked record not preserved: %+v err=%v", items, err)
	}
	for range 2 {
		if err = db.AckNodeMQTTOutbox(ctx, "test-node", "event", "durable-event", 0); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := db.CountPendingNodeMQTTOutbox(ctx, "test-node"); err != nil || count != 0 {
		t.Fatalf("exact repeated ACK did not clear pending: %d %v", count, err)
	}
}

func TestNodeMQTTTaskAndOutboxAreDurableAndExact(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	accepted := json.RawMessage(`{"id":"cmd-1","seq":1,"time":"2026-09-13 08:00:00","state":"accepted","result":null,"error":null}`)
	task, created, err := database.AcceptNodeMQTTTask(ctx, NodeMQTTTask{ID: "cmd-1", Node: "mdd-1", Action: "sms.send", RequestHash: "abc", RequestJSON: json.RawMessage(`{"id":"cmd-1"}`), ExpiresAt: time.Now().Add(time.Minute)}, accepted)
	if err != nil {
		t.Fatal(err)
	}
	if !created || task.LatestSeq != 1 || task.State != "accepted" {
		t.Fatalf("accepted task = %+v created=%v", task, created)
	}
	if _, created, err = database.AcceptNodeMQTTTask(ctx, NodeMQTTTask{ID: "cmd-1", Node: "mdd-1", Action: "sms.send", RequestHash: "different", RequestJSON: json.RawMessage(`{"id":"cmd-1","different":true}`)}, accepted); err != nil || created {
		t.Fatalf("duplicate created=%v err=%v", created, err)
	}

	running := json.RawMessage(`{"id":"cmd-1","seq":2,"time":"2026-09-13 08:00:01","state":"running","result":null,"error":null}`)
	task, err = database.AdvanceNodeMQTTTask(ctx, "cmd-1", "running", json.RawMessage(`null`), json.RawMessage(`null`), running)
	if err != nil {
		t.Fatal(err)
	}
	if task.LatestSeq != 2 || task.State != "running" {
		t.Fatalf("running task = %+v", task)
	}
	if pending, _ := database.CountPendingNodeMQTTOutbox(ctx, "mdd-1"); pending != 2 {
		t.Fatalf("pending = %d, want 2", pending)
	}
	if err := database.AckNodeMQTTOutbox(ctx, "mdd-1", "task", "cmd-1", 2); err != nil {
		t.Fatal(err)
	}
	if pending, _ := database.CountPendingNodeMQTTOutbox(ctx, "mdd-1"); pending != 1 {
		t.Fatalf("ACK seq=2 implicitly acknowledged another seq: pending=%d", pending)
	}
}

func TestNodeMQTTEventOutboxIsIdempotent(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	payload := json.RawMessage(`{"id":"evt-1","event":"sms.received","time":"2026-09-13 08:00:00","data":{}}`)
	if err := database.EnqueueNodeMQTTEvent(ctx, "mdd-1", "evt-1", payload); err != nil {
		t.Fatal(err)
	}
	if err := database.EnqueueNodeMQTTEvent(ctx, "mdd-1", "evt-1", payload); err != nil {
		t.Fatal(err)
	}
	if pending, _ := database.CountPendingNodeMQTTOutbox(ctx, "mdd-1"); pending != 1 {
		t.Fatalf("pending duplicate events = %d", pending)
	}
}

func TestRejectNodeMQTTTaskStartsAtRejectedSequenceOne(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	task, created, err := database.RejectNodeMQTTTask(ctx, NodeMQTTTask{
		ID: "bad-command", Node: "mdd-1", Action: "_invalid", RequestHash: "hash",
		RequestJSON: json.RawMessage(`{"id":"bad-command"}`),
	}, json.RawMessage(`{"code":"INVALID_ARGUMENT"}`), json.RawMessage(`{"id":"bad-command","seq":1,"state":"rejected"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !created || task.State != "rejected" || task.LatestSeq != 1 || task.FinishedAt.IsZero() {
		t.Fatalf("rejected task = %+v created=%v", task, created)
	}
}

func TestRecordExpiredNodeMQTTTaskStartsAtSequenceOne(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	task, created, err := database.RecordTerminalNodeMQTTTask(ctx, NodeMQTTTask{
		ID: "expired-command", Node: "mdd-1", Action: "sms.send", RequestHash: "hash",
		RequestJSON: json.RawMessage(`{"id":"expired-command"}`),
	}, "expired", json.RawMessage(`{"code":"COMMAND_EXPIRED"}`), json.RawMessage(`{"id":"expired-command","seq":1,"state":"expired"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !created || task.State != "expired" || task.LatestSeq != 1 {
		t.Fatalf("expired task = %+v created=%v", task, created)
	}
}

func TestTerminalDownloadTaskRedactsActivationMaterial(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	request := json.RawMessage(`{"id":"download","params":{"activation_code":"LPA:1$secret$matching"}}`)
	_, _, err = database.AcceptNodeMQTTTask(ctx, NodeMQTTTask{
		ID: "download", Node: "mdd-1", Action: "esim.profile.download", RequestHash: "hash",
		RequestJSON: request, ExpiresAt: time.Now().Add(time.Minute),
	}, json.RawMessage(`{"id":"download","seq":1,"state":"accepted"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.AdvanceNodeMQTTTask(ctx, "download", "succeeded", json.RawMessage(`{}`), json.RawMessage(`null`), json.RawMessage(`{"id":"download","seq":2,"state":"succeeded"}`))
	if err != nil {
		t.Fatal(err)
	}
	task, err := database.NodeMQTTTask(ctx, "download")
	if err != nil {
		t.Fatal(err)
	}
	if string(task.RequestJSON) != `{"redacted":true}` {
		t.Fatalf("terminal download request retained activation material: %s", task.RequestJSON)
	}
}

func TestWakeNodeMQTTOutboxMakesPendingMessagesDue(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now().UTC()
	_, _, err = database.AcceptNodeMQTTTask(ctx, NodeMQTTTask{
		ID: "wake-task", Node: "node-a", Action: "phones.list", RequestHash: "hash",
		RequestJSON: json.RawMessage(`{"id":"wake-task"}`), ExpiresAt: now.Add(time.Minute),
	}, json.RawMessage(`{"id":"wake-task","seq":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.MarkNodeMQTTOutboxAttempt(ctx, "task", "wake-task", 1, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	items, err := database.PendingNodeMQTTOutbox(ctx, "node-a", now.Add(time.Minute), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("outbox unexpectedly due before wake: items=%d err=%v", len(items), err)
	}
	if err := database.WakeNodeMQTTOutbox(ctx, "node-a"); err != nil {
		t.Fatal(err)
	}
	items, err = database.PendingNodeMQTTOutbox(ctx, "node-a", time.Now().Add(time.Second), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("outbox not due after wake: items=%d err=%v", len(items), err)
	}
}
