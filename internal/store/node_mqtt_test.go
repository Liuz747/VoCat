package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

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
