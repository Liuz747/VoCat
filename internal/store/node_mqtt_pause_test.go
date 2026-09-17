package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeMQTTReplayPreservesPriorBusinessAcknowledgement(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payload := json.RawMessage(`{"id":"done","seq":1,"state":"expired"}`)
	_, _, err = db.RecordTerminalNodeMQTTTask(ctx, NodeMQTTTask{ID: "done", Node: "node", Action: "phones.list", RequestHash: "hash", RequestJSON: json.RawMessage(`{}`)}, "expired", json.RawMessage(`null`), payload)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.AckNodeMQTTOutbox(ctx, "node", "task", "done", 1); err != nil {
		t.Fatal(err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	if count, err := db.CountPendingNodeMQTTOutbox(ctx, "node"); err != nil || count != 0 {
		t.Fatalf("replay reopened acknowledged business debt: count=%d err=%v", count, err)
	}
	items, err := db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Second), 100)
	if err != nil || len(items) != 1 || string(items[0].Payload) != string(payload) || items[0].AckedAt.IsZero() {
		t.Fatalf("exact acknowledged reply not scheduled: %+v %v", items, err)
	}
	if err = db.CompleteNodeMQTTReplay(ctx, "node", "task", "done", 1, items[0].ReplayGeneration); err != nil {
		t.Fatal(err)
	}
	if items, err = db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Hour), 100); err != nil || len(items) != 0 {
		t.Fatalf("confirmed replay kept retrying: %+v %v", items, err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	selected, err := db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Second), 100)
	if err != nil || len(selected) != 1 {
		t.Fatalf("select first replay: %v %v", selected, err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	if err = db.CompleteNodeMQTTReplay(ctx, "node", "task", "done", 1, selected[0].ReplayGeneration); err != nil {
		t.Fatal(err)
	}
	if items, err = db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Hour), 100); err != nil || len(items) != 1 {
		t.Fatalf("older publish erased a newer replay request: %+v %v", items, err)
	}
}

func TestNodeMQTTOutboxAdministrationCanReachRowsBehindPausedPage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 201; i++ {
		if err = db.EnqueueNodeMQTTEvent(ctx, "node", fmt.Sprintf("event-%03d", i), json.RawMessage(`{"event":"node.hello"}`)); err != nil {
			t.Fatal(err)
		}
	}
	page, next, err := db.ListNodeMQTTOutbox(ctx, "node", 0)
	if err != nil || len(page) != 200 || next == 0 {
		t.Fatalf("first page=%d cursor=%d err=%v", len(page), next, err)
	}
	for start := 0; start < 200; start += 100 {
		keys := []NodeMQTTOutboxKey{}
		for _, item := range page[start : start+100] {
			keys = append(keys, item.NodeMQTTOutboxKey)
		}
		if err = db.SetNodeMQTTOutboxPaused(ctx, "node", keys, true, "operator requested pause"); err != nil {
			t.Fatal(err)
		}
	}
	if allowed, err := db.NodeMQTTOutboxDeliverable(ctx, "node", "event", page[0].ID, 0); err != nil || allowed {
		t.Fatalf("stale fetched item remained deliverable: %v %v", allowed, err)
	}
	last, end, err := db.ListNodeMQTTOutbox(ctx, "node", next)
	if err != nil || len(last) != 1 || end != 0 || last[0].ID != "event-200" {
		t.Fatalf("paused first page hid last item: %+v %d %v", last, end, err)
	}
}

func TestNodeMQTTAckDoesNotReuseInflightReplayGeneration(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _, err = db.RecordTerminalNodeMQTTTask(ctx, NodeMQTTTask{ID: "done", Node: "node", Action: "phones.list", RequestHash: "h", RequestJSON: json.RawMessage(`{}`)}, "expired", json.RawMessage(`null`), json.RawMessage(`{"id":"done","seq":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	selected, err := db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Second), 100)
	if err != nil || len(selected) != 1 {
		t.Fatal(err)
	}
	if err = db.AckNodeMQTTOutbox(ctx, "node", "task", "done", 1); err != nil {
		t.Fatal(err)
	}
	if err = db.RequeueLatestNodeMQTTTask(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	if err = db.CompleteNodeMQTTReplay(ctx, "node", "task", "done", 1, selected[0].ReplayGeneration); err != nil {
		t.Fatal(err)
	}
	items, err := db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Second), 100)
	if err != nil || len(items) != 1 || items[0].ReplayGeneration <= selected[0].ReplayGeneration {
		t.Fatalf("ACK reused generation and lost newer replay: %+v %v", items, err)
	}
}

func TestNodeMQTTPauseSurvivesReconnectRestartAndKeepsUnconfirmedPayload(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"id":"legacy-hello","event":"node.hello","data":{}}`)
	if err = db.EnqueueNodeMQTTEvent(ctx, "node", "legacy-hello", payload); err != nil {
		t.Fatal(err)
	}
	if err = db.EnqueueNodeMQTTEvent(ctx, "other", "unrelated", payload); err != nil {
		t.Fatal(err)
	}
	// Exercise the durable pause flag directly before its administrative API exists.
	if _, err = db.db.Exec(`UPDATE node_mqtt_outbox SET paused_at=1,pause_reason='operator paused legacy integration' WHERE node='node'`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.WakeNodeMQTTOutbox(ctx, "node"); err != nil {
		t.Fatal(err)
	}
	if err = db.CleanupNodeMQTT(ctx, "node", time.Now().Add(365*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	items, err := db.PendingNodeMQTTOutbox(ctx, "node", time.Now().Add(time.Hour), 100)
	if err != nil || len(items) != 0 {
		t.Fatalf("paused messages scheduled: %+v %v", items, err)
	}
	if count, err := db.CountPendingNodeMQTTOutbox(ctx, "node"); err != nil || count != 0 {
		t.Fatalf("paused debt mixed with active debt: %d %v", count, err)
	}
	var stored string
	var acked int64
	if err = db.db.QueryRow(`SELECT payload_json,acked_at FROM node_mqtt_outbox WHERE business_id='legacy-hello'`).Scan(&stored, &acked); err != nil || stored != string(payload) || acked != 0 {
		t.Fatalf("pause erased data or forged ACK: %s %d %v", stored, acked, err)
	}
	if count, err := db.CountPendingNodeMQTTOutbox(ctx, "other"); err != nil || count != 1 {
		t.Fatalf("other node affected: %d %v", count, err)
	}
}
