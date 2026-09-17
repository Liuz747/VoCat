package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vocat/internal/nodemqtt"
	"vocat/internal/store"
)

type outboxSettingsFake struct{}

func (outboxSettingsFake) Settings() nodemqtt.Settings                    { return nodemqtt.Settings{Node: "node-a"} }
func (outboxSettingsFake) Status(context.Context) nodemqtt.Status         { return nodemqtt.Status{} }
func (outboxSettingsFake) Apply(context.Context, nodemqtt.Settings) error { return nil }
func (outboxSettingsFake) Test(context.Context, nodemqtt.Settings) error  { return nil }

func TestMQTTOutboxPauseResumeIsScopedAtomicAndDoesNotAcknowledge(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, node := range []string{"node-a", "node-b"} {
		if err = db.EnqueueNodeMQTTEvent(ctx, node, node+"-event", json.RawMessage(`{"event":"node.hello","data":{"private":"must-not-return"}}`)); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{store: db, nodeMQTTSettings: outboxSettingsFake{}, maxRequestBodyBytes: 4096}
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/"+path, strings.NewReader(body))
		if !s.routeNodeMQTTSettingsAPI(w, r, path) {
			t.Fatal("outbox administration route missing")
		}
		return w
	}
	w := request(http.MethodPost, "settings/node-mqtt/outbox/pause", `{"reason":"legacy integration","items":[{"kind":"event","id":"node-a-event","seq":0},{"kind":"event","id":"node-b-event","seq":0}]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-node batch status=%d %s", w.Code, w.Body)
	}
	if n, _ := db.CountPendingNodeMQTTOutbox(ctx, "node-a"); n != 1 {
		t.Fatal("failed batch changed own item")
	}
	w = request(http.MethodPost, "settings/node-mqtt/outbox/pause", `{"reason":"legacy integration","items":[{"kind":"event","id":"node-a-event","seq":0}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("pause status=%d %s", w.Code, w.Body)
	}
	db.WakeNodeMQTTOutbox(ctx, "node-a")
	if items, err := db.PendingNodeMQTTOutbox(ctx, "node-a", time.Now().Add(time.Hour), 100); err != nil || len(items) != 0 {
		t.Fatalf("paused items scheduled: %+v %v", items, err)
	}
	w = request(http.MethodGet, "settings/node-mqtt/outbox", "")
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "must-not-return") || !strings.Contains(w.Body.String(), "legacy integration") {
		t.Fatalf("list leaks payload or lost pause: %d %s", w.Code, w.Body)
	}
	w = request(http.MethodPost, "settings/node-mqtt/outbox/resume", `{"items":[{"kind":"event","id":"node-a-event","seq":0}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("resume status=%d %s", w.Code, w.Body)
	}
	if n, _ := db.CountPendingNodeMQTTOutbox(ctx, "node-a"); n != 1 {
		t.Fatal("resume lost unconfirmed message")
	}
	if n, _ := db.CountPendingNodeMQTTOutbox(ctx, "node-b"); n != 1 {
		t.Fatal("other node changed")
	}
}
