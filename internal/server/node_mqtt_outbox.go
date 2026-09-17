package server

import (
	"errors"
	"net/http"
	"strconv"

	"vocat/internal/store"
)

func (s *Server) handleNodeMQTTOutbox(w http.ResponseWriter, r *http.Request, operation string) {
	if s.nodeMQTTSettings == nil || s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "node_mqtt_unavailable", "总云 MQTT 不可用")
		return
	}
	var after int64
	if value := r.URL.Query().Get("after"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "after must be a nonnegative integer")
			return
		}
		after = parsed
	}
	node := s.nodeMQTTSettings.Settings().Node
	if operation != "" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		var body struct {
			Items  []store.NodeMQTTOutboxKey `json:"items"`
			Reason string                    `json:"reason"`
		}
		if err := s.decodeJSON(w, r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if err := s.store.SetNodeMQTTOutboxPaused(r.Context(), node, body.Items, operation == "pause", body.Reason); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, store.ErrNotFound) {
				status = http.StatusNotFound
			}
			writeError(w, status, "outbox_update_failed", err.Error())
			return
		}
	} else if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	items, next, err := s.store.ListNodeMQTTOutbox(r.Context(), node, after)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "outbox_read_failed", "读取补传队列失败")
		return
	}
	pending, err := s.store.CountPendingNodeMQTTOutbox(r.Context(), node)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "outbox_read_failed", "读取补传计数失败")
		return
	}
	paused, err := s.store.CountPausedNodeMQTTOutbox(r.Context(), node)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "outbox_read_failed", "读取暂停计数失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"node": node, "outbox_pending": pending, "outbox_paused": paused, "items": items, "complete": next == 0, "next_after": next}})
}
