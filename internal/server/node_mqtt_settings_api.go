package server

import (
	"context"
	"net/http"
	"strings"

	"vocat/internal/nodemqtt"
)

type NodeMQTTSettingsController interface {
	Settings() nodemqtt.Settings
	Status(context.Context) nodemqtt.Status
	Apply(context.Context, nodemqtt.Settings) error
	Test(context.Context, nodemqtt.Settings) error
}

func (s *Server) routeNodeMQTTSettingsAPI(w http.ResponseWriter, r *http.Request, cleanPath string) bool {
	segments := splitAPIPath(strings.Trim(cleanPath, "/"))
	if len(segments) >= 3 && segments[0] == "settings" && segments[1] == "node-mqtt" && segments[2] == "outbox" {
		if len(segments) == 3 {
			s.handleNodeMQTTOutbox(w, r, "")
			return true
		}
		if len(segments) == 4 && (segments[3] == "pause" || segments[3] == "resume") {
			s.handleNodeMQTTOutbox(w, r, segments[3])
			return true
		}
	}
	if len(segments) == 2 && segments[0] == "settings" && segments[1] == "node-mqtt" {
		s.handleNodeMQTTSettings(w, r)
		return true
	}
	if len(segments) == 3 && segments[0] == "settings" && segments[1] == "node-mqtt" && segments[2] == "test" {
		s.handleNodeMQTTTest(w, r)
		return true
	}
	return false
}

func (s *Server) handleNodeMQTTSettings(w http.ResponseWriter, r *http.Request) {
	if s.nodeMQTTSettings == nil {
		writeError(w, http.StatusServiceUnavailable, "node_mqtt_unavailable", "总云 MQTT 设置服务不可用")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"settings": s.nodeMQTTSettings.Settings(), "status": s.nodeMQTTSettings.Status(r.Context())}})
	case http.MethodPut:
		var settings nodemqtt.Settings
		if err := s.decodeJSON(w, r, &settings); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if err := s.nodeMQTTSettings.Apply(r.Context(), settings); err != nil {
			writeError(w, http.StatusBadGateway, "node_mqtt_apply_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"settings": s.nodeMQTTSettings.Settings(), "status": s.nodeMQTTSettings.Status(r.Context())}})
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *Server) handleNodeMQTTTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.nodeMQTTSettings == nil {
		writeError(w, http.StatusServiceUnavailable, "node_mqtt_unavailable", "总云 MQTT 设置服务不可用")
		return
	}
	var settings nodemqtt.Settings
	if err := s.decodeJSON(w, r, &settings); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.nodeMQTTSettings.Test(r.Context(), settings); err != nil {
		writeError(w, http.StatusBadGateway, "node_mqtt_test_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"connected": true}})
}
