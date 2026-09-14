package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/multisim"
)

// MultiSIMCallController is the optional call capability of the multi-tunnel
// controller. Lines are selected exactly like SendSMS: by ICCID or session ID.
// Answer, hang-up and media may leave the selector empty and let the call ID
// find its line.
type MultiSIMCallController interface {
	LineCalls(string) ([]multisim.LineCall, error)
	DialCall(context.Context, string, string, string) (vowifi.Call, multisim.LineIdentity, error)
	AnswerCall(context.Context, string, string, string) (vowifi.Call, multisim.LineIdentity, error)
	HangupCall(context.Context, string, string, string) (multisim.LineIdentity, error)
	CallMedia(context.Context, string, string, string) (vowifi.CallMedia, multisim.LineIdentity, error)
}

// multiSIMCallJSON flattens one call with the line that carries it.
type multiSIMCallJSON struct {
	vowifi.Call
	SessionID   string `json:"session_id"`
	ICCID       string `json:"iccid"`
	PhoneNumber string `json:"phone_number,omitempty"`
}

func multiSIMCallView(identity multisim.LineIdentity, call vowifi.Call) multiSIMCallJSON {
	return multiSIMCallJSON{Call: call, SessionID: identity.SessionID, ICCID: identity.ICCID, PhoneNumber: identity.PhoneNumber}
}

// handleMultiSIMCallRoute serves /calls, /calls/{dial|answer|hangup} and
// /calls/media for a device owned by a multi-tunnel group. The physical modem
// sits in airplane mode while the group owns it, so every call goes through
// one line's IMS session and there is no AT fallback.
func (s *Server) handleMultiSIMCallRoute(w http.ResponseWriter, r *http.Request, config store.Device, tail []string) bool {
	if len(tail) == 0 || tail[0] != "calls" || len(tail) > 2 {
		return false
	}
	if s.multisim == nil {
		writeMultiSIMConflict(w)
		return true
	}
	controller, ok := s.multisim.(MultiSIMCallController)
	if !ok {
		writeError(w, http.StatusNotImplemented, "vowifi_voice_unavailable", "the multi-tunnel runtime does not expose voice-call signalling")
		return true
	}
	if len(tail) == 1 {
		return s.handleMultiSIMCalls(w, r, config, controller)
	}
	switch tail[1] {
	case "dial", "answer", "hangup":
		return s.handleMultiSIMCallAction(w, r, config, controller, tail[1])
	case "media":
		return s.handleMultiSIMCallMedia(w, r, config, controller)
	default:
		writeError(w, http.StatusNotFound, "not_found", "call action not found")
		return true
	}
}

func (s *Server) handleMultiSIMCalls(w http.ResponseWriter, r *http.Request, config store.Device, controller MultiSIMCallController) bool {
	if !requireMethod(w, r, http.MethodGet) {
		return true
	}
	calls, err := controller.LineCalls(config.ID)
	if err != nil {
		writeMultiSIMCallError(w, err)
		return true
	}
	views := make([]multiSIMCallJSON, 0, len(calls))
	for _, item := range calls {
		views = append(views, multiSIMCallView(item.Line, item.Call))
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
		"device_id": config.ID, "transport": "vowifi", "multisim": true, "calls": views,
	}})
	return true
}

func (s *Server) handleMultiSIMCallAction(w http.ResponseWriter, r *http.Request, config store.Device, controller MultiSIMCallController, action string) bool {
	if !requireMethod(w, r, http.MethodPost) {
		return true
	}
	var request struct {
		Number          string `json:"number"`
		DurationSeconds int    `json:"duration_seconds"`
		CallID          string `json:"call_id"`
		ICCID           string `json:"iccid"`
		SessionID       string `json:"session_id"`
	}
	if err := s.decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return true
	}
	selector := firstNonEmpty(strings.TrimSpace(request.ICCID), strings.TrimSpace(request.SessionID))
	number := strings.TrimSpace(request.Number)
	callID := strings.TrimSpace(request.CallID)
	duration := time.Duration(request.DurationSeconds) * time.Second
	if action == "dial" {
		if !validDialNumber(number) {
			writeError(w, http.StatusBadRequest, "invalid_number", "phone number is invalid")
			return true
		}
		if duration < 0 || duration > maxCallDuration {
			writeError(w, http.StatusBadRequest, "invalid_duration", "duration_seconds must be 0 (no automatic hang-up) or between 1 and 600")
			return true
		}
	} else if callID == "" {
		writeError(w, http.StatusBadRequest, "invalid_call_id", "call_id is required")
		return true
	}

	var (
		call     vowifi.Call
		identity multisim.LineIdentity
		err      error
	)
	switch action {
	case "dial":
		call, identity, err = controller.DialCall(r.Context(), config.ID, selector, number)
	case "answer":
		call, identity, err = controller.AnswerCall(r.Context(), config.ID, selector, callID)
	case "hangup":
		identity, err = controller.HangupCall(r.Context(), config.ID, selector, callID)
	}
	if err != nil {
		s.logger.Warn("multi-tunnel call operation failed",
			"category", "call", "event", "call."+action,
			"device_id", config.ID, "session_id", identity.SessionID, "number", number, "call_id", callID,
			"transport", "vowifi", "raw_error", err,
		)
		writeMultiSIMCallError(w, err)
		return true
	}
	if action == "dial" {
		callID = call.ID
		if duration > 0 {
			go s.hangupMultiSIMCallAfter(config.ID, identity.SessionID, callID, duration)
		}
	}
	s.recordAudit(r.Context(), "admin", "call."+action, "device", config.ID, "success", "vowifi")
	response := map[string]any{
		"accepted": true, "action": action, "number": number, "call_id": callID,
		"duration_seconds": int(duration / time.Second), "transport": "vowifi", "multisim": true,
		"session_id": identity.SessionID, "iccid": identity.ICCID, "phone_number": identity.PhoneNumber,
	}
	if action != "hangup" {
		response["call"] = multiSIMCallView(identity, call)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"data": response})
	return true
}

func (s *Server) handleMultiSIMCallMedia(w http.ResponseWriter, r *http.Request, config store.Device, controller MultiSIMCallController) bool {
	if !requireMethod(w, r, http.MethodGet) {
		return true
	}
	callID := strings.TrimSpace(r.URL.Query().Get("call_id"))
	if callID == "" || len(callID) > 256 {
		writeError(w, http.StatusBadRequest, "invalid_call_id", "call_id is required")
		return true
	}
	selector := firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("iccid")), strings.TrimSpace(r.URL.Query().Get("session_id")))
	media, _, err := controller.CallMedia(r.Context(), config.ID, selector, callID)
	if err != nil {
		writeMultiSIMCallError(w, err)
		return true
	}
	s.serveCallMediaSocket(w, r, media, config.ID, callID)
	return true
}

func (s *Server) hangupMultiSIMCallAfter(deviceID, sessionID, callID string, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	<-timer.C
	controller, ok := s.multisim.(MultiSIMCallController)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := controller.HangupCall(ctx, deviceID, sessionID, callID); err != nil {
		s.logger.Warn("automatic multi-tunnel call hangup failed", "device_id", deviceID, "session_id", sessionID, "call_id", callID, "error", err)
	}
}

// writeMultiSIMCallError maps line-selection failures to the same codes the
// SMS path uses, so the UI can treat both alike; anything else is the IMS
// session's own verdict and is passed through verbatim.
func writeMultiSIMCallError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, multisim.ErrLineRequired):
		writeError(w, http.StatusConflict, "multisim_line_required", "此设备开启了多隧道，通话时请指定线路（iccid 或 session_id）。")
	case errors.Is(err, multisim.ErrNotRegistered):
		writeError(w, http.StatusConflict, "multisim_line_unavailable", "该线路没有启用多隧道，或没有这通电话")
	case errors.Is(err, multisim.ErrLineNotReady), errors.Is(err, vowifi.ErrNotRunning):
		writeError(w, http.StatusConflict, "multisim_line_not_ready", "该线路的 IMS 注册尚未就绪，请等待线路显示 ims_ready 后再通话。")
	case errors.Is(err, multisim.ErrOperationInProgress), errors.Is(err, multisim.ErrClosed):
		writeMultiSIMConflict(w)
	default:
		writeError(w, http.StatusBadGateway, "vowifi_call_failed", err.Error())
	}
}
