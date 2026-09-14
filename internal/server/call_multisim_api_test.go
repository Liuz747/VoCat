package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"vocat/internal/vowifi"
	"vocat/internal/vowifi/multisim"
)

// fakeMultiSIMCallController adds the optional call capability on top of the
// SMS fake so the same server wiring serves both.
type fakeMultiSIMCallController struct {
	fakeMultiSIMController
	calls    []multisim.LineCall
	dialErr  error
	line     multisim.LineIdentity
	lastOp   string
	lastDev  string
	lastSel  string
	lastArg  string
	mediaErr error
}

func (f *fakeMultiSIMCallController) LineCalls(deviceID string) ([]multisim.LineCall, error) {
	f.lastOp, f.lastDev = "list", deviceID
	if !f.owned {
		return nil, multisim.ErrNotRegistered
	}
	return f.calls, nil
}

func (f *fakeMultiSIMCallController) DialCall(_ context.Context, deviceID, selector, number string) (vowifi.Call, multisim.LineIdentity, error) {
	f.lastOp, f.lastDev, f.lastSel, f.lastArg = "dial", deviceID, selector, number
	if f.dialErr != nil {
		return vowifi.Call{}, multisim.LineIdentity{}, f.dialErr
	}
	return vowifi.Call{ID: "call-1", Number: number, Direction: "outgoing", State: "dialing"}, f.line, nil
}

func (f *fakeMultiSIMCallController) AnswerCall(_ context.Context, deviceID, selector, callID string) (vowifi.Call, multisim.LineIdentity, error) {
	f.lastOp, f.lastDev, f.lastSel, f.lastArg = "answer", deviceID, selector, callID
	return vowifi.Call{ID: callID, Direction: "incoming", State: "active"}, f.line, nil
}

func (f *fakeMultiSIMCallController) HangupCall(_ context.Context, deviceID, selector, callID string) (multisim.LineIdentity, error) {
	f.lastOp, f.lastDev, f.lastSel, f.lastArg = "hangup", deviceID, selector, callID
	return f.line, nil
}

func (f *fakeMultiSIMCallController) CallMedia(_ context.Context, deviceID, selector, callID string) (vowifi.CallMedia, multisim.LineIdentity, error) {
	f.lastOp, f.lastDev, f.lastSel, f.lastArg = "media", deviceID, selector, callID
	return nil, f.line, f.mediaErr
}

func newMultiSIMCallTestServer(t *testing.T, controller MultiSIMController) *Server {
	t.Helper()
	sms := &fakeMultiSIMController{owned: true}
	server, _ := multiSIMSendTestServer(t, sms)
	server.multisim = controller
	return server
}

func TestMultiSIMCallsListCarriesLineIdentity(t *testing.T) {
	line := multisim.LineIdentity{DeviceID: "ec20", SessionID: "multisim-abc", ICCID: "8944100000000000002", PhoneNumber: "+12025550100"}
	controller := &fakeMultiSIMCallController{
		fakeMultiSIMController: fakeMultiSIMController{owned: true},
		calls:                  []multisim.LineCall{{Line: line, Call: vowifi.Call{ID: "in-1", Number: "+12025550199", Direction: "incoming", State: "ringing"}}},
	}
	server := newMultiSIMCallTestServer(t, controller)
	w := multiRequest(t, server, "GET", "/api/devices/ec20/calls", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	var payload struct {
		Data struct {
			Transport string `json:"transport"`
			MultiSIM  bool   `json:"multisim"`
			Calls     []struct {
				ID          string `json:"id"`
				State       string `json:"state"`
				SessionID   string `json:"session_id"`
				ICCID       string `json:"iccid"`
				PhoneNumber string `json:"phone_number"`
			} `json:"calls"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Data.Transport != "vowifi" || !payload.Data.MultiSIM || len(payload.Data.Calls) != 1 {
		t.Fatalf("payload = %s", w.Body.String())
	}
	got := payload.Data.Calls[0]
	if got.ID != "in-1" || got.State != "ringing" || got.SessionID != "multisim-abc" || got.ICCID != line.ICCID || got.PhoneNumber != line.PhoneNumber {
		t.Fatalf("call view = %+v", got)
	}
}

func TestMultiSIMDialRoutesToSelectedLine(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		dialErr  error
		wantCode int
		wantErr  string
		wantSel  string
	}{
		{name: "session selector", body: `{"number":"+12025550100","session_id":"multisim-abc"}`, wantCode: http.StatusAccepted, wantSel: "multisim-abc"},
		{name: "iccid selector", body: `{"number":"+12025550100","iccid":"8944100000000000002"}`, wantCode: http.StatusAccepted, wantSel: "8944100000000000002"},
		{name: "line required", body: `{"number":"+12025550100"}`, dialErr: multisim.ErrLineRequired, wantCode: http.StatusConflict, wantErr: "multisim_line_required"},
		{name: "unknown line", body: `{"number":"+12025550100","iccid":"8944100000000000009"}`, dialErr: multisim.ErrNotRegistered, wantCode: http.StatusConflict, wantErr: "multisim_line_unavailable", wantSel: "8944100000000000009"},
		{name: "line not ready", body: `{"number":"+12025550100","session_id":"multisim-abc"}`, dialErr: multisim.ErrLineNotReady, wantCode: http.StatusConflict, wantErr: "multisim_line_not_ready", wantSel: "multisim-abc"},
		{name: "group busy", body: `{"number":"+12025550100","session_id":"multisim-abc"}`, dialErr: multisim.ErrOperationInProgress, wantCode: http.StatusConflict, wantErr: "multisim_active", wantSel: "multisim-abc"},
		{name: "ims rejected", body: `{"number":"+12025550100","session_id":"multisim-abc"}`, dialErr: errors.New("ims: send SIP INVITE: broken pipe"), wantCode: http.StatusBadGateway, wantErr: "vowifi_call_failed", wantSel: "multisim-abc"},
		{name: "invalid number never reaches the line", body: `{"number":"12;ATH","session_id":"multisim-abc"}`, wantCode: http.StatusBadRequest, wantErr: "invalid_number"},
		{name: "duration bounded", body: `{"number":"+12025550100","session_id":"multisim-abc","duration_seconds":601}`, wantCode: http.StatusBadRequest, wantErr: "invalid_duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			controller := &fakeMultiSIMCallController{
				fakeMultiSIMController: fakeMultiSIMController{owned: true},
				dialErr:                tc.dialErr,
				line:                   multisim.LineIdentity{DeviceID: "ec20", SessionID: "multisim-abc", ICCID: "8944100000000000002", PhoneNumber: "+12025550100"},
			}
			server := newMultiSIMCallTestServer(t, controller)
			w := multiRequest(t, server, "POST", "/api/devices/ec20/calls/dial", tc.body)
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body=%s", w.Code, tc.wantCode, w.Body.String())
			}
			if tc.wantErr != "" {
				var envelope errorEnvelope
				if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope.Error.Code != tc.wantErr {
					t.Fatalf("error code = %q, want %q; body=%s", envelope.Error.Code, tc.wantErr, w.Body.String())
				}
			}
			if controller.lastSel != tc.wantSel {
				t.Fatalf("selector passed to the group = %q, want %q", controller.lastSel, tc.wantSel)
			}
			if tc.wantCode == http.StatusAccepted {
				var payload struct {
					Data struct {
						Accepted  bool   `json:"accepted"`
						CallID    string `json:"call_id"`
						SessionID string `json:"session_id"`
						MultiSIM  bool   `json:"multisim"`
						Call      struct {
							State string `json:"state"`
						} `json:"call"`
					} `json:"data"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if !payload.Data.Accepted || payload.Data.CallID != "call-1" || payload.Data.SessionID != "multisim-abc" || !payload.Data.MultiSIM || payload.Data.Call.State != "dialing" {
					t.Fatalf("payload = %s", w.Body.String())
				}
			}
		})
	}
}

func TestMultiSIMAnswerAndHangupResolveByCallID(t *testing.T) {
	controller := &fakeMultiSIMCallController{
		fakeMultiSIMController: fakeMultiSIMController{owned: true},
		line:                   multisim.LineIdentity{DeviceID: "ec20", SessionID: "multisim-abc", ICCID: "8944100000000000002"},
	}
	server := newMultiSIMCallTestServer(t, controller)
	w := multiRequest(t, server, "POST", "/api/devices/ec20/calls/answer", `{"call_id":"in-1"}`)
	if w.Code != http.StatusAccepted || controller.lastOp != "answer" || controller.lastSel != "" || controller.lastArg != "in-1" {
		t.Fatalf("answer: status=%d op=%s sel=%q arg=%q body=%s", w.Code, controller.lastOp, controller.lastSel, controller.lastArg, w.Body.String())
	}
	w = multiRequest(t, server, "POST", "/api/devices/ec20/calls/hangup", `{"call_id":"in-1","session_id":"multisim-abc"}`)
	if w.Code != http.StatusAccepted || controller.lastOp != "hangup" || controller.lastSel != "multisim-abc" || controller.lastArg != "in-1" {
		t.Fatalf("hangup: status=%d op=%s sel=%q arg=%q body=%s", w.Code, controller.lastOp, controller.lastSel, controller.lastArg, w.Body.String())
	}
	w = multiRequest(t, server, "POST", "/api/devices/ec20/calls/hangup", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("hangup without call id: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestMultiSIMCallsWithoutCallSupportReport501(t *testing.T) {
	server := newMultiSIMCallTestServer(t, &fakeMultiSIMController{owned: true})
	w := multiRequest(t, server, "GET", "/api/devices/ec20/calls", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	w = multiRequest(t, server, "POST", "/api/devices/ec20/calls/dial", `{"number":"+12025550100","session_id":"multisim-abc"}`)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("dial status = %d; body=%s", w.Code, w.Body.String())
	}
}

func TestMultiSIMCallMediaRejectsMissingCall(t *testing.T) {
	controller := &fakeMultiSIMCallController{
		fakeMultiSIMController: fakeMultiSIMController{owned: true},
		mediaErr:               vowifi.ErrNotRunning,
	}
	server := newMultiSIMCallTestServer(t, controller)
	w := multiRequest(t, server, "GET", "/api/devices/ec20/calls/media", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("media without call id: status=%d body=%s", w.Code, w.Body.String())
	}
	w = multiRequest(t, server, "GET", "/api/devices/ec20/calls/media?call_id=in-1&session_id=multisim-abc", "")
	if w.Code != http.StatusConflict || controller.lastSel != "multisim-abc" || controller.lastArg != "in-1" {
		t.Fatalf("media: status=%d sel=%q arg=%q body=%s", w.Code, controller.lastSel, controller.lastArg, w.Body.String())
	}
}
