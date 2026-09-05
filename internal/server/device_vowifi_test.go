package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vocat/internal/store"
	"vocat/internal/vowifi"
	vowifiruntime "vocat/internal/vowifi/runtime"
)

type fakeVoWiFiController struct {
	state      vowifi.State
	enabled    []bool
	reconnects int
	err        error
}

func (controller *fakeVoWiFiController) State(string) (vowifi.State, error) {
	return controller.state, controller.err
}

func (controller *fakeVoWiFiController) RequestEnabled(
	_ string,
	enabled bool,
) (vowifi.State, error) {
	controller.enabled = append(controller.enabled, enabled)
	return controller.state, controller.err
}

func (controller *fakeVoWiFiController) RequestReconnect(string) (vowifi.State, error) {
	controller.reconnects++
	return controller.state, controller.err
}

func TestShouldDeferModemSMSSync(t *testing.T) {
	tests := []struct {
		name  string
		state vowifi.State
		err   error
		want  bool
	}{
		{
			name:  "cellular session",
			state: vowifi.State{Phase: vowifi.PhaseIdle},
		},
		{
			name:  "vowifi SIM setup",
			state: vowifi.State{Enabled: true, Phase: vowifi.PhaseSIMReady},
			want:  true,
		},
		{
			name:  "vowifi IMS registration",
			state: vowifi.State{Enabled: true, Phase: vowifi.PhaseIMSReady},
			want:  true,
		},
		{
			name:  "stable vowifi catch-up",
			state: vowifi.State{Enabled: true, Phase: vowifi.PhaseSMSReady, SMSReady: true},
		},
		{
			name:  "failed vowifi cellular fallback",
			state: vowifi.State{Enabled: true, Phase: vowifi.PhaseFailed},
		},
		{
			name:  "unknown vowifi state",
			state: vowifi.State{Enabled: true, Phase: vowifi.PhaseSIMReady},
			err:   context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldDeferModemSMSSync(test.state, test.err); got != test.want {
				t.Fatalf("shouldDeferModemSMSSync() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestVoWiFiEnableUpdatesPolicyAndQueuesRuntime(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	config := store.Device{ID: "ec20", Name: "EC20"}
	if err := database.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	controller := &fakeVoWiFiController{
		state: vowifi.State{DeviceID: "ec20", Phase: vowifi.PhaseIdle},
	}
	server := &Server{
		store:               database,
		vowifi:              controller,
		logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxRequestBodyBytes: 4096,
	}
	request := httptest.NewRequest(
		http.MethodPatch,
		"/api/devices/ec20/vowifi",
		bytes.NewBufferString(`{"enabled":true}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleVoWiFiEnabled(response, request, config, true)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(controller.enabled) != 1 || !controller.enabled[0] {
		t.Fatalf("queued enables = %#v", controller.enabled)
	}
	stored, err := database.Device(context.Background(), "ec20")
	if err != nil {
		t.Fatal(err)
	}
	if !stored.VoWiFiEnabled {
		t.Fatal("VoWiFi policy was not persisted")
	}
}

func TestVoWiFiRepeatedEnableWhileStartingIsAccepted(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	config := store.Device{ID: "ec20", Name: "EC20", VoWiFiEnabled: true}
	if err := database.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	controller := &fakeVoWiFiController{
		// A reconnect briefly enters Stopping/Enabled=false while the persisted
		// policy remains enabled. Repeating "enable" is still the same intent.
		state: vowifi.State{DeviceID: "ec20", Phase: vowifi.PhaseStopping, Enabled: false},
		err:   vowifiruntime.ErrOperationInProgress,
	}
	server := &Server{
		store:               database,
		vowifi:              controller,
		logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxRequestBodyBytes: 4096,
	}
	request := httptest.NewRequest(
		http.MethodPatch,
		"/api/devices/ec20/vowifi",
		bytes.NewBufferString(`{"enabled":true}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleVoWiFiEnabled(response, request, config, true)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	stored, err := database.Device(context.Background(), "ec20")
	if err != nil {
		t.Fatal(err)
	}
	if !stored.VoWiFiEnabled {
		t.Fatal("idempotent enable reverted the desired policy")
	}
}

func TestVoWiFiReconnectRequiresEnabledPolicy(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	controller := &fakeVoWiFiController{}
	server := &Server{
		store:  database,
		vowifi: controller,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/devices/ec20/vowifi/actions/reconnect",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleVoWiFiReconnect(
		response,
		request,
		store.Device{ID: "ec20", Name: "EC20"},
		true,
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if controller.reconnects != 0 {
		t.Fatalf("reconnects = %d", controller.reconnects)
	}
}

// deleteTestVoWiFiController simulates the asynchronous runtime: a disable
// request completes on the next State() poll unless the test pins it busy.
type deleteTestVoWiFiController struct {
	state      vowifi.State
	enabled    []bool
	neverStops bool
}

func (controller *deleteTestVoWiFiController) State(string) (vowifi.State, error) {
	if len(controller.enabled) > 0 && !controller.enabled[len(controller.enabled)-1] && !controller.neverStops {
		controller.state.Active = false
		controller.state.TunnelReady = false
		controller.state.IMSReady = false
		controller.state.SMSReady = false
		controller.state.Enabled = false
		controller.state.Phase = vowifi.PhaseIdle
	}
	return controller.state, nil
}

func (controller *deleteTestVoWiFiController) RequestEnabled(_ string, enabled bool) (vowifi.State, error) {
	controller.enabled = append(controller.enabled, enabled)
	if !enabled {
		controller.state.Phase = vowifi.PhaseStopping
	}
	return controller.state, nil
}

func (controller *deleteTestVoWiFiController) RequestReconnect(string) (vowifi.State, error) {
	return controller.state, nil
}

func newDeleteTestServer(t *testing.T, controller VoWiFiController) (*Server, *store.Store) {
	t.Helper()
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	config := store.Device{ID: "ec20", Name: "EC20", VoWiFiEnabled: true}
	if err := database.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	return &Server{
		store:               database,
		vowifi:              controller,
		logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxRequestBodyBytes: 4096,
	}, database
}

// Regression: deleting a device record used to leave its IKE tunnel and IMS
// registration alive (orphan binding for up to the REGISTER expiry, ~3600 s),
// and the runtime worker kept failing with FOREIGN KEY errors. The record must
// be torn down (SIP de-registration, tunnel close) before the row disappears.
func TestDeleteDeviceStopsVoWiFiBeforeRemovingRecord(t *testing.T) {
	controller := &deleteTestVoWiFiController{state: vowifi.State{
		DeviceID: "ec20", Enabled: true, Active: true, TunnelReady: true, IMSReady: true, Phase: vowifi.PhaseSMSReady,
	}}
	server, database := newDeleteTestServer(t, controller)
	response := httptest.NewRecorder()
	server.handleDevicePath(response, httptest.NewRequest(http.MethodDelete, "/api/devices/ec20", nil), "ec20", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(controller.enabled) != 1 || controller.enabled[0] {
		t.Fatalf("runtime disable requests = %#v, want exactly one disable", controller.enabled)
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"vowifi_teardown":"stopped"`)) {
		t.Fatalf("body = %s, want vowifi_teardown stopped", response.Body.String())
	}
	if _, err := database.Device(context.Background(), "ec20"); err == nil {
		t.Fatal("device record still exists after DELETE")
	}
}

func TestDeleteDeviceStillRemovesRecordWhenTeardownTimesOut(t *testing.T) {
	previous := deviceDeleteTeardownTimeout
	deviceDeleteTeardownTimeout = 50 * time.Millisecond
	t.Cleanup(func() { deviceDeleteTeardownTimeout = previous })
	controller := &deleteTestVoWiFiController{neverStops: true, state: vowifi.State{
		DeviceID: "ec20", Enabled: true, Active: true, TunnelReady: true, Phase: vowifi.PhaseSMSReady,
	}}
	server, database := newDeleteTestServer(t, controller)
	response := httptest.NewRecorder()
	server.handleDevicePath(response, httptest.NewRequest(http.MethodDelete, "/api/devices/ec20", nil), "ec20", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"vowifi_teardown":"timeout"`)) {
		t.Fatalf("body = %s, want vowifi_teardown timeout", response.Body.String())
	}
	if _, err := database.Device(context.Background(), "ec20"); err == nil {
		t.Fatal("device record still exists after DELETE")
	}
}

func TestDeleteDeviceWithoutRunningVoWiFiSkipsTeardown(t *testing.T) {
	controller := &deleteTestVoWiFiController{state: vowifi.State{DeviceID: "ec20", Phase: vowifi.PhaseIdle}}
	server, database := newDeleteTestServer(t, controller)
	response := httptest.NewRecorder()
	server.handleDevicePath(response, httptest.NewRequest(http.MethodDelete, "/api/devices/ec20", nil), "ec20", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(controller.enabled) != 0 {
		t.Fatalf("runtime requests = %#v, want none for an idle device", controller.enabled)
	}
	if _, err := database.Device(context.Background(), "ec20"); err == nil {
		t.Fatal("device record still exists after DELETE")
	}
}
