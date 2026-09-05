package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/store"
	"vocat/internal/vowifi"
)

func TestNextRotationProfileCyclesAfterCurrentAndFallsBackToFirst(t *testing.T) {
	profiles := []automaticTaskRotationProfile{
		{ICCID: "8901240527185779025", AID: "A0"},
		{ICCID: "8901240527185778332", AID: "A0"},
		{ICCID: "8901240527185778316", AID: "A0"},
	}
	for _, test := range []struct {
		current string
		want    string
	}{
		{current: "8901240527185779025", want: "8901240527185778332"},
		{current: "8901240527185778332", want: "8901240527185778316"},
		{current: "8901240527185778316", want: "8901240527185779025"},
		{current: " 8901240527185778316 ", want: "8901240527185779025"},
		{current: "unknown", want: "8901240527185779025"},
		{current: "", want: "8901240527185779025"},
	} {
		got, err := nextRotationProfile(profiles, test.current)
		if err != nil || got.ICCID != test.want {
			t.Fatalf("nextRotationProfile(current=%q) = %+v, %v; want %s", test.current, got, err, test.want)
		}
	}
	if _, err := nextRotationProfile(nil, "x"); err == nil {
		t.Fatal("empty rotation list did not error")
	}
}

func TestProfileRotationTaskDecodeValidation(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, logger: regionTestLogger(), maxRequestBodyBytes: 1 << 20}
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/automatic-tasks", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.handleAutomaticTasks(response, request)
		return response
	}
	base := func(overrides string) string {
		return `{"name":"rotate","enabled":true,"device_id":"ec20","profile_iccid":"8901240527185779025",` +
			`"task_type":"profile_rotation","environment":"vowifi","interval_days":1,"interval_seconds":30,` +
			`"start_date":"2026-09-04","run_time":"00:00","timezone":"America/Los_Angeles","retry_count":1,"notify":true,` +
			`"payload":{"profiles":[{"iccid":"8901240527185779025","aid":"A0"},{"iccid":"8901240527185778332","aid":"A0"}]}` +
			overrides + `}`
	}
	response := post(base(""))
	if response.Code != http.StatusCreated {
		t.Fatalf("valid rotation task status = %d, body=%s", response.Code, response.Body.String())
	}
	var created struct {
		Data store.AutomaticTask `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Data.TaskType != "profile_rotation" || created.Data.IntervalSeconds != 30 {
		t.Fatalf("created task = %+v", created.Data)
	}
	var payload automaticTaskPayload
	if err := json.Unmarshal(created.Data.Payload, &payload); err != nil || len(payload.Profiles) != 2 {
		t.Fatalf("persisted payload = %s, %v", created.Data.Payload, err)
	}

	for name, body := range map[string]string{
		"single profile":     strings.Replace(base(""), `,{"iccid":"8901240527185778332","aid":"A0"}`, "", 1),
		"duplicate profile":  strings.Replace(base(""), `8901240527185778332`, `8901240527185779025`, 1),
		"interval too short": strings.Replace(base(""), `"interval_seconds":30`, `"interval_seconds":3`, 1),
		"no interval":        strings.Replace(base(""), `"interval_seconds":30`, `"interval_seconds":0`, 1),
		"cellular":           strings.Replace(base(""), `"environment":"vowifi"`, `"environment":"cellular"`, 1),
	} {
		if response := post(body); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, body=%s", name, response.Code, response.Body.String())
		}
	}
	// Existing task types must not accept a second-based interval.
	sms := `{"name":"sms","enabled":true,"device_id":"ec20","profile_iccid":"8901240527185779025","task_type":"sms",` +
		`"environment":"vowifi","interval_days":1,"interval_seconds":30,"start_date":"2026-09-04","run_time":"00:00",` +
		`"timezone":"UTC","payload":{"phone":"+16505551692","message":"hi"}}`
	if response := post(sms); response.Code != http.StatusBadRequest {
		t.Errorf("sms with interval_seconds: status = %d, body=%s", response.Code, response.Body.String())
	}
}

// rotationSwitchController records the order of VoWiFi stop vs. eSIM switch.
type rotationSwitchController struct {
	fakeDeviceController
	log      *[]string
	switched *string
}

func (c rotationSwitchController) ESIMSwitchProfile(_ context.Context, _ string, iccid string, _ string) error {
	*c.log = append(*c.log, "switch:"+iccid)
	*c.switched = iccid
	return nil
}

func (c rotationSwitchController) Refresh(context.Context, string) (device.Snapshot, error) {
	return device.Snapshot{ICCID: *c.switched}, nil
}

type rotationVoWiFiController struct {
	state vowifi.State
	log   *[]string
}

func (c *rotationVoWiFiController) State(string) (vowifi.State, error) { return c.state, nil }
func (c *rotationVoWiFiController) RequestEnabled(_ string, enabled bool) (vowifi.State, error) {
	*c.log = append(*c.log, fmt.Sprintf("vowifi:%v", enabled))
	if enabled {
		c.state = vowifi.State{Enabled: true, Active: true, Phase: vowifi.PhaseSMSReady}
	} else {
		c.state = vowifi.State{Phase: vowifi.PhaseIdle}
	}
	return c.state, nil
}
func (c *rotationVoWiFiController) RequestReconnect(string) (vowifi.State, error) { return c.state, nil }

func TestAutomaticProfileSwitchStopsVoWiFiBeforeEnablingNextProfile(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20", Name: "EC20", VoWiFiEnabled: true}); err != nil {
		t.Fatal(err)
	}
	var log []string
	switched := "8901240527185779025"
	devices := rotationSwitchController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{ID: "ec20", Discovered: true, Snapshot: &device.Snapshot{ICCID: "8901240527185779025"}}},
		log: &log, switched: &switched,
	}
	vowifiController := &rotationVoWiFiController{state: vowifi.State{Enabled: true, Active: true, Phase: vowifi.PhaseSMSReady}, log: &log}
	server := &Server{store: database, devices: devices, vowifi: vowifiController, logger: regionTestLogger()}
	task := store.AutomaticTask{DeviceID: "ec20", ProfileICCID: "8901240527185778332", TaskType: "profile_rotation", Environment: "vowifi"}
	if _, _, _, err := server.ensureAutomaticTaskProfile(ctx, task, func(string) {}); err != nil {
		t.Fatalf("ensureAutomaticTaskProfile: %v", err)
	}
	want := []string{"vowifi:false", "switch:8901240527185778332"}
	if len(log) != len(want) || log[0] != want[0] || log[1] != want[1] {
		t.Fatalf("operation order = %v, want %v (the old profile must de-register before the eUICC switch)", log, want)
	}
}

func TestProfileRotationTaskEndTimeAndDwellFloor(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, logger: regionTestLogger(), maxRequestBodyBytes: 1 << 20}
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/automatic-tasks", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.handleAutomaticTasks(response, request)
		return response
	}
	base := func(extra string) string {
		return `{"name":"rotate","enabled":true,"device_id":"ec20","profile_iccid":"a",` +
			`"task_type":"profile_rotation","environment":"vowifi","interval_days":1,"interval_seconds":5,` +
			`"start_date":"2030-01-01","run_time":"10:00","timezone":"Asia/Shanghai",` +
			`"payload":{"profiles":[{"iccid":"a"},{"iccid":"b"}]}` + extra + `}`
	}
	response := post(base(`,"end_date":"2030-01-02","end_time":"09:30"`))
	if response.Code != http.StatusCreated {
		t.Fatalf("rotation with end time: status = %d, body=%s", response.Code, response.Body.String())
	}
	var created struct {
		Data store.AutomaticTask `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	if want := time.Date(2030, 1, 2, 9, 30, 0, 0, shanghai); !created.Data.EndAt.Equal(want) {
		t.Fatalf("end_at = %v, want %v", created.Data.EndAt, want)
	}
	if created.Data.IntervalSeconds != 5 {
		t.Fatalf("5 s dwell not accepted: %+v", created.Data)
	}
	if response := post(base("")); response.Code != http.StatusCreated {
		t.Fatalf("rotation without end time: status = %d, body=%s", response.Code, response.Body.String())
	}
	for name, body := range map[string]string{
		"end before start": base(`,"end_date":"2029-12-31","end_time":"10:00"`),
		"end equals start": base(`,"end_date":"2030-01-01","end_time":"10:00"`),
		"half end":         base(`,"end_date":"2030-01-02"`),
		"dwell 3s":         strings.Replace(base(""), `"interval_seconds":5`, `"interval_seconds":3`, 1),
	} {
		if response := post(body); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, body=%s", name, response.Code, response.Body.String())
		}
	}
}

func TestFormatRotationTimingSummarisesSwitchAndTunnel(t *testing.T) {
	got := formatRotationTiming(47*time.Second+300*time.Millisecond, 3800*time.Millisecond)
	if got != "换档 47.3 s · 隧道 3.8 s" {
		t.Fatalf("formatRotationTiming = %q", got)
	}
}
