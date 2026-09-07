package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
	"vocat/internal/vowifi/multisim"
)

type fakeMultiSIMController struct {
	owned       bool
	keepOwned   bool
	applyErr    error
	applies     int
	state       multisim.GroupState
	reconnected string
	refreshed   string
}

func (f *fakeMultiSIMController) Apply(_ context.Context, c multisim.Config) error {
	f.applies++
	if f.applyErr != nil {
		return f.applyErr
	}
	f.owned = c.Enabled || f.keepOwned
	f.state = multisim.GroupState{DeviceID: c.DeviceID, Enabled: c.Enabled, Busy: c.Enabled, Phase: "starting"}
	return nil
}
func (f *fakeMultiSIMController) Owns(string) bool                 { return f.owned }
func (f *fakeMultiSIMController) State(string) multisim.GroupState { return f.state }
func (f *fakeMultiSIMController) Reconnect(_ string, iccid string) error {
	f.reconnected = iccid
	return nil
}

const multiBody = `{"enabled":true,"profiles":[{"iccid":"8944100000000000001","aid":"A000"},{"iccid":"8944100000000000002","aid":"A000"}]}`

func multiRequest(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	if !s.routeDeviceAPI(w, httptest.NewRequest(method, path, bytes.NewBufferString(body))) {
		t.Fatal("route missed")
	}
	return w
}

func TestMultiSIMApplyAcceptanceAndRollback(t *testing.T) {
	s, db, _ := newProfileBindingTestServer(t)
	f := &fakeMultiSIMController{applyErr: errors.New("prepare refused")}
	s.multisim = f
	w := multiRequest(t, s, http.MethodPut, "/api/devices/ec20/multisim", multiBody)
	if w.Code < 400 {
		t.Fatal("rejected Apply accepted")
	}
	if _, err := db.MultiSIMConfig(context.Background(), "ec20"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("rejected config was persisted", err)
	}
	f.applyErr = nil
	w = multiRequest(t, s, http.MethodPut, "/api/devices/ec20/multisim", multiBody)
	if w.Code != http.StatusAccepted || strings.Contains(w.Body.String(), `"sms_ready":true`) {
		t.Fatal("acceptance incorrectly claimed readiness", w.Code, w.Body.String())
	}
	if cfg, err := db.MultiSIMConfig(context.Background(), "ec20"); err != nil || !cfg.Enabled {
		t.Fatal("config missing", err)
	}
	w = multiRequest(t, s, http.MethodGet, "/api/devices/ec20/multisim", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"phase":"starting"`) {
		t.Fatal("missing runtime state", w.Body.String())
	}
}

func TestMultiSIMGuardsMutationsAndSIMInventory(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{owned: true}
	for _, item := range []struct{ method, path string }{{"POST", "actions/at"}, {"POST", "actions/refresh"}, {"POST", "esim/actions/switch"}, {"PUT", ""}, {"GET", "esim"}} {
		w := multiRequest(t, s, item.method, "/api/devices/ec20/"+item.path, `{}`)
		if w.Code != http.StatusConflict {
			t.Fatalf("unguarded %s %s: %d", item.method, item.path, w.Code)
		}
	}
	w := multiRequest(t, s, "GET", "/api/devices/ec20/multisim", "")
	if w.Code != http.StatusOK {
		t.Fatal("state endpoint blocked")
	}
	_, err := s.executeAutomaticTask(context.Background(), store.AutomaticTask{DeviceID: "ec20"}, func(string) {})
	if err == nil {
		t.Fatal("queued task ran during ownership")
	}
}

func TestMultiSIMDeleteWaitsForOwnershipRelease(t *testing.T) {
	s, db, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{owned: true, keepOwned: true}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/api/devices/ec20", nil).WithContext(ctx)
	s.routeDeviceAPI(w, r)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected deferred deletion: %d %s", w.Code, w.Body.String())
	}
	if _, err := db.Device(context.Background(), "ec20"); err != nil {
		t.Fatal("device deleted before sessions closed")
	}
}

func (f *fakeMultiSIMController) Refresh(_ string, iccid string) error {
	f.refreshed = iccid
	return nil
}
func TestMultiSIMRefreshAcceptsKnownLineOnly(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	f := &fakeMultiSIMController{}
	s.multisim = f
	if w := multiRequest(t, s, "PUT", "/api/devices/ec20/multisim", multiBody); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := multiRequest(t, s, "POST", "/api/devices/ec20/multisim/lines/8944100000000000001/refresh", "")
	if w.Code != 202 || f.refreshed != "8944100000000000001" {
		t.Fatal(w.Code, w.Body.String(), f.refreshed)
	}
	f.refreshed = ""
	w = multiRequest(t, s, "POST", "/api/devices/ec20/multisim/lines/8944100000000000009/refresh", "")
	if w.Code != 409 || f.refreshed != "" {
		t.Fatal("unknown line accepted", w.Code, f.refreshed)
	}
}

func TestMultiSIMInventoryWaitsForDeviceGateThenRechecksOwnership(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	f := &fakeMultiSIMController{}
	s.multisim = f
	unlock, err := s.lockMultiSIMDevice(context.Background(), "ec20")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		s.routeDeviceAPI(w, httptest.NewRequest("GET", "/api/devices/ec20/esim", nil))
		done <- w
	}()
	select {
	case w := <-done:
		unlock()
		t.Fatalf("inventory bypassed device transaction: %d", w.Code)
	case <-time.After(20 * time.Millisecond):
	}
	if w := multiRequest(t, s, "GET", "/api/devices/ec20/multisim", ""); w.Code != 200 {
		unlock()
		t.Fatal("state GET blocked")
	}
	f.owned = true
	unlock()
	select {
	case w := <-done:
		if w.Code != 409 {
			t.Fatalf("ownership not rechecked: %d", w.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("inventory gate stuck")
	}
}

func TestMultiSIMModemDeletionBlockedButIMSDeletionAllowed(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{owned: true}
	if err := s.deleteSMSMessages(context.Background(), []store.SMSMessage{{DeviceID: "ec20", Source: "cellular_at", MessageID: "modem:test"}}); !errors.Is(err, errMultiSIMActive) {
		t.Fatal("modem deletion not guarded", err)
	}
	stored, err := s.store.SaveSMSMessage(context.Background(), store.SMSMessage{DeviceID: "ec20", Source: "ims", MessageID: "ims-test", Peer: "test", Body: "test", Direction: "inbound", Timestamp: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSMSMessages(context.Background(), []store.SMSMessage{stored}); err != nil {
		t.Fatal("IMS-only deletion blocked", err)
	}
}

func TestMultiSIMGuardsTelegramAndProxyReconnect(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{owned: true}
	bot := &telegramBot{server: s}
	if _, err := bot.executeATCommand(context.Background(), "ec20", "AT"); !errors.Is(err, errMultiSIMActive) {
		t.Fatal("Telegram AT bypass", err)
	}
	if _, err := bot.executeESIMSwitch(context.Background(), telegramPendingAction{DeviceID: "ec20"}); !errors.Is(err, errMultiSIMActive) {
		t.Fatal("Telegram eSIM bypass", err)
	}
	if requested, err := s.requestProfileProxyRouteReconnect("ec20", testProfileICCID); requested || !errors.Is(err, errMultiSIMActive) {
		t.Fatal("proxy single-session reconnect bypass", requested, err)
	}
}

func TestMultiSIMDisablePreservesProfilesAndCleanupCardGuard(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		owned      bool
	}{{"owned omitted", `{"enabled":false}`, true}, {"owned empty", `{"enabled":false,"profiles":[]}`, true}, {"stopped omitted", `{"enabled":false}`, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, _ := newProfileBindingTestServer(t)
			f := &fakeMultiSIMController{keepOwned: tc.owned}
			s.multisim = f
			if w := multiRequest(t, s, "PUT", "/api/devices/ec20/multisim", multiBody); w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			before, err := db.MultiSIMConfig(context.Background(), "ec20")
			if err != nil {
				t.Fatal(err)
			}
			if !tc.owned {
				before.Enabled = false
				if _, err := db.SaveMultiSIMConfig(context.Background(), before); err != nil {
					t.Fatal(err)
				}
				f.owned = false
			}
			if w := multiRequest(t, s, "PUT", "/api/devices/ec20/multisim", tc.body); w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			after, err := db.MultiSIMConfig(context.Background(), "ec20")
			if err != nil || after.Enabled || len(after.Profiles) != len(before.Profiles) {
				t.Fatalf("disable discarded profiles: %+v %v", after, err)
			}
			for i, p := range before.Profiles {
				if after.Profiles[i] != p {
					t.Fatal("changed profile")
				}
				if tc.owned && !s.multiSIMCardOwned(context.Background(), p.ICCID) {
					t.Fatal("cleanup lost card guard")
				}
			}
		})
	}
}

type singleDeviceMultiSIMOwner struct{ fakeMultiSIMController }

func (f *singleDeviceMultiSIMOwner) Owns(id string) bool { return id == "ec20" && f.owned }

func TestMultiSIMRejectsAliasesToOwnedPhysicalDevice(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			s, db, _ := newProfileBindingTestServer(t)
			s.devices = fakeDeviceController{entry: device.Device{ID: "ec20", Discovered: true, Candidate: modem.Candidate{USBPath: "1-1"}}}
			s.multisim = &singleDeviceMultiSIMOwner{fakeMultiSIMController: fakeMultiSIMController{owned: !persisted}}
			if persisted {
				if _, err := db.SaveMultiSIMConfig(context.Background(), store.MultiSIMConfig{DeviceID: "ec20", Enabled: true, Profiles: []store.MultiSIMProfile{{ICCID: "8944100000000000001", AID: "A000"}, {ICCID: "8944100000000000002", AID: "A000"}}}); err != nil {
					t.Fatal(err)
				}
			}
			body := `{"config":{"id":"alias","device_type":"pcie_ec20_ec25","usb_path":"1-1","name":"alias"}}`
			if w := multiRequest(t, s, "POST", "/api/devices", body); w.Code != 409 {
				t.Fatal("alias create accepted", w.Code, w.Body.String())
			}
			if _, err := db.Device(context.Background(), "alias"); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("alias persisted", err)
			}
			if err := db.UpsertDevice(context.Background(), store.Device{ID: "alias", Name: "elsewhere", USBPath: "2-1"}); err != nil {
				t.Fatal(err)
			}
			if w := multiRequest(t, s, "PUT", "/api/devices/alias", body); w.Code != 409 {
				t.Fatal("alias update accepted", w.Code, w.Body.String())
			}
			if got, err := db.Device(context.Background(), "alias"); err != nil || got.USBPath != "2-1" {
				t.Fatal("alias binding changed", got.USBPath, err)
			}
			if w := multiRequest(t, s, "PUT", "/api/devices/alias", `{"config":{"id":"alias","device_type":"pcie_ec20_ec25","usb_path":"2-1","name":"renamed"}}`); w.Code != 200 {
				t.Fatal("unrelated rename blocked", w.Code, w.Body.String())
			}
		})
	}
}
func TestMultiSIMEnableWaitsForBindingTransaction(t *testing.T) {
	s, _, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{}
	unlock, err := s.lockMultiSIMBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		s.routeDeviceAPI(w, httptest.NewRequest("PUT", "/api/devices/ec20/multisim", strings.NewReader(multiBody)))
		done <- w
	}()
	select {
	case w := <-done:
		unlock()
		t.Fatal("enable bypassed binding transaction", w.Code)
	case <-time.After(20 * time.Millisecond):
	}
	if w := multiRequest(t, s, "GET", "/api/devices/ec20/multisim", ""); w.Code != 200 {
		unlock()
		t.Fatal("state blocked")
	}
	unlock()
	select {
	case w := <-done:
		if w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("enable stuck")
	}
}

func TestMultiSIMAcceptsUSBEC20WithLegacyQMIConfigLabels(t *testing.T) {
	s, db, _ := newProfileBindingTestServer(t)
	s.multisim = &fakeMultiSIMController{}
	cfg, err := db.Device(context.Background(), "ec20")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DeviceBackend = "qmi"
	cfg.ESIMTransport = "qmi"
	if err := db.UpsertDevice(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	// Hardware dispatch is checked by runtime Prepare. Config labels alone do
	// not identify native QMI and must not reject this USB EC20 request.
	if w := multiRequest(t, s, "PUT", "/api/devices/ec20/multisim", multiBody); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestMultiSIMAliasWaitsThenRechecksOwnership(t *testing.T) {
	s, db, _ := newProfileBindingTestServer(t)
	s.devices = fakeDeviceController{entry: device.Device{ID: "ec20", Discovered: true, Candidate: modem.Candidate{USBPath: "1-1"}}}
	f := &singleDeviceMultiSIMOwner{}
	s.multisim = f
	unlock, err := s.lockMultiSIMBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		s.routeDeviceAPI(w, httptest.NewRequest("POST", "/api/devices", strings.NewReader(`{"config":{"id":"alias","device_type":"pcie_ec20_ec25","usb_path":"1-1"}}`)))
		done <- w
	}()
	select {
	case w := <-done:
		unlock()
		t.Fatal("alias bypassed bindings gate", w.Code)
	case <-time.After(20 * time.Millisecond):
	}
	f.owned = true
	unlock()
	select {
	case w := <-done:
		if w.Code != 409 {
			t.Fatal("ownership not rechecked", w.Code, w.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("alias create stuck")
	}
	if _, err := db.Device(context.Background(), "alias"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("alias persisted", err)
	}
}

func TestMultiSIMWrongIMEICannotBypassPhysicalAliasGuard(t *testing.T) {
	for _, method := range []string{"POST", "PUT"} {
		t.Run(method, func(t *testing.T) {
			s, db, _ := newProfileBindingTestServer(t)
			s.devices = fakeDeviceController{entry: device.Device{ID: "ec20", Discovered: true, Snapshot: &device.Snapshot{IMEI: "111111111111111"}, Candidate: modem.Candidate{USBPath: "1-1", ATPort: modem.Port{Path: "/dev/ttyUSB2"}}}}
			s.multisim = &singleDeviceMultiSIMOwner{fakeMultiSIMController: fakeMultiSIMController{owned: true}}
			endpoint := "/api/devices"
			if method == "PUT" {
				endpoint += "/alias"
				if err := db.UpsertDevice(context.Background(), store.Device{ID: "alias", Name: "elsewhere", USBPath: "2-1"}); err != nil {
					t.Fatal(err)
				}
			}
			body := `{"config":{"id":"alias","device_type":"pcie_ec20_ec25","usb_path":"1-1","at_port":"/dev/ttyUSB2","modem_imei":"999999999999999"}}`
			if w := multiRequest(t, s, method, endpoint, body); w.Code != 409 {
				t.Fatal("wrong IMEI bypassed physical ownership", w.Code, w.Body.String())
			}
			got, err := db.Device(context.Background(), "alias")
			if method == "POST" && !errors.Is(err, store.ErrNotFound) {
				t.Fatal("alias persisted", err)
			}
			if method == "PUT" && (err != nil || got.USBPath != "2-1") {
				t.Fatal("alias rebound", got.USBPath, err)
			}
		})
	}
}
