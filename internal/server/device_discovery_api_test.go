package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
)

type discoverySnapshotController struct {
	fakeDeviceController
	entries       []device.Device
	discoverCalls int
}

type addRecoveryController struct {
	fakeDeviceController
	flightErr error
}

func (controller *addRecoveryController) SetFlight(context.Context, string, bool) (device.FlightResult, error) {
	return device.FlightResult{}, controller.flightErr
}

func (controller *discoverySnapshotController) Discover(context.Context) ([]device.Device, error) {
	controller.discoverCalls++
	return append([]device.Device(nil), controller.entries...), nil
}

func TestDiscoveredDevicesPerformsFreshScanAndOmitsAbsentEntries(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	controller := &discoverySnapshotController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: "stale-device", Discovered: false,
			Candidate: modem.Candidate{ID: "stale-device", USBPath: "1-1"},
		}},
		entries: []device.Device{
			{
				ID: "current-device", Discovered: true,
				Candidate: modem.Candidate{ID: "current-device", USBPath: "2-1"},
			},
			{
				ID: "absent-device", Discovered: false,
				Candidate: modem.Candidate{ID: "absent-device", USBPath: "3-1"},
			},
		},
	}
	server := &Server{
		store: database, logger: regionTestLogger(),
		maxRequestBodyBytes: 4096, devices: controller,
	}
	request := httptest.NewRequest(http.MethodGet, "/api/devices/discovered", nil)
	recorder := httptest.NewRecorder()

	if !server.handleDiscoveredDevices(recorder, request) {
		t.Fatal("handleDiscoveredDevices returned false")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if controller.discoverCalls != 1 {
		t.Fatalf("Discover calls = %d, want 1", controller.discoverCalls)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "current-device") {
		t.Fatalf("response omits current device: %s", body)
	}
	if strings.Contains(body, "stale-device") || strings.Contains(body, "absent-device") {
		t.Fatalf("response contains an absent device: %s", body)
	}
}

func TestAddDevicePersistsConfigWhenInitialAirplaneModeIsTemporarilyUnavailable(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	entry := device.Device{
		ID: "usb-2c7c-0125-2-2", Discovered: true,
		Candidate: modem.Candidate{
			ID: "usb-2c7c-0125-2-2", VendorID: "2c7c", ProductID: "0125",
			Product: "Quectel EC20 / EC25", USBPath: "/sys/bus/usb/devices/2-2",
			ATPort:     modem.Port{Path: "/dev/ttyUSB2", Name: "ttyUSB2", Role: modem.PortRoleAT},
			QMIControl: "/dev/cdc-wdm0", NetworkInterface: "wwan0",
		},
	}
	controller := &addRecoveryController{
		fakeDeviceController: fakeDeviceController{entry: entry},
		flightErr:            errors.New("AT port is temporarily busy"),
	}
	server := &Server{
		store: database, logger: regionTestLogger(),
		maxRequestBodyBytes: 4096, devices: controller,
	}
	body := `{"config":{"id":"ec20","name":"EC20","device_type":"pcie_ec20_ec25","usb_path":"/sys/bus/usb/devices/2-2","at_port":"/dev/ttyUSB2","control_device":"/dev/cdc-wdm0","interface":"wwan0","device_backend":"qmi","esim_transport":"qmi"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/devices", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	if !server.handleDevices(recorder, request) {
		t.Fatal("handleDevices returned false")
	}
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "saved") || !strings.Contains(recorder.Body.String(), "retry") {
		t.Fatalf("response lacks recovery warning: %s", recorder.Body.String())
	}
	config, err := database.Device(context.Background(), "ec20")
	if err != nil {
		t.Fatalf("saved device missing: %v", err)
	}
	if config.NetworkEnabled || !config.VoWiFiEnabled || config.USBPath != entry.Candidate.USBPath {
		t.Fatalf("saved safe config = %+v", config)
	}
}

// The default device quota is 0 = unlimited. Adding a module must never be
// refused with device_limit_reached unless an administrator set a positive
// quota; the fleet size is bounded by USB topology, not by software.
func TestHandleDevicesAddIsNotRefusedWhenQuotaIsUnlimited(t *testing.T) {
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("existing-%d", i)
		if err := database.UpsertDevice(context.Background(), store.Device{ID: id, Name: id}); err != nil {
			t.Fatalf("seed device %s: %v", id, err)
		}
	}
	entry := device.Device{
		ID: "usb-2c7c-0125-2-2", Discovered: true,
		Candidate: modem.Candidate{
			ID: "usb-2c7c-0125-2-2", VendorID: "2c7c", ProductID: "0125",
			Product: "Quectel EC20 / EC25", USBPath: "/sys/bus/usb/devices/2-2",
			ATPort:     modem.Port{Path: "/dev/ttyUSB2", Name: "ttyUSB2", Role: modem.PortRoleAT},
			QMIControl: "/dev/cdc-wdm0", NetworkInterface: "wwan0",
		},
	}
	server := &Server{
		store: database, logger: regionTestLogger(),
		maxRequestBodyBytes: 4096, devices: fakeDeviceController{entry: entry},
	}
	body := `{"config":{"id":"ec20-41","name":"EC20","device_type":"pcie_ec20_ec25","usb_path":"/sys/bus/usb/devices/2-2","at_port":"/dev/ttyUSB2","control_device":"/dev/cdc-wdm0","interface":"wwan0","device_backend":"qmi","esim_transport":"qmi"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/devices", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	if !server.handleDevices(recorder, request) {
		t.Fatal("handleDevices returned false")
	}
	if strings.Contains(recorder.Body.String(), "device_limit_reached") {
		t.Fatalf("41st device refused by quota although none is set: %s", recorder.Body.String())
	}
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}
