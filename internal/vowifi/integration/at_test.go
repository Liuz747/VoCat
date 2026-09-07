package integration

import (
	"context"
	"testing"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
)

type fakeATDevices struct {
	entries     []device.Device
	executedID  string
	sensitiveID string
}

func (devices *fakeATDevices) Get(id string) (device.Device, error) {
	for _, entry := range devices.entries {
		if entry.ID == id {
			return entry, nil
		}
	}
	return device.Device{}, device.ErrNotFound
}

func (devices *fakeATDevices) List() []device.Device {
	return append([]device.Device(nil), devices.entries...)
}

func (devices *fakeATDevices) ExecuteAT(
	_ context.Context,
	id string,
	_ string,
) (modem.Response, error) {
	devices.executedID = id
	return modem.Response{Final: "OK"}, nil
}

func (devices *fakeATDevices) ExecuteSensitiveAT(
	_ context.Context,
	id string,
	_ string,
) (modem.Response, error) {
	devices.sensitiveID = id
	return modem.Response{Final: "OK"}, nil
}

func TestATMapperResolvesConfiguredIDByStableATPath(t *testing.T) {
	database := testStore(t)
	if err := database.UpsertDevice(context.Background(), store.Device{
		ID:     "living-room",
		Name:   "EC20",
		ATPort: "/dev/serial/by-id/usb-ec20-if02",
	}); err != nil {
		t.Fatal(err)
	}
	devices := &fakeATDevices{entries: []device.Device{{
		ID:         "usb-1-2",
		Discovered: true,
		Candidate: modem.Candidate{
			ATPort: modem.Port{
				Path:       "/dev/ttyUSB2",
				StablePath: "/dev/serial/by-id/usb-ec20-if02",
			},
		},
	}}}
	mapper := ATMapper{Store: database, Devices: devices}
	if _, err := mapper.ExecuteAT(
		context.Background(),
		"living-room",
		"AT",
	); err != nil {
		t.Fatal(err)
	}
	if devices.executedID != "usb-1-2" {
		t.Fatalf("ExecuteAT physical ID = %q", devices.executedID)
	}
	if _, err := mapper.ExecuteSensitiveAT(
		context.Background(),
		"living-room",
		"AT+CSIM=1",
	); err != nil {
		t.Fatal(err)
	}
	if devices.sensitiveID != "usb-1-2" {
		t.Fatalf("ExecuteSensitiveAT physical ID = %q", devices.sensitiveID)
	}
}

// A ttyUSB/cdc-wdm pair is recycled by the kernel on every re-enumeration, so
// a coincidental node-name match must never outvote the USB bus position: two
// different positions are two different modems (DEVICE-REPORT §24, §25.15).
func TestATMapperTrustsUSBTopologyOverRecycledNodeNames(t *testing.T) {
	database := testStore(t)
	if err := database.UpsertDevice(context.Background(), store.Device{
		ID:            "ec20_1",
		Name:          "EC20 1",
		ATPort:        "/dev/ttyUSB2",
		ControlDevice: "/dev/cdc-wdm0",
		// Simulate metadata left from a formerly swapped hub mapping.
		USBPath:   "/sys/bus/usb/devices/1-6",
		ModemIMEI: "111111111111111",
	}); err != nil {
		t.Fatal(err)
	}
	devices := &fakeATDevices{entries: []device.Device{
		{
			ID:         "quectel-0125-1-6",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/1-6",
				QMIControl: "/dev/cdc-wdm1",
				ATPort:     modem.Port{Path: "/dev/ttyUSB6"},
			},
		},
		{
			ID:         "quectel-0306-1-5",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/1-5",
				QMIControl: "/dev/cdc-wdm0",
				ATPort:     modem.Port{Path: "/dev/ttyUSB2"},
			},
		},
	}}
	mapper := ATMapper{Store: database, Devices: devices}
	if _, err := mapper.ExecuteAT(context.Background(), "ec20_1", "AT+CIMI"); err != nil {
		t.Fatal(err)
	}
	if devices.executedID != "quectel-0125-1-6" {
		t.Fatalf("ExecuteAT physical ID = %q, want the modem at the configured USB position", devices.executedID)
	}
}

func TestATMapperPrefersLiveIMEIOverAllStalePaths(t *testing.T) {
	database := testStore(t)
	if err := database.UpsertDevice(context.Background(), store.Device{
		ID:            "ec20_1",
		Name:          "EC20 1",
		ATPort:        "/dev/ttyUSB2",
		ControlDevice: "/dev/cdc-wdm0",
		USBPath:       "/sys/bus/usb/devices/1-5",
		ModemIMEI:     "222222222222222",
	}); err != nil {
		t.Fatal(err)
	}
	devices := &fakeATDevices{entries: []device.Device{
		{
			ID:         "old-paths",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/1-5",
				QMIControl: "/dev/cdc-wdm0",
				ATPort:     modem.Port{Path: "/dev/ttyUSB2"},
			},
			Snapshot: &device.Snapshot{IMEI: "111111111111111"},
		},
		{
			ID:         "live-imei",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/2-3",
				QMIControl: "/dev/cdc-wdm4",
				ATPort:     modem.Port{Path: "/dev/ttyUSB10"},
			},
			Snapshot: &device.Snapshot{IMEI: "222222222222222"},
		},
	}}
	mapper := ATMapper{Store: database, Devices: devices}
	if _, err := mapper.ExecuteAT(context.Background(), "ec20_1", "AT+CIMI"); err != nil {
		t.Fatal(err)
	}
	if devices.executedID != "live-imei" {
		t.Fatalf("ExecuteAT physical ID = %q, want live IMEI candidate", devices.executedID)
	}
}

// Regression (production, 2026-09-05): device 1-1-3-1 kept its 09-03 node names
// (/dev/ttyUSB2, /dev/cdc-wdm1). After a hub reset those names were handed to
// the modem at 1-1.3.4.2, so the VoWiFi runtime of 1-1-3-1 bound to that modem
// and both logical devices registered the same IMPU with one instance-id.
func TestATMapperIgnoresNodeNamesInheritedByAnotherModem(t *testing.T) {
	database := testStore(t)
	if err := database.UpsertDevice(context.Background(), store.Device{
		ID:            "line-3-1",
		Name:          "EC20 3-1",
		ATPort:        "/dev/ttyUSB2",
		ControlDevice: "/dev/cdc-wdm1",
		USBPath:       "/sys/bus/usb/devices/1-1.3.1",
	}); err != nil {
		t.Fatal(err)
	}
	devices := &fakeATDevices{entries: []device.Device{
		{
			ID:         "rotating-modem",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/1-1.3.4.2",
				QMIControl: "/dev/cdc-wdm1",
				ATPort:     modem.Port{Path: "/dev/ttyUSB2"},
			},
		},
		{
			ID:         "own-modem",
			Discovered: true,
			Candidate: modem.Candidate{
				USBPath:    "/sys/bus/usb/devices/1-1.3.1",
				QMIControl: "/dev/cdc-wdm3",
				ATPort:     modem.Port{Path: "/dev/ttyUSB10"},
			},
		},
	}}
	mapper := ATMapper{Store: database, Devices: devices}
	if _, err := mapper.ExecuteAT(context.Background(), "line-3-1", "AT+CIMI"); err != nil {
		t.Fatal(err)
	}
	if devices.executedID != "own-modem" {
		t.Fatalf("ExecuteAT physical ID = %q, want the modem at the configured USB position", devices.executedID)
	}
}

type transactionATDevices struct {
	fakeATDevices
	lockedID string
	released bool
}

func (devices *transactionATDevices) BeginUICCTransaction(ctx context.Context, id string) (context.Context, func(), error) {
	devices.lockedID = id
	return ctx, func() { devices.released = true }, nil
}
func TestATMapperPinsPhysicalReaderForWholeTransaction(t *testing.T) {
	database := testStore(t)
	config := store.Device{ID: "line", Name: "line", ATPort: "/dev/stable-reader"}
	if err := database.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	devices := &transactionATDevices{fakeATDevices: fakeATDevices{entries: []device.Device{{ID: "reader-a", Discovered: true, Candidate: modem.Candidate{ATPort: modem.Port{Path: "/dev/stable-reader"}}}}}}
	mapper := ATMapper{Store: database, Devices: devices}
	transactions, ok := any(mapper).(interface {
		BeginUICCTransaction(context.Context, string) (context.Context, func(), error)
	})
	if !ok {
		t.Fatal("mapper has no keyed transaction boundary")
	}
	ctx, release, err := transactions.BeginUICCTransaction(context.Background(), "line")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// A concurrent discovery/configuration change must not redirect an APDU to
	// another physical reader after reader-a's lock has been acquired.
	devices.entries = []device.Device{{ID: "reader-b", Discovered: true, Candidate: modem.Candidate{ATPort: modem.Port{Path: "/dev/stable-reader"}}}}
	if _, err := mapper.ExecuteAT(ctx, "line", "AT+CCID"); err != nil {
		t.Fatal(err)
	}
	if _, err := mapper.ExecuteSensitiveAT(ctx, "line", "AT+CSIM"); err != nil {
		t.Fatal(err)
	}
	if devices.lockedID != "reader-a" || devices.executedID != devices.lockedID || devices.sensitiveID != devices.lockedID {
		t.Fatalf("transaction changed readers: lock=%s AT=%s sensitive=%s", devices.lockedID, devices.executedID, devices.sensitiveID)
	}
}
