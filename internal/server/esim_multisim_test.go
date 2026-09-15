package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"vocat/internal/device"
	"vocat/internal/store"
	"vocat/internal/vowifi/multisim"
)

const (
	ownedICCID1 = "8944100000000000001"
	ownedICCID2 = "8944100000000000002"
	ownedICCID3 = "8944100000000000003"
)

type fakeMultiSIMCards struct {
	multi           *fakeMultiSIMController
	readerErr       error
	readerCalls     int
	deleteICCID     string
	deleteFallback  []multisim.Profile
	appliesAtDelete int
}

func (f *fakeMultiSIMCards) WithOwnedReader(ctx context.Context, _ string, operation func(context.Context, string) error) error {
	f.readerCalls++
	if f.readerErr != nil {
		return f.readerErr
	}
	return operation(ctx, "ec20")
}

func (f *fakeMultiSIMCards) DeleteOwnedProfile(ctx context.Context, _ string, iccid string, fallback []multisim.Profile, deleteProfile func(context.Context, string) error) error {
	f.deleteICCID, f.deleteFallback = iccid, fallback
	f.appliesAtDelete = f.multi.applies
	return deleteProfile(ctx, "ec20")
}

type esimRecordingDevices struct {
	fakeDeviceController
	profiles       device.EsimInfo
	downloadICCID  string
	deleteErr      error
	deleted        []string
	inventoryCalls int
}

func (f *esimRecordingDevices) ESIMInventory(context.Context, string) ([]device.EsimInventoryEntry, error) {
	f.inventoryCalls++
	return []device.EsimInventoryEntry{{Info: f.profiles}}, nil
}

func (f *esimRecordingDevices) ESIMListProfiles(context.Context, string) (device.EsimInfo, error) {
	return f.profiles, nil
}

func (f *esimRecordingDevices) ESIMDownloadProfile(context.Context, string, device.EsimDownloadParams, func(device.EsimProgress)) (*device.EsimDownloadResult, error) {
	f.profiles.Profiles = append(f.profiles.Profiles, device.EsimProfile{ICCID: f.downloadICCID, Name: "Wireless"})
	return &device.EsimDownloadResult{ICCID: f.downloadICCID}, nil
}

func (f *esimRecordingDevices) ESIMDeleteProfile(_ context.Context, _ string, iccid, _ string) (*device.EsimDeleteResult, error) {
	f.deleted = append(f.deleted, iccid)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &device.EsimDeleteResult{}, nil
}

func newOwnedESIMServer(t *testing.T, iccids ...string) (*Server, *store.Store, *fakeMultiSIMController, *fakeMultiSIMCards, *esimRecordingDevices) {
	t.Helper()
	s, database, _ := newProfileBindingTestServer(t)
	multi := &fakeMultiSIMController{owned: true, keepOwned: true}
	cards := &fakeMultiSIMCards{multi: multi}
	devices := &esimRecordingDevices{
		fakeDeviceController: fakeDeviceController{entry: device.Device{ID: "ec20", Discovered: true}},
		profiles:             device.EsimInfo{AID: "A000"},
	}
	cfg := store.MultiSIMConfig{DeviceID: "ec20", Enabled: true}
	for _, iccid := range iccids {
		cfg.Profiles = append(cfg.Profiles, store.MultiSIMProfile{ICCID: iccid, AID: "A000"})
		devices.profiles.Profiles = append(devices.profiles.Profiles, device.EsimProfile{ICCID: iccid})
	}
	if _, err := database.SaveMultiSIMConfig(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	s.multisim, s.multisimCards, s.devices = multi, cards, devices
	return s, database, multi, cards, devices
}

func savedProfileICCIDs(t *testing.T, database *store.Store) []string {
	t.Helper()
	cfg, err := database.MultiSIMConfig(context.Background(), "ec20")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(cfg.Profiles))
	for _, profile := range cfg.Profiles {
		out = append(out, profile.ICCID)
	}
	return out
}

func TestOwnedEC20ESIMInventoryUsesGroupReader(t *testing.T) {
	s, _, _, cards, devices := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	if w := multiRequest(t, s, "GET", "/api/devices/ec20/esim", ""); w.Code != http.StatusOK {
		t.Fatalf("inventory during a running group: %d %s", w.Code, w.Body.String())
	}
	if cards.readerCalls != 1 || devices.inventoryCalls != 1 {
		t.Fatalf("inventory must run once inside the group's reader: reader=%d inventory=%d", cards.readerCalls, devices.inventoryCalls)
	}
	for _, item := range []struct{ method, path string }{{"POST", "esim/actions/switch"}, {"POST", "esim/actions/disable"}, {"POST", "actions/at"}} {
		if w := multiRequest(t, s, item.method, "/api/devices/ec20/"+item.path, `{}`); w.Code != http.StatusConflict {
			t.Fatalf("%s %s must stay guarded while the group runs: %d", item.method, item.path, w.Code)
		}
	}
}

func TestOwnedEC20ESIMReaderUnavailableIsConflict(t *testing.T) {
	s, _, _, cards, _ := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	cards.readerErr = errors.New("multisim: reader is not ready")
	if w := multiRequest(t, s, "GET", "/api/devices/ec20/esim", ""); w.Code != http.StatusConflict {
		t.Fatalf("an unready group reader must answer 409, got %d", w.Code)
	}
}

func TestOwnedEC20DownloadJoinsRunningGroup(t *testing.T) {
	s, database, multi, cards, devices := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	devices.downloadICCID = ownedICCID3
	w := multiRequest(t, s, "GET", "/api/devices/ec20/esim/actions/download?smdp=smdp.example&matching_id=CODE", "")
	body := w.Body.String()
	if !strings.Contains(body, `"step":"done"`) || !strings.Contains(body, `"multisim_added":true`) {
		t.Fatalf("download must finish and report the new line: %s", body)
	}
	if cards.readerCalls != 1 {
		t.Fatalf("download must hold the group's reader, calls=%d", cards.readerCalls)
	}
	if got := savedProfileICCIDs(t, database); len(got) != 3 || got[2] != ownedICCID3 {
		t.Fatalf("saved profiles = %v", got)
	}
	if len(multi.lastConfig.Profiles) != 3 || multi.lastConfig.Profiles[2].AID != "A000" || multi.lastConfig.Profiles[2].Name != "Wireless" {
		t.Fatalf("running group was not extended: %+v", multi.lastConfig)
	}
}

func TestOwnedEC20DeleteDetachesLineBeforeDeletingProfile(t *testing.T) {
	s, database, _, cards, devices := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	w := multiRequest(t, s, "DELETE", "/api/devices/ec20/esim/profiles/"+ownedICCID1, "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete during a running group: %d %s", w.Code, w.Body.String())
	}
	if len(devices.deleted) != 1 || devices.deleted[0] != ownedICCID1 {
		t.Fatalf("deleted = %v", devices.deleted)
	}
	if cards.appliesAtDelete != 1 {
		t.Fatalf("the line must leave the group before the card deletes its profile, applies at delete=%d", cards.appliesAtDelete)
	}
	if len(cards.deleteFallback) != 1 || cards.deleteFallback[0].ICCID != ownedICCID2 {
		t.Fatalf("fallback = %+v", cards.deleteFallback)
	}
	if got := savedProfileICCIDs(t, database); len(got) != 1 || got[0] != ownedICCID2 {
		t.Fatalf("saved profiles = %v", got)
	}
}

func TestOwnedEC20DeleteRefusesLastLine(t *testing.T) {
	s, _, multi, _, devices := newOwnedESIMServer(t, ownedICCID1)
	w := multiRequest(t, s, "DELETE", "/api/devices/ec20/esim/profiles/"+ownedICCID1, "")
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "multisim_last_profile") {
		t.Fatalf("deleting the last line must be refused: %d %s", w.Code, w.Body.String())
	}
	if len(devices.deleted) != 0 || multi.applies != 0 {
		t.Fatalf("nothing may change: deleted=%v applies=%d", devices.deleted, multi.applies)
	}
}

func TestOwnedEC20DeleteFailureRestoresLine(t *testing.T) {
	s, database, multi, _, devices := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	devices.deleteErr = errors.New("card refused")
	if w := multiRequest(t, s, "DELETE", "/api/devices/ec20/esim/profiles/"+ownedICCID1, ""); w.Code == http.StatusOK {
		t.Fatal("a failed deletion must not report success")
	}
	if got := savedProfileICCIDs(t, database); len(got) != 2 {
		t.Fatalf("the detached line must be restored, saved = %v", got)
	}
	if multi.applies != 2 || len(multi.lastConfig.Profiles) != 2 {
		t.Fatalf("running group must be restored: applies=%d config=%+v", multi.applies, multi.lastConfig)
	}
}

func TestOwnedEC20DeleteProfileOutsideGroupKeepsLines(t *testing.T) {
	s, database, multi, cards, devices := newOwnedESIMServer(t, ownedICCID1, ownedICCID2)
	if w := multiRequest(t, s, "DELETE", "/api/devices/ec20/esim/profiles/"+ownedICCID3, ""); w.Code != http.StatusOK {
		t.Fatalf("delete of an unused profile: %d %s", w.Code, w.Body.String())
	}
	if multi.applies != 0 || len(devices.deleted) != 1 || len(cards.deleteFallback) != 2 {
		t.Fatalf("applies=%d deleted=%v fallback=%+v", multi.applies, devices.deleted, cards.deleteFallback)
	}
	if got := savedProfileICCIDs(t, database); len(got) != 2 {
		t.Fatalf("saved profiles = %v", got)
	}
}
