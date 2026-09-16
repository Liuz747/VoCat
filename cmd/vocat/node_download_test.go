package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
)

const testDownloadICCID = "8901240527191882177"

type nodeDownloadController struct {
	nodeESIMController
	t         *testing.T
	locked    *bool
	info      device.EsimInfo
	listErr   error
	downloads int
	reads     int
}

func (f *nodeDownloadController) ESIMDownloadProfile(_ context.Context, id string, _ device.EsimDownloadParams, _ func(device.EsimProgress)) (*device.EsimDownloadResult, error) {
	if !*f.locked || id != "physical" {
		f.t.Fatal("download did not use owned physical reader")
	}
	f.downloads++
	return &device.EsimDownloadResult{ICCID: testDownloadICCID + "F"}, nil
}
func (f *nodeDownloadController) ESIMListProfiles(_ context.Context, id string) (device.EsimInfo, error) {
	if !*f.locked || id != "physical" {
		f.t.Fatal("readback did not use owned physical reader")
	}
	f.reads++
	return f.info, f.listErr
}
func TestNodeDownloadPreservesGroupAndRequiresReadback(t *testing.T) {
	for _, mode := range []string{"installed", "read-error", "missing", "unknown-state"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
				t.Fatal(err)
			}
			initial := store.MultiSIMConfig{DeviceID: "record", Enabled: true, Profiles: []store.MultiSIMProfile{{ICCID: "8900000000000000001", AID: "A000"}}}
			if _, err = db.SaveMultiSIMConfig(ctx, initial); err != nil {
				t.Fatal(err)
			}
			locked := false
			transactions := 0
			f := &nodeDownloadController{t: t, locked: &locked, info: device.EsimInfo{Profiles: []device.EsimProfile{{ICCID: testDownloadICCID, AID: "A001", StateText: "disabled"}}}}
			if mode == "read-error" {
				f.listErr = errors.New("reader failed")
			}
			if mode == "missing" {
				f.info.Profiles = nil
			}
			if mode == "unknown-state" {
				f.info.Profiles[0].State = 7
				f.info.Profiles[0].StateText = ""
			}
			service := &nodeActionService{database: db, esim: f, readCard: func(ctx context.Context, _ string, op func(context.Context, string) error) error {
				transactions++
				locked = true
				defer func() { locked = false }()
				return op(ctx, "physical")
			}}
			params, _ := json.Marshal(map[string]string{"activation_code": "LPA:1$example.test$test"})
			result, actionErr := service.downloadProfile(ctx, nodemqtt.Command{Target: &nodemqtt.Target{Device: "record", Slot: "123456789012345", BindingVersion: 1}, Params: params}, func(string, *int) {})
			if mode == "read-error" || mode == "missing" {
				if actionErr == nil || actionErr.State != "uncertain" {
					t.Fatalf("unconfirmed write: result=%v error=%+v", result, actionErr)
				}
			} else {
				if actionErr != nil {
					t.Fatalf("download error: %+v", actionErr)
				}
				want := "disabled"
				if mode == "unknown-state" {
					want = "unknown"
				}
				if result.(map[string]any)["profile_state"] != want {
					t.Fatalf("state=%v", result)
				}
			}
			saved, err := db.MultiSIMConfig(ctx, "record")
			if err != nil {
				t.Fatal(err)
			}
			if len(saved.Profiles) != 1 || saved.Profiles[0].ICCID != initial.Profiles[0].ICCID {
				t.Fatalf("download changed running group: %+v", saved)
			}
			if transactions != 1 || f.downloads != 1 || f.reads != 1 {
				t.Fatalf("transactions=%d downloads=%d reads=%d", transactions, f.downloads, f.reads)
			}
		})
	}
}

func TestNodeExplicitTargetFindsWrittenProfileOutsideGroup(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.UpsertDevice(ctx, store.Device{ID: "record", Name: "test", ModemIMEI: "123456789012345"}); err != nil {
		t.Fatal(err)
	}
	locked := false
	f := &nodeDownloadController{t: t, locked: &locked, info: device.EsimInfo{Profiles: []device.EsimProfile{{ICCID: testDownloadICCID, AID: "A001"}}}}
	service := &nodeActionService{database: db, esim: f, phoneCache: phoneCollection{generatedAt: time.Now()}, readCard: func(ctx context.Context, _ string, op func(context.Context, string) error) error {
		locked = true
		defer func() { locked = false }()
		return op(ctx, "physical")
	}}
	record, _, failure := service.resolveCommandTarget(ctx, nodemqtt.Target{Device: "record", Slot: "123456789012345", ICCID: testDownloadICCID, BindingVersion: 1})
	if failure != nil || record.Target.ICCID != testDownloadICCID || record.AID != "A001" {
		t.Fatalf("record=%+v error=%+v", record, failure)
	}
	if _, err = db.MultiSIMConfig(ctx, "record"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("target lookup modified config: %v", err)
	}
}
