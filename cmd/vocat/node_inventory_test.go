package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
	"vocat/internal/vowifi/integration"
)

type inventoryDevicesFake struct {
	integration.ATDeviceController
	entries []device.Device
}

func (f *inventoryDevicesFake) List() []device.Device {
	return append([]device.Device(nil), f.entries...)
}
func (f *inventoryDevicesFake) Get(id string) (device.Device, error) {
	for _, d := range f.entries {
		if d.ID == id {
			return d, nil
		}
	}
	return device.Device{}, device.ErrNotFound
}
func inventoryFixture(t *testing.T, n int) (*nodeActionService, *inventoryDevicesFake) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fake := &inventoryDevicesFake{}
	for i := 0; i < n; i++ {
		imei := fmt.Sprintf("86000000000%04d", i)
		if err = db.UpsertDevice(ctx, store.Device{ID: imei, Name: imei, ModemIMEI: imei}); err != nil {
			t.Fatal(err)
		}
		fake.entries = append(fake.entries, device.Device{ID: fmt.Sprintf("physical-%d", i), Discovered: true, Candidate: modem.Candidate{VendorID: "2c7c", ProductID: "0125"}, Snapshot: &device.Snapshot{IMEI: imei}, LastUpdated: time.Now()})
	}
	service := &nodeActionService{database: db, mapper: integration.ATMapper{Store: db, Devices: fake}}
	return service, fake
}
func inventoryCall(t *testing.T, s *nodeActionService, size int, cursor string) map[string]any {
	t.Helper()
	params, _ := json.Marshal(map[string]any{"page_size": size, "cursor": cursor})
	r, e := s.getInventory(context.Background(), nodemqtt.Command{Params: params})
	if e != nil {
		t.Fatalf("inventory: %+v", e)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
func TestInventoryListsTwentyEmptyModemsWithoutHistoricalAliases(t *testing.T) {
	s, fake := inventoryFixture(t, 20)
	ctx := context.Background()
	for _, cfg := range []store.Device{{ID: "old-alias", Name: "alias", ModemIMEI: fake.entries[0].Snapshot.IMEI}, {ID: "removed", Name: "removed", ModemIMEI: "869999999999999"}} {
		if err := s.database.UpsertDevice(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	r := inventoryCall(t, s, 100, "")
	items := r["items"].([]any)
	if len(items) != 20 {
		t.Fatalf("got %d items, want 20 physical EC20s including blank cards", len(items))
	}
	seen := map[string]bool{}
	for _, v := range items {
		row := v.(map[string]any)
		imei := row["slot"].(string)
		if seen[imei] {
			t.Fatalf("duplicate IMEI %s", imei)
		}
		seen[imei] = true
		if row["device"] != imei {
			t.Fatalf("alias chosen: %v", row)
		}
		if row["profiles_complete"] != false {
			t.Fatal("saved group is not a complete card inventory")
		}
	}
}
func TestInventoryPaginationKeepsPhysicalSnapshot(t *testing.T) {
	s, fake := inventoryFixture(t, 20)
	first := inventoryCall(t, s, 7, "")
	cursor := first["cursor"].(string)
	fake.entries = nil
	// Changing live membership or the database ordering must not shift later pages.
	if err := s.database.UpsertDevice(context.Background(), store.Device{ID: "changed", Name: "000-first", ModemIMEI: "869999999999999"}); err != nil {
		t.Fatal(err)
	}
	all := append([]any(nil), first["items"].([]any)...)
	for cursor != "" {
		r := inventoryCall(t, s, 7, cursor)
		if r["generated_at"] != first["generated_at"] {
			t.Fatal("pagination changed observation time")
		}
		all = append(all, r["items"].([]any)...)
		cursor, _ = r["cursor"].(string)
	}
	seen := map[string]bool{}
	for _, v := range all {
		imei := v.(map[string]any)["slot"].(string)
		if seen[imei] {
			t.Fatalf("duplicate %s", imei)
		}
		seen[imei] = true
	}
	if len(seen) != 20 {
		t.Fatalf("got %d distinct modems", len(seen))
	}
}

func TestInventoryIncludesUnconfiguredModemAndIgnoresVirtualReader(t *testing.T) {
	s, fake := inventoryFixture(t, 1)
	fake.entries = append(fake.entries, device.Device{ID: "unconfigured", Discovered: true, Candidate: modem.Candidate{VendorID: "2c7c", ProductID: "0125"}, Snapshot: &device.Snapshot{IMEI: "860000000009999"}}, device.Device{ID: "pcsc-alias", Discovered: true, Candidate: modem.Candidate{VendorID: "2c7c", HardwareKind: "pcsc"}, Snapshot: &device.Snapshot{IMEI: fake.entries[0].Snapshot.IMEI}})
	r := inventoryCall(t, s, 100, "")
	items := r["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected two physical modems, got %d", len(items))
	}
	last := items[1].(map[string]any)
	if last["slot"] != "860000000009999" || last["state"] != "pending" {
		t.Fatalf("unconfigured module lost: %v", last)
	}
}
func TestInventoryRejectsIncompleteOrAmbiguousIMEI(t *testing.T) {
	for _, mode := range []string{"missing", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			s, fake := inventoryFixture(t, 2)
			if mode == "missing" {
				fake.entries[1].Snapshot = nil
			} else {
				fake.entries[1].Snapshot.IMEI = fake.entries[0].Snapshot.IMEI
			}
			r, e := s.getInventory(context.Background(), nodemqtt.Command{Params: json.RawMessage(`{}`)})
			if r != nil || e == nil || e.Code != "IDENTITY_PENDING" {
				t.Fatalf("incomplete inventory reported as success: %v %+v", r, e)
			}
		})
	}
}
func TestInventoryCursorRejectsChangedPageSizeAndExpiry(t *testing.T) {
	s, _ := inventoryFixture(t, 3)
	first := inventoryCall(t, s, 1, "")
	cursor := first["cursor"].(string)
	params, _ := json.Marshal(map[string]any{"page_size": 2, "cursor": cursor})
	_, e := s.getInventory(context.Background(), nodemqtt.Command{Params: params})
	if e == nil || e.Code != "CURSOR_EXPIRED" {
		t.Fatalf("changed page size accepted: %+v", e)
	}
	for key, snap := range s.inventorySnapshots {
		snap.expiresAt = time.Now().Add(-time.Second)
		s.inventorySnapshots[key] = snap
	}
	params, _ = json.Marshal(map[string]any{"page_size": 1, "cursor": cursor})
	_, e = s.getInventory(context.Background(), nodemqtt.Command{Params: params})
	if e == nil || e.Code != "CURSOR_EXPIRED" {
		t.Fatalf("expired cursor accepted: %+v", e)
	}
}
