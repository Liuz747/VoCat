package server

import (
	"context"
	"testing"

	"vocat/internal/device"
	"vocat/internal/store"
)

// A configured device whose stored record predates a working IMEI read matches
// only by USB path and node names. Once the modem reports its IMEI, persist it:
// the IMEI is the one identifier that survives re-enumeration and a move to a
// different USB port, and it is what keeps a record from ever binding to
// another modem (DEVICE-REPORT §24, §25.15).
func TestBackfillModemIMEIRecordsTheLiveIdentityOnce(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, logger: regionTestLogger()}
	config, err := database.Device(ctx, "ec20")
	if err != nil {
		t.Fatal(err)
	}

	server.backfillModemIMEI(ctx, config, device.Device{Snapshot: &device.Snapshot{IMEI: "862107048113898"}})
	stored, err := database.Device(ctx, "ec20")
	if err != nil {
		t.Fatal(err)
	}
	if stored.ModemIMEI != "862107048113898" {
		t.Fatalf("stored IMEI = %q, want the live one", stored.ModemIMEI)
	}

	// A different modem answering later must not overwrite the recorded identity.
	server.backfillModemIMEI(ctx, stored, device.Device{Snapshot: &device.Snapshot{IMEI: "867732035510379"}})
	stored, err = database.Device(ctx, "ec20")
	if err != nil {
		t.Fatal(err)
	}
	if stored.ModemIMEI != "862107048113898" {
		t.Fatalf("stored IMEI = %q, want it left alone once recorded", stored.ModemIMEI)
	}

	// Nothing to record yet: no snapshot, or a snapshot without an IMEI.
	if err := database.UpsertDevice(ctx, store.Device{ID: "reader", Name: "Reader"}); err != nil {
		t.Fatal(err)
	}
	blank, err := database.Device(ctx, "reader")
	if err != nil {
		t.Fatal(err)
	}
	server.backfillModemIMEI(ctx, blank, device.Device{})
	server.backfillModemIMEI(ctx, blank, device.Device{Snapshot: &device.Snapshot{}})
	stored, err = database.Device(ctx, "reader")
	if err != nil {
		t.Fatal(err)
	}
	if stored.ModemIMEI != "" {
		t.Fatalf("stored IMEI = %q, want empty when the modem reports none", stored.ModemIMEI)
	}
}
