package store

import (
	"context"
	"testing"
	"time"
)

func TestSMSSubscriptionIdentityIsImmutableAcrossRescans(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, ":memory:")
	mustSaveDevice(t, database, "ec20-1", "EC20")
	if err := database.UpsertDeviceRuntime(ctx, DeviceRuntime{
		DeviceID: "ec20-1", ICCID: "iccid-b", IMSI: "imsi-b", PhoneNumber: "+442222",
	}); err != nil {
		t.Fatal(err)
	}

	original, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: "modem:ME:7:stable", DeviceID: "ec20-1", ModemIMEI: "867394042309830",
		ICCID: "iccid-a", IMSI: "imsi-a", LocalPhone: "+441111",
		Peer: "JETPAC", Direction: "inbound", Body: "profile A",
		Timestamp: time.Unix(1_700_000_000, 0).UTC(), Source: "cellular_at",
	})
	if err != nil {
		t.Fatal(err)
	}
	rescanned, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: original.MessageID, DeviceID: "ec20-1", ModemIMEI: original.ModemIMEI,
		ICCID: "iccid-b", IMSI: "imsi-b", LocalPhone: "+442222",
		Peer: "JETPAC", Direction: "inbound", Body: "profile A",
		Timestamp: original.Timestamp, Source: "cellular_at",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rescanned.ID != original.ID || rescanned.ICCID != "iccid-a" ||
		rescanned.IMSI != "imsi-a" || rescanned.LocalPhone != "+441111" {
		t.Fatalf("rescanned identity = %#v, want immutable profile A identity", rescanned)
	}

	if _, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: "modem:ME:8:new", DeviceID: "ec20-1", ModemIMEI: original.ModemIMEI,
		ICCID: "iccid-b", IMSI: "imsi-b", LocalPhone: "+442222",
		Peer: "JETPAC", Direction: "inbound", Body: "profile B",
		Timestamp: original.Timestamp.Add(time.Minute), Source: "cellular_at",
	}); err != nil {
		t.Fatal(err)
	}
	contacts, err := database.ListSMSContacts(ctx, SMSFilter{ModemIMEI: original.ModemIMEI})
	if err != nil {
		t.Fatal(err)
	}
	if len(contacts) != 2 {
		t.Fatalf("contacts = %#v, want one thread per ICCID", contacts)
	}
	phones := map[string]string{}
	for _, contact := range contacts {
		phones[contact.ICCID] = contact.LocalPhone
	}
	if phones["iccid-a"] != "+441111" || phones["iccid-b"] != "+442222" {
		t.Fatalf("contact phone snapshots = %#v", phones)
	}
	filtered, err := database.ListSMSMessages(ctx, SMSFilter{ICCID: "iccid-a", Peer: "JETPAC"})
	if err != nil || len(filtered) != 1 || filtered[0].Body != "profile A" {
		t.Fatalf("ICCID-filtered messages = %#v, %v", filtered, err)
	}
}

func TestLegacySMSDoesNotAcquireMismatchedCurrentICCID(t *testing.T) {
	ctx := context.Background()
	database := openTestStore(t, ":memory:")
	mustSaveDevice(t, database, "ec20-1", "EC20")
	legacy, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: "legacy", DeviceID: "ec20-1",
		IMSI: "imsi-a", Peer: "JETPAC", Direction: "inbound", Body: "legacy",
	})
	if err != nil {
		t.Fatal(err)
	}
	rescanned, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: legacy.MessageID, DeviceID: legacy.DeviceID, ModemIMEI: "867394042309830",
		ICCID: "iccid-b", IMSI: "imsi-a", LocalPhone: "+442222",
		Peer: legacy.Peer, Direction: legacy.Direction, Body: legacy.Body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rescanned.ICCID != "" || rescanned.IMSI != "imsi-a" || rescanned.LocalPhone != "" {
		t.Fatalf("legacy message acquired mismatched identity: %#v", rescanned)
	}
	unknown, err := database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: "legacy-unknown", DeviceID: "ec20-1",
		Peer: "JETPAC", Direction: "inbound", Body: "unknown",
	})
	if err != nil {
		t.Fatal(err)
	}
	unknown, err = database.SaveSMSMessage(ctx, SMSMessage{
		MessageID: unknown.MessageID, DeviceID: unknown.DeviceID, ModemIMEI: "867394042309830",
		ICCID: "iccid-b", IMSI: "imsi-b", LocalPhone: "+442222",
		Peer: unknown.Peer, Direction: unknown.Direction, Body: unknown.Body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unknown.ICCID != "" || unknown.IMSI != "" || unknown.LocalPhone != "" {
		t.Fatalf("unidentified legacy message acquired current identity: %#v", unknown)
	}
}
