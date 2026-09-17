package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
)

type phoneCardReaderFake struct {
	nodeESIMController
	cards     map[string]device.EsimInfo
	reads     int
	err       error
	errAt     string
	afterRead func(string)
	locked    *bool
	t         *testing.T
}

func (f *phoneCardReaderFake) ESIMListProfilesFresh(_ context.Context, id string) (device.EsimInfo, error) {
	if !*f.locked {
		f.t.Fatal("card read outside reader transaction")
	}
	f.reads++
	if f.afterRead != nil {
		f.afterRead(id)
	}
	if f.errAt == "" || f.errAt == id {
		return f.cards[id], f.err
	}
	return f.cards[id], nil
}
func phoneHardwareFixture(t *testing.T, n int) (*nodeActionService, *inventoryDevicesFake, *phoneCardReaderFake) {
	s, devices := inventoryFixture(t, n)
	locked := false
	reader := &phoneCardReaderFake{cards: map[string]device.EsimInfo{}, locked: &locked, t: t}
	s.esim = reader
	s.snapshots = make(map[string]phoneSnapshot)
	s.readCard = func(ctx context.Context, id string, op func(context.Context, string) error) error {
		locked = true
		defer func() { locked = false }()
		for _, d := range devices.entries {
			if d.Snapshot != nil && d.Snapshot.IMEI == id {
				return op(ctx, d.ID)
			}
		}
		return errors.New("device not present")
	}
	for i, d := range devices.entries {
		reader.cards[d.ID] = device.EsimInfo{AID: "A0000005591010", Profiles: []device.EsimProfile{{ICCID: fmt.Sprintf("890000000000000%04d", i), StateText: "disabled"}}}
	}
	return s, devices, reader
}
func callPhones(t *testing.T, s *nodeActionService, size int, cursor string) map[string]any {
	return callPhonesWithEmptySlots(t, s, size, cursor)
}
func callPhonesWithEmptySlots(t *testing.T, s *nodeActionService, size int, cursor string, includeEmpty ...bool) map[string]any {
	t.Helper()
	params := map[string]any{"page_size": size, "cursor": cursor}
	if len(includeEmpty) > 0 {
		params["include_empty_slots"] = includeEmpty[0]
	}
	p, _ := json.Marshal(params)
	r, e := s.listPhones(context.Background(), nodemqtt.Command{Params: p})
	if e != nil {
		t.Fatalf("phones: %+v", e)
	}
	wire, _ := json.Marshal(r)
	var out map[string]any
	if err := json.Unmarshal(wire, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestPhonesListReadsProfilesWithoutSavedGroupAndKeepsWireShape(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 3)
	f.cards[d.entries[2].ID] = device.EsimInfo{AID: "A0000005591010"} // confirmed blank card
	// A saved group is stale and must not replace the actual card profile.
	_, err := s.database.SaveMultiSIMConfig(context.Background(), store.MultiSIMConfig{DeviceID: d.entries[0].Snapshot.IMEI, Profiles: []store.MultiSIMProfile{{ICCID: "8999999999999999999", AID: "A0000005591010"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.database.UpsertDevice(context.Background(), store.Device{ID: "removed", Name: "removed", ModemIMEI: "869999999999999"}); err != nil {
		t.Fatal(err)
	}
	result := callPhonesWithEmptySlots(t, s, 50, "", true)
	if len(result) != 5 {
		t.Fatalf("changed result shape: %v", result)
	}
	items := result["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("got %d phone rows; want two profiles plus one explicit empty-card row", len(items))
	}
	if f.reads != 3 {
		t.Fatalf("read %d cards, want 3", f.reads)
	}
	for i, item := range items {
		row := item.(map[string]any)
		target := row["target"].(map[string]any)
		wantICCID := ""
		if profiles := f.cards[d.entries[i].ID].Profiles; len(profiles) > 0 {
			wantICCID = profiles[0].ICCID
		}
		if target["slot"] != d.entries[i].Snapshot.IMEI || target["iccid"] != wantICCID {
			t.Fatalf("missing module/profile identity: %v", target)
		}
		if i == 2 && row["reason"] != "空卡，无 Profile" {
			t.Fatalf("empty card not explicit: %v", row)
		}
		if len(row) != 5 || len(target) != 5 || target["phone"] != nil || row["available"] != false || row["tunnel_state"] != "stopped" {
			t.Fatalf("unexpected shape or readiness: %v", row)
		}
	}
}
func TestPhonesListNewQueriesReadAgainButPagesKeepSnapshot(t *testing.T) {
	s, _, f := phoneHardwareFixture(t, 3)
	first := callPhones(t, s, 1, "")
	if f.reads != 3 {
		t.Fatalf("reads=%d", f.reads)
	}
	old := first["generated_at"]
	f.err = errors.New("reader now unavailable")
	cursor := first["cursor"].(string)
	for cursor != "" {
		r := callPhones(t, s, 1, cursor)
		if r["generated_at"] != old {
			t.Fatal("snapshot changed")
		}
		cursor, _ = r["cursor"].(string)
	}
	if f.reads != 3 {
		t.Fatal("continuation reread cards")
	}
	s.phoneCache = phoneCollection{generatedAt: time.Now(), items: []nodePhoneRecord{{Target: nodeActionTarget{ICCID: "stale"}}}}
	r, e := s.listPhones(context.Background(), nodemqtt.Command{Params: json.RawMessage(`{}`)})
	if r != nil || e == nil {
		t.Fatalf("new query fell back to stale cache: %v %+v", r, e)
	}
	if f.reads != 4 {
		t.Fatalf("new query did not read hardware: %d", f.reads)
	}
}
func TestPhonesListCardFailureDoesNotReturnPartialSuccess(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 2)
	f.errAt = d.entries[1].ID
	f.err = errors.New("card read failed")
	r, e := s.listPhones(context.Background(), nodemqtt.Command{Params: json.RawMessage(`{}`)})
	if r != nil || e == nil {
		t.Fatalf("read failure returned success: %v %+v", r, e)
	}
}

func TestPhonesListRejectsSameIMEIUSBReenumeration(t *testing.T) {
	for _, after := range []string{"during-read", "after-earlier-read"} {
		t.Run(after, func(t *testing.T) {
			s, d, f := phoneHardwareFixture(t, 2)
			f.afterRead = func(id string) {
				if (after == "during-read" && id == d.entries[0].ID) || (after == "after-earlier-read" && id == d.entries[1].ID) {
					d.entries[0].Candidate.USBGeneration = "new-generation"
				}
			}
			r, e := s.listPhones(context.Background(), nodemqtt.Command{Params: json.RawMessage(`{}`)})
			if r != nil || e == nil || e.Code != "IDENTITY_PENDING" {
				t.Fatalf("USB reenumeration accepted: %v %+v", r, e)
			}
		})
	}
}

func TestPhonesListDefaultsToAllNineteenSlotsIncludingEmptyCards(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 19)
	for i, entry := range d.entries {
		if i%2 == 0 {
			f.cards[entry.ID] = device.EsimInfo{AID: "A0000005591010"}
		}
	}
	r := callPhones(t, s, 50, "")
	items := r["items"].([]any)
	slots := map[string]bool{}
	for _, raw := range items {
		row := raw.(map[string]any)
		target := row["target"].(map[string]any)
		slots[target["slot"].(string)] = true
		if len(row) != 5 || len(target) != 5 {
			t.Fatal("wire fields changed")
		}
	}
	if len(slots) != 19 || len(items) != 19 {
		t.Fatalf("got %d rows / %d slots", len(items), len(slots))
	}
}

func TestPhonesListExplicitOptOutFitsPlatformAndDoesNotTurnProfilesIntoSlots(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 2)
	f.cards[d.entries[1].ID] = device.EsimInfo{AID: "A0000005591010"}
	profiles := make([]device.EsimProfile, 21)
	for i := range profiles {
		profiles[i] = device.EsimProfile{ICCID: fmt.Sprintf("89012405271857%05d", i), StateText: "disabled"}
	}
	f.cards[d.entries[0].ID] = device.EsimInfo{AID: "A0000005591010", Profiles: profiles}
	result := callPhonesWithEmptySlots(t, s, 50, "", false)
	items := result["items"].([]any)
	if len(items) != 21 {
		t.Fatalf("platform expects 21 real profiles, got %d records", len(items))
	}
	for _, raw := range items {
		target := raw.(map[string]any)["target"].(map[string]any)
		if target["iccid"] == "" || target["slot"] != d.entries[0].Snapshot.IMEI {
			t.Fatalf("invalid profile or invented slot: %v", target)
		}
	}
	inventory := inventoryCall(t, s, 50, "")
	if len(inventory["items"].([]any)) != 2 {
		t.Fatal("physical inventory lost blank module")
	}
}

func TestPhonesListBlankOnlyAndCursorModeIsolation(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 2)
	for _, entry := range d.entries {
		f.cards[entry.ID] = device.EsimInfo{AID: "A0000005591010"}
	}
	result := callPhones(t, s, 10, "")
	if len(result["items"].([]any)) != 2 || result["complete"] != true || result["cursor"] != nil {
		t.Fatalf("default list omitted blank modules: %v", result)
	}
	first := callPhones(t, s, 1, "")
	second := callPhones(t, s, 1, first["cursor"].(string))
	if len(second["items"].([]any)) != 1 || second["complete"] != true {
		t.Fatalf("default continuation omitted blank module: %v", second)
	}
	params, _ := json.Marshal(map[string]any{"page_size": 1, "cursor": first["cursor"], "include_empty_slots": false})
	if _, err := s.listPhones(context.Background(), nodemqtt.Command{Params: params}); err == nil || err.Code != "CURSOR_EXPIRED" {
		t.Fatalf("cursor crossed listing modes: %v", err)
	}
}

func TestPhonesCheckFollowsSwappedCardWithoutSavedGroup(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 2)
	ctx := context.Background()
	iccid := f.cards[d.entries[0].ID].Profiles[0].ICCID
	oldTarget := nodemqtt.Target{Device: d.entries[0].Snapshot.IMEI, Slot: d.entries[0].Snapshot.IMEI, ICCID: iccid, BindingVersion: 1}
	newTarget := nodemqtt.Target{Device: d.entries[1].Snapshot.IMEI, Slot: d.entries[1].Snapshot.IMEI, ICCID: iccid, BindingVersion: 1}
	// Both the desired group and the short-lived cache still name the old slot.
	_, err := s.database.SaveMultiSIMConfig(ctx, store.MultiSIMConfig{DeviceID: oldTarget.Device, Profiles: []store.MultiSIMProfile{{ICCID: iccid}}})
	if err != nil {
		t.Fatal(err)
	}
	s.phoneCache = phoneCollection{generatedAt: time.Now(), items: []nodePhoneRecord{{Target: nodeActionTarget{Device: oldTarget.Device, Slot: oldTarget.Slot, ICCID: iccid, BindingVersion: 1}, Available: true, TunnelState: "registered"}}}
	f.cards[d.entries[1].ID] = f.cards[d.entries[0].ID]
	f.cards[d.entries[0].ID] = device.EsimInfo{AID: "A0000005591010"}
	r, failure := s.checkPhone(ctx, nodemqtt.Command{Target: &newTarget, Params: json.RawMessage(`{}`)})
	if failure != nil {
		t.Fatalf("new slot rejected: %+v", failure)
	}
	if r.(map[string]any)["available"] != false || r.(map[string]any)["tunnel_state"] != "stopped" {
		t.Fatalf("invented tunnel readiness: %v", r)
	}
	if f.reads != 1 {
		t.Fatalf("targeted check read %d cards", f.reads)
	}
	if _, failure = s.checkPhone(ctx, nodemqtt.Command{Target: &oldTarget, Params: json.RawMessage(`{}`)}); failure == nil || failure.Code != "PROFILE_NOT_FOUND" {
		t.Fatalf("stale old binding accepted: %+v", failure)
	}
}

type absentPhoneReader struct {
	*phoneCardReaderFake
	absentID   string
	afterProbe func()
}

func (f *absentPhoneReader) ExecuteAT(_ context.Context, id, command string) (modem.Response, error) {
	if command != "AT+CPIN?" {
		f.t.Fatalf("unexpected presence command %q", command)
	}
	if f.afterProbe != nil {
		f.afterProbe()
	}
	if id == f.absentID {
		return modem.Response{}, &modem.CommandError{Final: "+CME ERROR: 10"}
	}
	return modem.Response{Lines: []string{"+CPIN: READY"}, Final: "OK"}, nil
}

func TestPhonesListReportsConfirmedAbsentSIMWithoutReadingAPDU(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 2)
	f.errAt = d.entries[0].ID
	f.err = errors.New("no card APDU must not be attempted")
	s.esim = &absentPhoneReader{phoneCardReaderFake: f, absentID: d.entries[0].ID}
	r := callPhones(t, s, 50, "")
	items := r["items"].([]any)
	if len(items) != 2 || f.reads != 1 {
		t.Fatalf("items=%v reads=%d", items, f.reads)
	}
	row := items[0].(map[string]any)
	if row["reason"] != "未检测到 SIM 卡" || row["available"] != false || row["target"].(map[string]any)["iccid"] != "" {
		t.Fatalf("absence misreported as blank card: %v", row)
	}
}

func TestPhonesListRejectsReenumerationDuringAbsenceProbe(t *testing.T) {
	s, d, f := phoneHardwareFixture(t, 1)
	s.esim = &absentPhoneReader{phoneCardReaderFake: f, absentID: d.entries[0].ID, afterProbe: func() { d.entries[0].Candidate.USBGeneration = "changed" }}
	if _, failure := s.listPhones(context.Background(), nodemqtt.Command{Params: json.RawMessage(`{}`)}); failure == nil || failure.Code != "IDENTITY_PENDING" {
		t.Fatalf("identity change accepted: %+v", failure)
	}
}
