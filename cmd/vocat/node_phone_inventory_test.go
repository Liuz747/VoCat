package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"vocat/internal/device"
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
	t.Helper()
	p, _ := json.Marshal(map[string]any{"page_size": size, "cursor": cursor})
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
	result := callPhones(t, s, 50, "")
	if len(result) != 5 {
		t.Fatalf("changed result shape: %v", result)
	}
	items := result["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("got %d phone rows; want two real cards, no blank-card row", len(items))
	}
	if f.reads != 3 {
		t.Fatalf("read %d cards, want 3", f.reads)
	}
	for i, item := range items {
		row := item.(map[string]any)
		target := row["target"].(map[string]any)
		if target["slot"] != d.entries[i].Snapshot.IMEI || target["iccid"] != f.cards[d.entries[i].ID].Profiles[0].ICCID {
			t.Fatalf("missing real profile: %v", target)
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
