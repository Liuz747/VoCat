package nodemqtt

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDecodeCommandStrictAndBeijingTime(t *testing.T) {
	payload := []byte(`{"id":"cmd-1","action":"sms.send","time":"2026-09-13 08:00:00","expires_at":"2026-09-13 08:02:00","target":{"device":"pool","slot":"6","phone":"+18603161135","binding_version":1},"params":{"to":"10086","text":"hello"}}`)
	command, err := DecodeCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	if command.ID != "cmd-1" || command.Target == nil || command.Target.BindingVersion != 1 {
		t.Fatalf("unexpected command: %+v", command)
	}
	created, _ := ParseTime(command.Time)
	if created.UTC().Hour() != 0 {
		t.Fatalf("Beijing time parsed as %s", created.UTC())
	}
	if _, err := DecodeCommand(append(payload[:len(payload)-1], []byte(`,"unknown":true}`)...)); err == nil {
		t.Fatal("unknown command field was accepted")
	}
}

func TestValidateTarget(t *testing.T) {
	phone := "+18603161135"
	target := &Target{Device: "pool", Slot: "6", Phone: &phone, BindingVersion: 1}
	if err := ValidateTarget(target, true); err != nil {
		t.Fatal(err)
	}
	target.BindingVersion = 2
	if err := ValidateTarget(target, true); err == nil {
		t.Fatal("binding version 2 was accepted")
	}
}

func TestDecodeAckRequiresExactTaskSequence(t *testing.T) {
	ack, err := DecodeAck([]byte(`{"time":"2026-09-13 08:00:00","items":[{"kind":"task","id":"cmd-1","seq":2},{"kind":"event","id":"evt-1"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ack.Items) != 2 || ack.Items[0].Seq == nil || *ack.Items[0].Seq != 2 {
		t.Fatalf("unexpected ACK: %+v", ack)
	}
	if _, err := DecodeAck([]byte(`{"time":"2026-09-13 08:00:00","items":[{"kind":"task","id":"cmd-1"}]}`)); err == nil {
		t.Fatal("task ACK without seq was accepted")
	}
}

func TestTaskMessageKeepsExplicitNulls(t *testing.T) {
	payload := marshalTask("cmd", 1, time.Now(), "accepted", nil, nil)
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded["result"]) != "null" || string(decoded["error"]) != "null" {
		t.Fatalf("null fields missing: %s", payload)
	}
}

func TestEventTargetKeepsUnknownPhoneAsNull(t *testing.T) {
	payload, err := json.Marshal(EventMessage{
		ID: "evt-1", Event: "sms.received", Time: "2026-09-13 08:00:00",
		Target: &EventTarget{Device: "pool", Slot: "6", BindingVersion: 1, ICCID: "8986000000000000001"},
		Data:   json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	var target map[string]json.RawMessage
	if err := json.Unmarshal(decoded["target"], &target); err != nil {
		t.Fatal(err)
	}
	if string(target["phone"]) != "null" {
		t.Fatalf("event phone must be explicit null: %s", payload)
	}
}
