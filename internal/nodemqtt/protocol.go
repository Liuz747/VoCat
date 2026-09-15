package nodemqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const (
	ProtocolVersion = "1.0"
	TimeLayout      = "2006-01-02 15:04:05"
)

var (
	idPattern        = regexp.MustCompile(`^[^\x00-\x1f]{1,128}$`)
	e164Pattern      = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)
	digitsPattern    = regexp.MustCompile(`^[0-9]{1,32}$`)
	recipientPattern = regexp.MustCompile(`^\+?[0-9]{1,32}$`)
	beijing          = time.FixedZone("UTC+08:00", 8*60*60)
)

type Target struct {
	Device         string  `json:"device"`
	Slot           string  `json:"slot"`
	Phone          *string `json:"phone,omitempty"`
	ICCID          string  `json:"iccid,omitempty"`
	BindingVersion int     `json:"binding_version"`
}

type Command struct {
	ID        string          `json:"id"`
	Action    string          `json:"action"`
	Time      string          `json:"time"`
	ExpiresAt string          `json:"expires_at"`
	Target    *Target         `json:"target,omitempty"`
	Params    json.RawMessage `json:"params"`
}

type TaskError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Certainty string `json:"certainty"`
}

type TaskMessage struct {
	ID     string          `json:"id"`
	Seq    int             `json:"seq"`
	Time   string          `json:"time"`
	State  string          `json:"state"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type EventMessage struct {
	ID     string          `json:"id"`
	Event  string          `json:"event"`
	Time   string          `json:"time"`
	Target *EventTarget    `json:"target,omitempty"`
	Data   json.RawMessage `json:"data"`
}

// EventTarget keeps phone present as either a string or explicit JSON null,
// as required for number-level events. Command targets use omission semantics.
type EventTarget struct {
	Device         string  `json:"device"`
	Slot           string  `json:"slot"`
	Phone          *string `json:"phone"`
	BindingVersion int     `json:"binding_version"`
	ICCID          string  `json:"iccid"`
}

type AckMessage struct {
	Time  string    `json:"time"`
	Items []AckItem `json:"items"`
}

type AckItem struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Seq  *int   `json:"seq,omitempty"`
}

func DecodeCommand(payload []byte) (Command, error) {
	var value Command
	if err := decodeStrict(payload, &value); err != nil {
		return value, err
	}
	value.ID, value.Action = strings.TrimSpace(value.ID), strings.TrimSpace(value.Action)
	if !idPattern.MatchString(value.ID) {
		return value, errors.New("id must contain 1-128 printable characters")
	}
	if value.Action == "" {
		return value, errors.New("action is required")
	}
	if _, err := ParseTime(value.Time); err != nil {
		return value, fmt.Errorf("invalid time: %w", err)
	}
	if value.ExpiresAt == "" {
		return value, errors.New("expires_at is required")
	}
	expires, err := ParseTime(value.ExpiresAt)
	if err != nil {
		return value, fmt.Errorf("invalid expires_at: %w", err)
	}
	created, _ := ParseTime(value.Time)
	if !expires.After(created) {
		return value, errors.New("expires_at must be later than time")
	}
	if len(value.Params) == 0 || string(value.Params) == "null" {
		return value, errors.New("params object is required")
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(value.Params, &params); err != nil || params == nil {
		return value, errors.New("params must be an object")
	}
	return value, nil
}

func DecodeAck(payload []byte) (AckMessage, error) {
	var value AckMessage
	if err := decodeStrict(payload, &value); err != nil {
		return value, err
	}
	if _, err := ParseTime(value.Time); err != nil {
		return value, fmt.Errorf("invalid ACK time: %w", err)
	}
	if len(value.Items) < 1 || len(value.Items) > 100 {
		return value, errors.New("ACK items must contain 1-100 entries")
	}
	for _, item := range value.Items {
		if (item.Kind != "task" && item.Kind != "event") || !idPattern.MatchString(strings.TrimSpace(item.ID)) {
			return value, errors.New("invalid ACK item")
		}
		if item.Kind == "task" && (item.Seq == nil || *item.Seq < 1) {
			return value, errors.New("task ACK requires a positive seq")
		}
		if item.Kind == "event" && item.Seq != nil {
			return value, errors.New("event ACK must omit seq")
		}
	}
	return value, nil
}

func ValidateTarget(target *Target, requireProfile bool) error {
	if target == nil {
		return errors.New("target is required")
	}
	target.Device, target.Slot, target.ICCID = strings.TrimSpace(target.Device), strings.TrimSpace(target.Slot), strings.TrimSpace(target.ICCID)
	if target.Device == "" || len([]rune(target.Device)) > 64 {
		return errors.New("target.device is required")
	}
	if target.Slot == "" || len([]rune(target.Slot)) > 32 {
		return errors.New("target.slot is required")
	}
	if target.BindingVersion != 1 {
		return errors.New("target.binding_version must be 1")
	}
	if target.Phone != nil {
		phone := strings.TrimSpace(*target.Phone)
		target.Phone = &phone
		if !e164Pattern.MatchString(phone) {
			return errors.New("target.phone must be E.164")
		}
	}
	if target.ICCID != "" && !digitsPattern.MatchString(target.ICCID) {
		return errors.New("target.iccid must contain 1-32 digits")
	}
	if requireProfile && target.Phone == nil && target.ICCID == "" {
		return errors.New("target.phone or target.iccid is required")
	}
	if !requireProfile && (target.Phone != nil || target.ICCID != "") {
		return errors.New("target.phone and target.iccid are not allowed")
	}
	return nil
}

func ValidateRecipient(value string) bool { return recipientPattern.MatchString(value) }
func ValidE164(value string) bool         { return e164Pattern.MatchString(value) }
func ParseTime(value string) (time.Time, error) {
	return time.ParseInLocation(TimeLayout, value, beijing)
}
func FormatTime(value time.Time) string { return value.In(beijing).Format(TimeLayout) }
func NullJSON() json.RawMessage         { return json.RawMessage("null") }

func MarshalCanonical(value any) ([]byte, error) { return json.Marshal(value) }

func decodeStrict(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
