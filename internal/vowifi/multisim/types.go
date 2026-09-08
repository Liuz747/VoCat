// Package multisim coordinates independent VoWiFi sessions sharing one eUICC.
package multisim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"vocat/internal/vowifi"
)

var (
	ErrOperationInProgress = errors.New("multisim: a configuration operation is in progress")
	ErrClosed              = errors.New("multisim: manager is closed")
	ErrNotRegistered       = errors.New("multisim: line is not registered")
	ErrIdentityMismatch    = errors.New("multisim: live SIM identity does not match the requested profile")
	ErrLineRequired        = errors.New("multisim: the group has several lines; select one by ICCID or session ID")
)

// LineIdentity names the subscription that carried a line operation. DeviceID
// is the physical reader; SessionID is the line's own runtime identity.
type LineIdentity struct {
	DeviceID    string
	SessionID   string
	ICCID       string
	IMSI        string
	PhoneNumber string
}

type Profile struct {
	ICCID string `json:"iccid"`
	AID   string `json:"aid"`
	Name  string `json:"name,omitempty"`
}

type Config struct {
	DeviceID string    `json:"device_id"`
	Enabled  bool      `json:"enabled"`
	Profiles []Profile `json:"profiles"`
}

func (config Config) Validate() error {
	if config.DeviceID == "" || config.DeviceID != strings.TrimSpace(config.DeviceID) {
		return errors.New("multisim: physical device ID is required")
	}
	// No upper bound: one eUICC may carry dozens of profiles and the startup
	// admission gate (StartupGate) keeps reader contention bounded regardless
	// of the group size.
	if config.Enabled && len(config.Profiles) < 1 {
		return errors.New("multisim: enabled groups require at least one profile")
	}
	iccids := make(map[string]bool)
	for _, profile := range config.Profiles {
		if err := profile.validate(); err != nil {
			return err
		}
		if iccids[profile.ICCID] {
			return errors.New("multisim: profiles must have unique ICCIDs")
		}
		iccids[profile.ICCID] = true
	}
	return nil
}

func (profile Profile) validate() error {
	if !digits(profile.ICCID) {
		return errors.New("multisim: profile ICCID must contain only decimal digits")
	}
	if len(profile.AID) == 0 {
		return errors.New("multisim: profile AID is required")
	}
	if _, err := hex.DecodeString(profile.AID); err != nil {
		return errors.New("multisim: profile AID must contain complete hexadecimal bytes")
	}
	return nil
}

func digits(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// LineID is stable, opaque, and separate from the physical device namespace.
func LineID(deviceID, iccid string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(deviceID), deviceID, iccid)))
	return "multisim-" + hex.EncodeToString(sum[:16])
}

type LineState struct {
	ICCID     string       `json:"iccid"`
	Name      string       `json:"name,omitempty"`
	SessionID string       `json:"session_id"`
	State     vowifi.State `json:"state"`
}

type GroupState struct {
	DeviceID  string      `json:"device_id"`
	Enabled   bool        `json:"enabled"`
	Busy      bool        `json:"busy"`
	Phase     string      `json:"phase"`
	LastError string      `json:"last_error,omitempty"`
	Lines     []LineState `json:"lines"`
	UpdatedAt time.Time   `json:"updated_at"`
}

type Options struct {
	Logger           *slog.Logger
	OperationTimeout time.Duration
	CleanupTimeout   time.Duration
	RetryInitial     time.Duration
	RetryMaximum     time.Duration
	Prepare          func(context.Context, Config) error
	Restore          func(context.Context, Config) error
	Factory          func(context.Context, Config, Profile, string) (*vowifi.Orchestrator, error)
}

// AuthBackend addresses exactly one physical reader. Its own APDU lock must be
// independent of AuthBroker's transaction lock. Identity methods must read the
// actual UICC, never a configured or cached projection of its active identity.
type AuthBackend interface {
	vowifi.SIMIdentityReader
	vowifi.PreferredAKAProvider
	ActiveICCID(context.Context) (string, error)
	SwitchProfile(context.Context, Profile) error
}

type BrokerOptions struct {
	DeviceID       string
	Backend        AuthBackend
	RequestTimeout time.Duration
	Logger         *slog.Logger
}
