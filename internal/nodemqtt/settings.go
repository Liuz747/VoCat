package nodemqtt

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

const SecretMask = "********"

var nodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Settings struct {
	Enabled                bool   `json:"enabled"`
	Scheme                 string `json:"scheme"`
	Host                   string `json:"host"`
	Port                   int    `json:"port"`
	Node                   string `json:"node"`
	ClientID               string `json:"client_id"`
	Username               string `json:"username"`
	Password               string `json:"password"`
	CACertificate          string `json:"ca_certificate,omitempty"`
	KeepAliveSeconds       int    `json:"keep_alive_seconds"`
	SessionExpirySeconds   int    `json:"session_expiry_seconds"`
	HeartbeatSeconds       int    `json:"heartbeat_seconds"`
	BusinessACKEnabled     bool   `json:"business_ack_enabled"`
	CommandWorkers         int    `json:"command_workers"`
	MaxPayloadBytes        int    `json:"max_payload_bytes"`
	PhoneCacheSeconds      int    `json:"phone_cache_seconds"`
	PhoneRefreshTimeoutSec int    `json:"phone_refresh_timeout_seconds"`
}

func DefaultSettings() Settings {
	return Settings{Scheme: "tcp", Port: 1883, KeepAliveSeconds: 30, SessionExpirySeconds: 86400, HeartbeatSeconds: 30, CommandWorkers: 4, MaxPayloadBytes: 131072, PhoneCacheSeconds: 30, PhoneRefreshTimeoutSec: 15}
}

func (settings *Settings) Normalize() {
	settings.Scheme = strings.ToLower(strings.TrimSpace(settings.Scheme))
	settings.Host = strings.TrimSpace(settings.Host)
	settings.Node = strings.TrimSpace(settings.Node)
	settings.ClientID = strings.TrimSpace(settings.ClientID)
	settings.Username = strings.TrimSpace(settings.Username)
	settings.CACertificate = strings.TrimSpace(settings.CACertificate)
	if settings.Scheme == "" {
		settings.Scheme = "tcp"
	}
	if settings.Port == 0 {
		if settings.Scheme == "tls" {
			settings.Port = 8883
		} else {
			settings.Port = 1883
		}
	}
	if settings.KeepAliveSeconds == 0 {
		settings.KeepAliveSeconds = 30
	}
	if settings.SessionExpirySeconds == 0 {
		settings.SessionExpirySeconds = 86400
	}
	if settings.HeartbeatSeconds == 0 {
		settings.HeartbeatSeconds = 30
	}
	if settings.CommandWorkers == 0 {
		settings.CommandWorkers = 4
	}
	if settings.MaxPayloadBytes == 0 {
		settings.MaxPayloadBytes = 131072
	}
	if settings.PhoneCacheSeconds == 0 {
		settings.PhoneCacheSeconds = 30
	}
	if settings.PhoneRefreshTimeoutSec == 0 {
		settings.PhoneRefreshTimeoutSec = 15
	}
	if settings.ClientID == "" && nodePattern.MatchString(settings.Node) {
		settings.ClientID = "cp-node-" + settings.Node
	}
}

func (settings Settings) Validate() error {
	if settings.Scheme != "tcp" && settings.Scheme != "tls" {
		return errors.New("scheme must be tcp or tls")
	}
	if settings.Host == "" || strings.ContainsAny(settings.Host, "/?#") {
		return errors.New("broker host is required and must not contain a URL path")
	}
	if settings.Port < 1 || settings.Port > 65535 {
		return errors.New("broker port must be between 1 and 65535")
	}
	if !nodePattern.MatchString(settings.Node) {
		return errors.New("node must contain 1-64 letters, digits, underscores, or hyphens")
	}
	if settings.ClientID == "" || len([]rune(settings.ClientID)) > 128 {
		return errors.New("client ID must contain 1-128 characters")
	}
	if settings.KeepAliveSeconds < 5 || settings.KeepAliveSeconds > 3600 {
		return errors.New("keep alive must be between 5 and 3600 seconds")
	}
	if settings.SessionExpirySeconds < 60 || uint64(settings.SessionExpirySeconds) > uint64(^uint32(0)) {
		return errors.New("session expiry must be between 60 and 4294967295 seconds")
	}
	if settings.HeartbeatSeconds < 5 || settings.HeartbeatSeconds > 3600 {
		return errors.New("heartbeat interval must be between 5 and 3600 seconds")
	}
	if settings.CommandWorkers < 1 || settings.CommandWorkers > 32 {
		return errors.New("command workers must be between 1 and 32")
	}
	if settings.MaxPayloadBytes < 1024 || settings.MaxPayloadBytes > 1024*1024 {
		return errors.New("max payload must be between 1024 and 1048576 bytes")
	}
	if settings.PhoneCacheSeconds != 30 || settings.PhoneRefreshTimeoutSec != 15 {
		return errors.New("phone cache and refresh timeout are fixed at 30 and 15 seconds")
	}
	return nil
}

func (settings Settings) BrokerURL() string {
	scheme := "mqtt"
	if settings.Scheme == "tls" {
		scheme = "tls"
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(settings.Host, strconv.Itoa(settings.Port)))
}

func (settings Settings) Public() Settings {
	if settings.Password != "" {
		settings.Password = SecretMask
	}
	return settings
}

type Status struct {
	Enabled       bool   `json:"enabled"`
	Connected     bool   `json:"connected"`
	Subscribed    bool   `json:"subscribed"`
	LastError     string `json:"last_error,omitempty"`
	OutboxPending int    `json:"outbox_pending"`
}
