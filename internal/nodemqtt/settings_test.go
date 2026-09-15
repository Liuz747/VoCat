package nodemqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSettingsNormalizeAndValidate(t *testing.T) {
	settings := DefaultSettings()
	settings.Host = "mqtt.example.com"
	settings.Node = "mdd-test-001"
	settings.Normalize()
	if settings.ClientID != "cp-node-mdd-test-001" {
		t.Fatalf("client ID = %q", settings.ClientID)
	}
	if err := settings.Validate(); err != nil {
		t.Fatal(err)
	}
	settings.PhoneCacheSeconds = 31
	if err := settings.Validate(); err == nil {
		t.Fatal("unsupported freshness setting accepted")
	}
}

func TestSettingsPublicMasksPassword(t *testing.T) {
	settings := DefaultSettings()
	settings.Password = "secret"
	if got := settings.Public().Password; got != SecretMask {
		t.Fatalf("password = %q", got)
	}
}

func TestSettingsJSONUsesAPIFieldNames(t *testing.T) {
	settings := DefaultSettings()
	settings.ClientID = "cp-node-test"
	document, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	text := string(document)
	for _, field := range []string{"client_id", "keep_alive_seconds", "phone_refresh_timeout_seconds", "business_ack_enabled"} {
		if !strings.Contains(text, `"`+field+`"`) {
			t.Fatalf("missing API field %q in %s", field, text)
		}
	}

	var decoded Settings
	if err := json.Unmarshal([]byte(`{"client_id":"cp-node-test","keep_alive_seconds":30}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ClientID != "cp-node-test" || decoded.KeepAliveSeconds != 30 {
		t.Fatalf("decoded settings = %+v", decoded)
	}
}

func TestLogPayloadRedactsCommandSecrets(t *testing.T) {
	logged, ok := logPayload([]byte(`{"id":"cmd-1","params":{"activation_code":"secret","confirmation_code":"code","text":"hello"}}`)).(map[string]any)
	if !ok {
		t.Fatalf("logged payload = %#v", logged)
	}
	params := logged["params"].(map[string]any)
	for _, field := range []string{"activation_code", "confirmation_code", "text"} {
		if params[field] != "[REDACTED]" {
			t.Fatalf("%s was not redacted: %#v", field, params[field])
		}
	}
}
