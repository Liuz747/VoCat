package multisim

import (
	"fmt"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{DeviceID: "physical-reader", Enabled: true, Profiles: []Profile{
		{ICCID: "8910000000000000001", AID: "A000000001"},
		{ICCID: "8910000000000000002", AID: "A000000002"},
	}}
}

func TestConfigRejectsAmbiguousProfileSelection(t *testing.T) {
	valid := testConfig()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"duplicate ICCID", func(c *Config) { c.Profiles[1].ICCID = c.Profiles[0].ICCID }},
		{"invalid ICCID", func(c *Config) { c.Profiles[0].ICCID = "89wrong" }},
		{"invalid AID", func(c *Config) { c.Profiles[0].AID = "no-hex" }},
		{"no enabled line", func(c *Config) { c.Profiles = nil }},
		{"empty device", func(c *Config) { c.DeviceID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig()
			tc.change(&c)
			if c.Validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if err := (Config{DeviceID: "reader"}).Validate(); err != nil {
		t.Fatalf("cannot disable empty group: %v", err)
	}
}

func TestConfigAllowsOneLineAndLargeGroups(t *testing.T) {
	one := testConfig()
	one.Profiles = one.Profiles[:1]
	if err := one.Validate(); err != nil {
		t.Fatalf("single-profile group rejected: %v", err)
	}
	large := Config{DeviceID: "reader", Enabled: true}
	for i := 0; i < 20; i++ {
		large.Profiles = append(large.Profiles, Profile{ICCID: fmt.Sprintf("89100000000000000%02d", i), AID: "A000000001"})
	}
	if err := large.Validate(); err != nil {
		t.Fatalf("20-profile group rejected: %v", err)
	}
}

func TestConfigAllowsSharedISDRAID(t *testing.T) {
	c := testConfig()
	c.Profiles[0].AID = "A0000005591010FFFFFFFF8900000100"
	c.Profiles[1].AID = "a0000005591010ffffffff8900000100"
	if err := c.Validate(); err != nil {
		t.Fatalf("same eUICC management AID rejected: %v", err)
	}
}

func TestLineIDSeparatesPhysicalAndSubscriptionIdentities(t *testing.T) {
	a := LineID("device-a", "8910000000000000001")
	if a == "" || strings.Contains(a, "8910000000000000001") {
		t.Fatal("line ID exposes identity or is empty")
	}
	if a != LineID("device-a", "8910000000000000001") || a == LineID("device-b", "8910000000000000001") || a == LineID("device-a", "8910000000000000002") {
		t.Fatal("line ID not stable and scoped")
	}
	if LineID("ab", "c") == LineID("a", "bc") {
		t.Fatal("ambiguous concatenation")
	}
}
