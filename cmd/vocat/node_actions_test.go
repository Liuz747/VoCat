package main

import (
	"testing"

	"vocat/internal/vowifi"
)

func TestParseActivationCode(t *testing.T) {
	smdp, matching, err := parseActivationCode("LPA:1$smdp.example.com$MATCH")
	if err != nil || smdp != "smdp.example.com" || matching != "MATCH" {
		t.Fatalf("parse = %q %q %v", smdp, matching, err)
	}
	if _, _, err := parseActivationCode("bad"); err == nil {
		t.Fatal("invalid activation code accepted")
	}
}

func TestNodeTunnelStateRequiresSMSReady(t *testing.T) {
	state, available, _ := nodeTunnelState(vowifi.State{IMSReady: true, SMSReady: false, Active: true, Enabled: true})
	if available || state != "registered" {
		t.Fatalf("state=%s available=%v", state, available)
	}
	state, available, reason := nodeTunnelState(vowifi.State{IMSReady: true, SMSReady: true})
	if !available || state != "registered" || reason != nil {
		t.Fatalf("state=%s available=%v reason=%v", state, available, reason)
	}
}
