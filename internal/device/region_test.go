package device

import (
	"context"
	"testing"
)

// injectSnapshot stores the SIM identity without replaying the full modem transcript.
func injectSnapshot(t *testing.T, manager *Manager, id string, snapshot *Snapshot) {
	t.Helper()
	state, err := manager.lookup(id)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	manager.setResult(id, state, snapshot, nil)
}

func TestCardMCCMNC(t *testing.T) {
	t.Parallel()
	if mcc, _ := CardMCCMNC("460001234567890"); mcc != "460" {
		t.Fatalf("CardMCCMNC mcc = %q, want 460", mcc)
	}
	if mcc, mnc := CardMCCMNCWithLength("454006395879502", 2); mcc != "454" || mnc != "00" {
		t.Fatalf("CardMCCMNCWithLength = (%q, %q), want (454, 00)", mcc, mnc)
	}
	for _, bad := range []string{"", "4600", "4600X1234"} {
		if mcc, _ := CardMCCMNC(bad); mcc != "" {
			t.Fatalf("CardMCCMNC(%q) mcc = %q, want empty", bad, mcc)
		}
	}
}

func TestPlaceholderIMSIIsNotTreatedAsARealCarrier(t *testing.T) {
	t.Parallel()
	if !IsPlaceholderIMSI("460000000000000") {
		t.Fatal("all-zero subscriber identity should be treated as an unprovisioned placeholder")
	}
	if IsPlaceholderIMSI("460001234567890") {
		t.Fatal("real subscriber identity was classified as a placeholder")
	}
	if mcc, mnc := CardMCCMNC("460000000000000"); mcc != "" || mnc != "" {
		t.Fatalf("placeholder MCC/MNC = %q/%q, want empty", mcc, mnc)
	}
}

func TestSetNetworkAllowedRegardlessOfSIMRegion(t *testing.T) {
	for _, imsi := range []string{"460001234567890", "461001234567890", "310260123456789"} {
		t.Run(imsi, func(t *testing.T) {
			client := &transcriptClient{steps: []clientStep{
				{command: `AT+CGDCONT=1,"IPV4V6","internet"`, response: okResponse()},
				{command: "AT+CGATT=1", response: okResponse()},
				{command: "AT+CGACT=1,1", response: okResponse()},
			}}
			manager, id := newStartedTestManager(t, client)
			injectSnapshot(t, manager, id, &Snapshot{DeviceID: id, IMSI: imsi})
			result, err := manager.SetNetwork(context.Background(), id, NetworkRequest{
				Enabled: true, APN: "internet", IPVersion: "IPV4V6",
			})
			if err != nil {
				t.Fatalf("enable network: %v", err)
			}
			if !result.Enabled {
				t.Fatalf("enable result = %#v", result)
			}
			client.assertDone(t)
		})
	}
}

// A missing snapshot must not prevent network access.
func TestSetNetworkAllowedWhenSIMRegionUnknown(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: `AT+CGDCONT=1,"IPV4V6","internet"`, response: okResponse()},
		{command: "AT+CGATT=1", response: okResponse()},
		{command: "AT+CGACT=1,1", response: okResponse()},
	}}
	manager, id := newStartedTestManager(t, client)
	if _, err := manager.SetNetwork(context.Background(), id, NetworkRequest{
		Enabled: true, APN: "internet", IPVersion: "IPV4V6",
	}); err != nil {
		t.Fatalf("enable network with unknown region: %v", err)
	}
	client.assertDone(t)
}

func TestSendSMSAllowedRegardlessOfSIMRegion(t *testing.T) {
	for _, imsi := range []string{"460001234567890", "461001234567890", "310260123456789"} {
		t.Run(imsi, func(t *testing.T) {
			client := &transcriptClient{
				steps: []clientStep{
					{command: "AT+CMGF=1", response: okResponse()},
					{command: `AT+CSCS="GSM"`, response: okResponse()},
					{command: "AT+CSMP=49,167,0,0", response: okResponse()},
				},
				promptSteps: []promptClientStep{{
					command: `AT+CMGS="+8613800138000"`, payload: "HELLO", response: okResponse("+CMGS: 23"),
				}},
			}
			manager, id := newStartedTestManager(t, client)
			injectSnapshot(t, manager, id, &Snapshot{DeviceID: id, IMSI: imsi})
			result, err := manager.SendSMS(context.Background(), id, "+8613800138000", "HELLO")
			if err != nil {
				t.Fatalf("SendSMS: %v", err)
			}
			if !result.AllPartsAccepted || result.PartsAccepted != 1 {
				t.Fatalf("SMS submission = %+v", result)
			}
			client.assertDone(t)
		})
	}
}
