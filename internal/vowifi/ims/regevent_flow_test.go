package ims

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"vocat/internal/vowifi"
)

// End-to-end shape of the registration-state event package on the wire:
// register, subscribe (TS 24.229 5.1.1.3), then let the registrar push a
// NOTIFY saying the binding was deactivated and watch the UE re-register on
// the transport it already has (5.1.1.5A). Getting a NOTIFY must never cost a
// tunnel rebuild, which is the whole reason for preferring it over waiting for
// the next refresh.
func TestRegEventSubscriptionAndDeactivatedNotifyReRegisters(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	subscribed := make(chan map[string]string, 1)
	reRegistered := make(chan struct{}, 1)
	serverDone := make(chan error, 1)

	go func() {
		serverDone <- func() error {
			registers := 0
			var ueAddr *net.UDPAddr
			var subscribeCallID string
			for {
				packet := make([]byte, 65535)
				count, remote, err := listener.ReadFromUDP(packet)
				if err != nil {
					return nil // listener closed by the test
				}
				ueAddr = remote
				startLine, headers, err := parseTestRequest(packet[:count])
				if err != nil {
					return err
				}
				if strings.HasPrefix(startLine, "SIP/2.0") {
					// The UE's 200 to our NOTIFY; this fixture only drives requests.
					continue
				}
				method := strings.Fields(startLine)[0]
				switch method {
				case "REGISTER":
					registers++
					if registers == 1 {
						nonce := make([]byte, 32)
						challenge := []string{
							`WWW-Authenticate: Digest realm="ims.mnc001.mcc001.3gppnetwork.org", nonce="` +
								base64.StdEncoding.EncodeToString(nonce) + `", algorithm=AKAv1-MD5, qop="auth"`,
						}
						if _, err := listener.WriteToUDP(testResponse(401, "Unauthorized",
							headers["call-id"], headers["cseq"], challenge), remote); err != nil {
							return err
						}
						continue
					}
					if registers > 2 {
						select {
						case reRegistered <- struct{}{}:
						default:
						}
					}
					if _, err := listener.WriteToUDP(testResponse(200, "OK",
						headers["call-id"], headers["cseq"],
						[]string{"Contact: " + headers["contact"] + ";expires=600"}), remote); err != nil {
						return err
					}
				case "SUBSCRIBE":
					subscribeCallID = headers["call-id"]
					select {
					case subscribed <- headers:
					default:
					}
					if _, err := listener.WriteToUDP(testResponse(200, "OK",
						headers["call-id"], headers["cseq"],
						[]string{"Expires: 600000"}), remote); err != nil {
						return err
					}
					// RFC 6665: the subscription is confirmed by an immediate
					// NOTIFY. This one reports the binding as deactivated,
					// which 24.229 5.1.1.5A answers with a new registration.
					body := `<?xml version="1.0"?>
<reginfo xmlns="urn:ietf:params:xml:ns:reginfo" version="1" state="full">
  <registration aor="sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org" id="a7" state="terminated">
    <contact id="76" state="terminated" event="deactivated">
      <uri>` + contactURIFromHeader(headers["contact"]) + `</uri>
    </contact>
  </registration>
</reginfo>`
					notify := strings.Join([]string{
						"NOTIFY sip:ue SIP/2.0",
						"Via: SIP/2.0/UDP " + listener.LocalAddr().String() + ";branch=z9hG4bKnotify",
						"From: <sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org>;tag=reg",
						"To: <sip:001010123456789@ims.mnc001.mcc001.3gppnetwork.org>;tag=ue",
						"Call-ID: " + subscribeCallID,
						"CSeq: 1 NOTIFY",
						"Event: reg",
						"Subscription-State: active;expires=600000",
						"Content-Type: application/reginfo+xml",
						fmt.Sprintf("Content-Length: %d", len(body)),
						"", body,
					}, "\r\n")
					if _, err := listener.WriteToUDP([]byte(notify), ueAddr); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected method %q", method)
				}
			}
		}()
	}()

	provider, err := NewProvider(&recordingAKA{result: vowifi.AKAResult{RES: []byte{1, 2, 3, 4, 5, 6, 7, 8}}}, Config{
		PCSCF:                       listener.LocalAddr().String(),
		LocalAddress:                "127.0.0.1",
		Transport:                   "udp",
		TransactionTimeout:          2 * time.Second,
		SecurityMode:                SecurityDisabled,
		SubscribeRegistrationEvents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := provider.Start(context.Background(), vowifi.IMSRequest{
		DeviceID: "ec20",
		Identity: vowifi.SIMIdentity{IMSI: "001010123456789", HomeMCC: "001", HomeMNC: "01"},
		Tunnel: evidenceTunnel{evidence: vowifi.TunnelEvidence{
			Established: true, LocalIPv4: "127.0.0.1", PCSCF: []string{listener.LocalAddr().String()},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closed, cancel := context.WithCancel(context.Background())
		cancel()
		_ = session.Close(closed)
	}()

	select {
	case headers := <-subscribed:
		if got := strings.TrimSpace(headers["event"]); !strings.EqualFold(got, "reg") {
			t.Fatalf("SUBSCRIBE Event = %q, want reg", got)
		}
		if !strings.Contains(strings.ToLower(headers["accept"]), "application/reginfo+xml") {
			t.Fatalf("SUBSCRIBE Accept = %q, want application/reginfo+xml", headers["accept"])
		}
		if strings.TrimSpace(headers["contact"]) == "" {
			t.Fatal("SUBSCRIBE omitted Contact; NOTIFYs would have nowhere to go")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no SUBSCRIBE for the reg event package")
	}

	select {
	case <-reRegistered:
	case err := <-serverDone:
		t.Fatalf("loopback registrar stopped before the re-registration: %v", err)
	case <-time.After(8 * time.Second):
		t.Fatal("a deactivated reg event NOTIFY did not trigger a re-registration")
	}
}

// contactURIFromHeader pulls the bare URI out of a Contact header the way a
// registrar stores it, dropping the angle brackets and the UE's parameters.
func contactURIFromHeader(value string) string {
	value = strings.TrimSpace(value)
	if start := strings.Index(value, "<"); start >= 0 {
		if end := strings.Index(value[start+1:], ">"); end >= 0 {
			return value[start+1 : start+1+end]
		}
	}
	if cut := strings.Index(value, ";"); cut >= 0 {
		return strings.TrimSpace(value[:cut])
	}
	return value
}
