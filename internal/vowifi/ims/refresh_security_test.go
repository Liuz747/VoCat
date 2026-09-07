package ims

import (
	"net"
	"testing"
	"time"
)

// Optional security may decline during initial negotiation, but doing so
// during reauthentication would close listeners used by live SMS receivers.
func TestRuntimeSecurityDowngradeLeavesLiveTransportIntact(t *testing.T) {
	tcp, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	session := &Session{
		provider:       &Provider{config: Config{SecurityMode: SecurityOptional}},
		runtimeStarted: true, securityActive: true, protectedTCP: tcp, protectedUDP: udp,
		securityProposal: securityProposal{spiClient: 1001, spiServer: 1002, portClient: 40666, portServer: 55610},
	}
	if _, _, err := session.registrationSecurity(&sipResponse{StatusCode: 401}); err == nil {
		t.Fatal("runtime security downgrade was accepted instead of requiring a new session")
	}
	if session.securityDeclined || session.protectedTCP != tcp || session.protectedUDP != udp || session.securityProposal.spiClient != 1001 {
		t.Fatal("failed reauthentication mutated the live security transport")
	}
	if err := tcp.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("live TCP listener was closed: %v", err)
	}
	if _, err := udp.WriteToUDP([]byte("still-owned"), udp.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("live UDP socket was closed: %v", err)
	}
}

func TestProtectedRefreshParsesOfferWithoutReplacingCurrentAgreement(t *testing.T) {
	const offer = "ipsec-3gpp;q=0.100;alg=hmac-sha-1-96;prot=esp;mod=trans;ealg=aes-cbc;spi-c=2001;spi-s=2002;port-c=50601;port-s=50600"
	packet, err := parseSIPPacket(testResponse(401, "Unauthorized", "security-refresh", "2 REGISTER", []string{"Security-Server: " + offer}))
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		provider:       &Provider{config: Config{SecurityMode: SecurityRequired}},
		runtimeStarted: true, securityActive: true,
		securityProposal:  securityProposal{spiClient: 1001, spiServer: 1002, portClient: 40666, portServer: 55610},
		securityAgreement: securityAgreement{verifyValue: "existing-agreement"},
	}
	agreement, use, err := session.registrationSecurity(packet.Response)
	if err != nil || !use || agreement.selected.spiClient != 2001 {
		t.Fatalf("protected reauthentication offer was not parsed: use=%v err=%v", use, err)
	}
	if session.securityAgreement.verifyValue != "existing-agreement" {
		t.Fatal("reauthentication replaced the existing transport agreement")
	}
	session.securityActive = false
	if _, _, err := session.registrationSecurity(packet.Response); err == nil {
		t.Fatal("runtime activation accepted without rebuilding its receivers")
	}
}
