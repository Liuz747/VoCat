package ims

import (
	"context"
	"encoding/xml"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"vocat/internal/vowifi"
)

// Registration-state event package (RFC 3680). The S-CSCF pushes a reginfo
// document whenever a binding changes, which is the only standard way to learn
// that a registration died between refreshes: with a 3600 s expiry refreshed at
// 80%, an unnoticed deregistration would otherwise stay invisible for the best
// part of an hour.

type regEventContact struct {
	URI   string `xml:"uri"`
	State string `xml:"state,attr"`
	Event string `xml:"event,attr"`
}

type regEventRegistration struct {
	AOR      string            `xml:"aor,attr"`
	State    string            `xml:"state,attr"`
	Contacts []regEventContact `xml:"contact"`
}

type regInfoDocument struct {
	XMLName       xml.Name               `xml:"reginfo"`
	State         string                 `xml:"state,attr"`
	Registrations []regEventRegistration `xml:"registration"`
}

// regEventVerdict is what a NOTIFY body means for this session.
type regEventVerdict int

const (
	// regEventNoChange covers everything this code does not positively
	// recognise as our binding going away. Monitoring must never be the reason
	// a working line is torn down, so anything unparseable, unknown, or about
	// somebody else lands here.
	regEventNoChange regEventVerdict = iota
	// regEventReRegister is TS 24.229 5.1.1.5A's "deactivated": the network
	// dropped the binding but wants the UE back, so re-register.
	regEventReRegister
	// regEventDeregistered is a binding that is gone for a reason of the
	// network's own (unregistered, rejected, expired, probation...).
	regEventDeregistered
)

func (verdict regEventVerdict) String() string {
	switch verdict {
	case regEventReRegister:
		return "re_register"
	case regEventDeregistered:
		return "deregistered"
	default:
		return "no_change"
	}
}

func parseRegInfo(body []byte) ([]regEventRegistration, error) {
	var document regInfoDocument
	if err := xml.Unmarshal(body, &document); err != nil {
		return nil, err
	}
	return document.Registrations, nil
}

// regInfoVerdict interprets a NOTIFY body without knowing which contact is
// ours. Used when the session has not recorded a registered contact yet.
func regInfoVerdict(body []byte) regEventVerdict {
	return regInfoVerdictFor(body, "")
}

// regInfoVerdictFor interprets a NOTIFY body, restricting itself to the given
// contact URI when one is known. A partial-state document can carry another
// subscriber's binding; acting on that would take down a healthy line.
func regInfoVerdictFor(body []byte, contactURI string) regEventVerdict {
	registrations, err := parseRegInfo(body)
	if err != nil {
		return regEventNoChange
	}
	contactURI = strings.TrimSpace(contactURI)
	verdict := regEventNoChange
	for _, registration := range registrations {
		for _, contact := range registration.Contacts {
			if contactURI != "" && !sameContactURI(contact.URI, contactURI) {
				continue
			}
			terminated := strings.EqualFold(strings.TrimSpace(contact.State), "terminated") ||
				strings.EqualFold(strings.TrimSpace(registration.State), "terminated")
			if !terminated {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(contact.Event), "deactivated") {
				// Strongest outcome: the network is asking for a new
				// registration rather than reporting a permanent removal.
				return regEventReRegister
			}
			verdict = regEventDeregistered
		}
	}
	return verdict
}

// sameContactURI compares the addressing part of two contact URIs. The
// registrar echoes the URI it stored, which may carry parameters or angle
// brackets this UE did not send.
func sameContactURI(left, right string) bool {
	return strings.EqualFold(normalizeContactURI(left), normalizeContactURI(right))
}

func normalizeContactURI(value string) string {
	value = strings.TrimSpace(value)
	if start := strings.Index(value, "<"); start >= 0 {
		if end := strings.Index(value[start+1:], ">"); end >= 0 {
			value = value[start+1 : start+1+end]
		}
	}
	if cut := strings.Index(value, ";"); cut >= 0 {
		value = value[:cut]
	}
	return strings.TrimSpace(value)
}

// The UE asks for a long subscription and lets the registrar shorten it, the
// same way TS 24.229 5.1.1.4 has it ask for a long registration.
const regEventSubscribeExpiry = 600000

// subscribeRegEvent performs the subscription TS 24.229 subclause 5.1.1.3
// requires after a successful registration:
//
//	"Upon receipt of a 2xx response to the initial registration, the UE shall
//	subscribe to the reg event package for the public user identity registered
//	at the user's registrar (S-CSCF) as described in RFC 3680."
//
// It runs on the established protected transport and is deliberately
// best-effort: a registrar that refuses the subscription costs observability,
// not service, so the caller logs and carries on.
func (session *Session) subscribeRegEvent(ctx context.Context) error {
	callToken, err := randomHex(18)
	if err != nil {
		return err
	}
	branch, err := randomHex(12)
	if err != nil {
		return err
	}
	// A subscription is its own dialog and needs its own local tag; reusing the
	// registration's would put two dialogs on one tag.
	localTag, err := randomHex(8)
	if err != nil {
		return err
	}
	callID := callToken + "@" + addressHost(session.conn.LocalAddr())

	session.mu.Lock()
	if session.closed || session.closing {
		session.mu.Unlock()
		return ErrSessionClosed
	}
	if err := ctx.Err(); err != nil {
		session.mu.Unlock()
		return err
	}
	cseq := session.cseq
	session.cseq++
	serviceRoutes := append([]string(nil), session.evidence.ServiceRoute...)
	securityHeaders := runtimeSecurityHeaders(
		session.securityActive,
		session.securityAgreement.verifyValue,
	)
	session.mu.Unlock()

	profile := vowifi.ResolveCarrierProfile(session.request.Identity)
	contact := session.buildContact(session.contactAddress(), profile.IMSRegisterOptions)
	transportUpper := strings.ToUpper(session.transport)
	lines := []string{
		"SUBSCRIBE " + session.identity.public + " SIP/2.0",
		fmt.Sprintf("Via: SIP/2.0/%s %s;branch=z9hG4bK%s;rport", transportUpper, session.conn.LocalAddr().String(), branch),
		"Max-Forwards: 70",
	}
	lines = append(lines, securityHeaders...)
	if len(serviceRoutes) == 0 {
		lines = append(lines, "Route: <sip:"+session.endpoint.address()+";transport="+session.transport+";lr>")
	} else {
		for _, route := range serviceRoutes {
			lines = append(lines, "Route: "+route)
		}
	}
	lines = append(lines,
		"From: <"+session.identity.public+">;tag="+localTag,
		"To: <"+session.identity.public+">",
		"Call-ID: "+callID,
		fmt.Sprintf("CSeq: %d SUBSCRIBE", cseq),
		"Contact: "+contact,
		"Event: reg",
		"Accept: application/reginfo+xml",
		fmt.Sprintf("Expires: %d", regEventSubscribeExpiry),
	)
	if pani := session.pAccessNetworkInfo(); pani != "" {
		lines = append(lines, "P-Access-Network-Info: "+pani)
	}
	lines = append(lines,
		"User-Agent: "+session.imsUserAgent(),
		"Content-Length: 0",
		"", "",
	)
	request := []byte(strings.Join(lines, "\r\n"))

	response, err := session.exchangeRuntime(ctx, request, sipTransactionKey{
		callID: callID, cseq: cseq, method: "SUBSCRIBE",
	})
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("ims: reg event SUBSCRIBE rejected with %d", response.StatusCode)
	}
	session.mu.Lock()
	session.regEventSubscribed = true
	session.mu.Unlock()
	return nil
}

// startRegEventSubscription subscribes in the background. Readiness must not
// wait on it, and a failure must not take the line down.
func (session *Session) startRegEventSubscription() {
	if !session.provider.config.SubscribeRegistrationEvents {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := session.subscribeRegEvent(ctx); err != nil {
			session.provider.config.Logger.Info("IMS registration-state subscription unavailable",
				"device_id", session.request.DeviceID, "error", err)
			return
		}
		session.provider.config.Logger.Info("IMS subscribed to the registration-state event package",
			"device_id", session.request.DeviceID)
	}()
}

// handleRegEventNotify reacts to a reg event NOTIFY. TS 24.229 subclause
// 5.1.1.5A: a contact of ours reported terminated with event "deactivated"
// means re-register; the other terminated events mean the binding is simply
// gone. Anything else is left alone.
func (session *Session) handleRegEventNotify(request *sipRequest) {
	event := strings.TrimSpace(strings.SplitN(request.value("Event"), ";", 2)[0])
	if !strings.EqualFold(event, "reg") {
		return
	}
	session.mu.Lock()
	contact := session.evidence.RegisteredContact
	session.mu.Unlock()
	verdict := regInfoVerdictFor(request.Body, contact)
	if verdict == regEventNoChange && contact != "" {
		// A document that reports some binding as gone, but not one we
		// recognise as ours, is the shape a contact-matching bug would take.
		// Say so instead of silently ignoring a real deregistration.
		if regInfoVerdict(request.Body) != regEventNoChange {
			session.provider.config.Logger.Info("IMS registration-state NOTIFY reported a binding this session does not own",
				"device_id", session.request.DeviceID,
				"registered_contact", contact)
		}
	}
	level := slog.LevelInfo
	if verdict != regEventNoChange {
		level = slog.LevelWarn
	}
	session.provider.config.Logger.Log(context.Background(), level,
		"IMS registration-state NOTIFY",
		"device_id", session.request.DeviceID,
		"verdict", verdict.String(),
		"subscription_state", strings.TrimSpace(request.value("Subscription-State")),
		"body_bytes", len(request.Body))
	if verdict == regEventNoChange {
		return
	}
	// Re-register on the transport that is already up rather than tearing the
	// line down: the access layer is fine, it is the binding that went away.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := session.RefreshRegistration(ctx); err != nil {
			session.provider.config.Logger.Warn("IMS re-registration after a registration-state NOTIFY failed",
				"device_id", session.request.DeviceID, "verdict", verdict.String(), "error", err)
			return
		}
		session.provider.config.Logger.Info("IMS re-registered after a registration-state NOTIFY",
			"device_id", session.request.DeviceID, "verdict", verdict.String())
	}()
}
