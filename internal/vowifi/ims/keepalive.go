package ims

import (
	"strconv"
	"strings"
)

// viaKeepSeconds reads the keep-alive period the registrar asked for out of the
// topmost Via of a REGISTER response.
//
// TS 24.229 subclause 5.1.1.2.1 d) has the UE offer a valueless "keep" Via
// parameter on its REGISTER; h) has it start sending keep-alives only once the
// answer carries a value. RFC 6223 defines the parameter, RFC 5626 defines what
// to then send. An echoed valueless "keep" is therefore not a request to send
// anything, and neither is a value we cannot use, so both report not-set.
//
// Only the topmost Via belongs to this UE; the ones below were added by proxies
// and their parameters are not addressed to us.
func viaKeepSeconds(values []string) (int, bool) {
	items := splitHeaderValues(values)
	if len(items) == 0 {
		return 0, false
	}
	raw := strings.TrimSpace(headerParameter(items[0], "keep"))
	if raw == "" {
		return 0, false
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0, false
	}
	return seconds, true
}

// observeRegistrarKeepAlive records whether this carrier's P-CSCF asks for SIP
// keep-alives, and logs the answer the first time it is seen and whenever it
// changes. Nothing sends keep-alives yet: on a VoWiFi bearer the UE address is
// assigned inside the ePDG tunnel, so the P-CSCF often sees no NAT and has no
// reason to ask (TS 24.229 Annex K.2.1.5 notes keep-alives may be undesirable
// then). Which way this carrier answers decides whether an RFC 5626 keep-alive
// is available to close the gap between registration refreshes at all, so it is
// worth one log line per session.
func (session *Session) observeRegistrarKeepAlive(response *sipResponse) {
	if response == nil {
		return
	}
	seconds, requested := viaKeepSeconds(response.values("Via"))
	if session.keepAliveObserved && seconds == session.registrarKeepSeconds {
		return
	}
	session.keepAliveObserved = true
	session.registrarKeepSeconds = seconds
	if requested {
		session.provider.config.Logger.Info("IMS registrar requested SIP keep-alives",
			"device_id", session.request.DeviceID,
			"keep_seconds", seconds)
		return
	}
	session.provider.config.Logger.Info("IMS registrar did not ask for SIP keep-alives",
		"device_id", session.request.DeviceID)
}
