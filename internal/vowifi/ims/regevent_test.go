package ims

import "testing"

// The registration-state event package (RFC 3680) is how the network tells a UE
// that its binding is gone without waiting for the next refresh. TS 24.229
// subclause 5.1.1.5A reacts to the state/event attributes, so the parser has to
// get those right on the shapes a real S-CSCF sends, and has to stay quiet on
// anything it does not understand rather than tearing a working line down.
func TestParseRegInfo(t *testing.T) {
	active := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<reginfo xmlns="urn:ietf:params:xml:ns:reginfo" version="1" state="full">
  <registration aor="sip:+15550100042@ims.mnc240.mcc310.3gppnetwork.org" id="a7" state="active">
    <contact id="76" state="active" event="registered">
      <uri>sip:+15550100042@10.20.30.40:5060</uri>
    </contact>
  </registration>
</reginfo>`)

	doc, err := parseRegInfo(active)
	if err != nil {
		t.Fatalf("parse active: %v", err)
	}
	if len(doc) != 1 {
		t.Fatalf("registrations = %d, want 1", len(doc))
	}
	if doc[0].State != "active" {
		t.Fatalf("registration state = %q", doc[0].State)
	}
	if len(doc[0].Contacts) != 1 {
		t.Fatalf("contacts = %d, want 1", len(doc[0].Contacts))
	}
	if doc[0].Contacts[0].Event != "registered" || doc[0].Contacts[0].State != "active" {
		t.Fatalf("contact = %+v", doc[0].Contacts[0])
	}
	if doc[0].Contacts[0].URI != "sip:+15550100042@10.20.30.40:5060" {
		t.Fatalf("contact uri = %q", doc[0].Contacts[0].URI)
	}
}

func TestRegInfoVerdict(t *testing.T) {
	body := func(registrationState, contactState, event string) []byte {
		return []byte(`<?xml version="1.0"?>
<reginfo xmlns="urn:ietf:params:xml:ns:reginfo" version="2" state="full">
  <registration aor="sip:a@example.com" id="a7" state="` + registrationState + `">
    <contact id="76" state="` + contactState + `" event="` + event + `">
      <uri>sip:a@10.0.0.1</uri>
    </contact>
  </registration>
</reginfo>`)
	}

	for _, testCase := range []struct {
		name string
		body []byte
		want regEventVerdict
	}{
		{
			name: "still registered",
			body: body("active", "active", "registered"),
			want: regEventNoChange,
		},
		{
			name: "refreshed binding",
			body: body("active", "active", "refreshed"),
			want: regEventNoChange,
		},
		{
			// 24.229 5.1.1.5A: deactivated means re-register, the subscription
			// is not over because the subscriber did something wrong.
			name: "deactivated asks for a fresh registration",
			body: body("terminated", "terminated", "deactivated"),
			want: regEventReRegister,
		},
		{
			name: "unregistered is a real deregistration",
			body: body("terminated", "terminated", "unregistered"),
			want: regEventDeregistered,
		},
		{
			name: "rejected is a real deregistration",
			body: body("terminated", "terminated", "rejected"),
			want: regEventDeregistered,
		},
		{
			// A contact going away while the registration as a whole stays
			// active is still this UE losing its binding.
			name: "our contact expired under an active registration",
			body: body("active", "terminated", "expired"),
			want: regEventDeregistered,
		},
		{
			name: "unparseable body changes nothing",
			body: []byte("not xml at all"),
			want: regEventNoChange,
		},
		{
			name: "empty body changes nothing",
			body: nil,
			want: regEventNoChange,
		},
		{
			name: "unknown event on a terminated contact still means gone",
			body: body("terminated", "terminated", "probation"),
			want: regEventDeregistered,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := regInfoVerdict(testCase.body); got != testCase.want {
				t.Fatalf("regInfoVerdict = %v, want %v", got, testCase.want)
			}
		})
	}
}

// A NOTIFY that reports some other subscriber's binding must never take this
// line down. The S-CSCF sends one document per subscription, but a shared
// dialog or a stray body is exactly the kind of thing that turns a monitoring
// feature into an outage.
func TestRegInfoVerdictIgnoresPartialStateForOtherContacts(t *testing.T) {
	partial := []byte(`<?xml version="1.0"?>
<reginfo xmlns="urn:ietf:params:xml:ns:reginfo" version="3" state="partial">
  <registration aor="sip:someone-else@example.com" id="b9" state="terminated">
    <contact id="11" state="terminated" event="unregistered">
      <uri>sip:someone-else@10.0.0.9</uri>
    </contact>
  </registration>
</reginfo>`)
	if got := regInfoVerdictFor(partial, "sip:a@10.0.0.1"); got != regEventNoChange {
		t.Fatalf("verdict for another contact = %v, want no change", got)
	}

	ours := []byte(`<?xml version="1.0"?>
<reginfo xmlns="urn:ietf:params:xml:ns:reginfo" version="3" state="partial">
  <registration aor="sip:a@example.com" id="b9" state="terminated">
    <contact id="11" state="terminated" event="deactivated">
      <uri>sip:a@10.0.0.1</uri>
    </contact>
  </registration>
</reginfo>`)
	if got := regInfoVerdictFor(ours, "sip:a@10.0.0.1"); got != regEventReRegister {
		t.Fatalf("verdict for our contact = %v, want re-register", got)
	}
}
