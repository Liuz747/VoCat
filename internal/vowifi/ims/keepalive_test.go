package ims

import "testing"

// TS 24.229 subclause 5.1.1.2.1 d) has the UE advertise that it is willing to
// send keep-alives by putting a valueless "keep" parameter in the topmost Via
// of its REGISTER. Subclause 5.1.1.2.1 h) then has it start sending them only
// when the registrar answers with a value. Parsing that answer is the cheap
// experiment that tells us whether RFC 5626 keep-alives are available on this
// carrier at all, so it has to survive the shapes a P-CSCF may send.
func TestViaKeepSeconds(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		values  []string
		want    int
		wantSet bool
	}{
		{
			name:    "registrar asks for a period",
			values:  []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;rport=5060;keep=30"},
			want:    30,
			wantSet: true,
		},
		{
			name:    "quoted value",
			values:  []string{`SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;keep="120"`},
			want:    120,
			wantSet: true,
		},
		{
			name:    "parameter order does not matter",
			values:  []string{"SIP/2.0/TCP 10.0.0.1:5060;keep=45;branch=z9hG4bKabc"},
			want:    45,
			wantSet: true,
		},
		{
			name: "only the topmost Via counts",
			values: []string{
				"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc",
				"SIP/2.0/TCP 10.0.0.2:5060;branch=z9hG4bKdef;keep=30",
			},
			wantSet: false,
		},
		{
			name:   "echoed valueless keep is not a request to send",
			values: []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;keep"},
			// The UE offered "keep" with no value; an echo carries no period
			// and per 5.1.1.2.1 h) does not start the keep-alives.
			wantSet: false,
		},
		{
			name:   "no keep parameter at all",
			values: []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;rport=5060"},
		},
		{
			name:   "keepalive is a different parameter",
			values: []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;keepalive=30"},
		},
		{
			name:   "non-numeric value is not a period",
			values: []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;keep=yes"},
		},
		{
			name:   "zero is not a usable period",
			values: []string{"SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bKabc;keep=0"},
		},
		{
			name: "no Via at all",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			seconds, ok := viaKeepSeconds(testCase.values)
			if ok != testCase.wantSet {
				t.Fatalf("viaKeepSeconds set = %v, want %v (value %d)", ok, testCase.wantSet, seconds)
			}
			if ok && seconds != testCase.want {
				t.Fatalf("viaKeepSeconds = %d, want %d", seconds, testCase.want)
			}
		})
	}
}
