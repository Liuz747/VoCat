package vowifi

import (
	"path/filepath"
	"testing"
)

// The deploy/ sample must stay loadable by LoadCarrierProfileDirectory and
// must shape a T-Mobile US registration like a handset would.
func TestDeployTMobileIPhoneLikeProfileLoads(t *testing.T) {
	dir := filepath.Join("..", "..", "deploy", "carrier-profiles.d")
	if err := LoadCarrierProfileDirectory(dir); err != nil {
		t.Fatalf("LoadCarrierProfileDirectory(%q) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = LoadCarrierProfileDirectory(filepath.Join(t.TempDir(), "none")) })

	profile := ResolveCarrierProfile(SIMIdentity{IMSI: "310240528577885", HomeMCC: "310", HomeMNC: "240", ICCID: "8901240527185778852"})
	if profile.ID != "tmobile-us-iphone-like" {
		t.Fatalf("resolved profile = %q, want tmobile-us-iphone-like", profile.ID)
	}
	if profile.IMSUserAgent != "iOS/18.2.1 iPhone (iPhone15,4)" {
		t.Fatalf("IMSUserAgent = %q", profile.IMSUserAgent)
	}
	opts := profile.IMSRegisterOptions
	if !opts.PPreferredIdentity {
		t.Fatalf("P-Preferred-Identity should be enabled")
	}
	if opts.SupportedHeader == nil || *opts.SupportedHeader != "path, gruu, sec-agree, outbound" {
		t.Fatalf("Supported = %v", opts.SupportedHeader)
	}
	if len(opts.ContactExtraTags) != 4 || opts.ContactExtraTags[2] != "+g.3gpp.srvcc-alerting" {
		t.Fatalf("ContactExtraTags = %v", opts.ContactExtraTags)
	}
	if len(opts.AcceptContactTags) != 2 {
		t.Fatalf("AcceptContactTags = %v", opts.AcceptContactTags)
	}
}
