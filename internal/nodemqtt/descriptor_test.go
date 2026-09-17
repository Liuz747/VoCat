package nodemqtt

import (
	"testing"
	"unicode/utf8"

	"vocat/internal/buildinfo"
)

func TestDescriptorVersionFitsPlatformWithoutLosingBuildIdentity(t *testing.T) {
	previous := buildinfo.Version
	t.Cleanup(func() { buildinfo.Version = previous })
	d := Descriptor{NodeType: "ec20_edge", Capabilities: []string{"phones.list"}}
	buildinfo.Version = "0.1.0-multitunnel.aaed64cbcbd5-mqtt-inventory"
	first := d.Payload(131072, 0)
	version := first["software_version"].(string)
	if utf8.RuneCountInString(version) > 40 || first["software_build"] != buildinfo.Version {
		t.Fatalf("platform version too long or full build lost: %#v", first)
	}
	buildinfo.Version += "-different"
	if d.Payload(131072, 0)["software_version"] == version {
		t.Fatal("different builds became indistinguishable")
	}
	buildinfo.Version = "0.1.0+abcdef123456"
	if d.Payload(131072, 0)["software_version"] != buildinfo.Version {
		t.Fatal("short version changed")
	}
}
