package device

import "testing"

func TestFreshProfilesResponseValidation(t *testing.T) {
	real := tlv([]byte{0xbf, 0x2d}, tlv([]byte{0xa0}, esimTestProfile(t, "8944100000000000001", "carrier", "name", 0)))
	good := [][]byte{real, {0xbf, 0x2d, 2, 0xa0, 0}, {0xbf, 0x3d, 2, 0x30, 0}}
	for _, raw := range good {
		if _, err := parseProfilesInfoFresh(raw); err != nil {
			t.Fatalf("valid response rejected %x: %v", raw, err)
		}
	}
	bad := [][]byte{nil, {0xbf, 0x2d, 0}, real[:len(real)-1], append(append([]byte{}, real...), 0), {0xbf, 0x2d, 3, 0xa0, 2, 0xe3}, {0xbf, 0x2d, 3, 0x81, 1, 1}, {0xbf, 0x2d, 3, 2, 1, 1}, tlv([]byte{0xbf, 0x2d}, tlv([]byte{0xa0}, tlv([]byte{0xe3}, tlv([]byte{0x91}, []byte("no identity")))))}
	for _, raw := range bad {
		if rows, err := parseProfilesInfoFresh(raw); err == nil {
			t.Fatalf("bad response accepted %x: %+v", raw, rows)
		}
	}
}
