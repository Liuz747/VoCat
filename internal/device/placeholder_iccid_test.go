package device

import "testing"

func TestIsPlaceholderICCID(t *testing.T) {
	cases := map[string]bool{
		"89111111111111111111":   true,
		" 89111111111111111111 ": true,
		"89111111111111111111F":  true,
		"89111111111111111111f":  true,
		"89860012345678901234":   false,
		"89012802332235639156":   false,
		"":                       false,
		"8911":                   false,
		"891111111111111111112":  false,
	}
	for input, want := range cases {
		if got := IsPlaceholderICCID(input); got != want {
			t.Errorf("IsPlaceholderICCID(%q) = %v, want %v", input, got, want)
		}
	}
}
