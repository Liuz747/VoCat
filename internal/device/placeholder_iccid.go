package device

import "strings"

// PlaceholderICCID is the identity a blank consumer eUICC reports through
// EF_ICCID while no profile is enabled. It is not a subscription: its
// MCC/MNC (111/11) map to no carrier, so any single-line VoWiFi attempt on it
// only loops on ePDG resolution. Observed on the 49 blank EC20 eUICCs during the
// 2026-09-14 expansion.
const PlaceholderICCID = "89111111111111111111"

// IsPlaceholderICCID reports whether a live ICCID is the blank-eUICC
// placeholder identity rather than a real profile.
func IsPlaceholderICCID(iccid string) bool {
	clean := strings.TrimRight(strings.TrimSpace(iccid), "fF")
	return clean == PlaceholderICCID
}
