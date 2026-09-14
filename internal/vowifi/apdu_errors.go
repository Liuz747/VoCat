package vowifi

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"vocat/internal/modem"
)

var errEC20CSIMExchange = errors.New("vocat: EC20 CSIM exchange failed")

// APDUStatusError carries only the status word of an APDU the card rejected.
type APDUStatusError struct{ SW uint16 }

func (e *APDUStatusError) Error() string { return fmt.Sprintf("status word %04X", e.SW) }

// ModemFinalError keeps only the final result code of a failed AT command. The
// command and its response lines can carry APDU material and are dropped.
type ModemFinalError struct{ Final string }

func (e *ModemFinalError) Error() string { return "modem returned " + e.Final }

var modemErrorCode = regexp.MustCompile(`^\+(CME|CMS) ERROR:\s*(\d{1,5})$`)

// newModemFinalError normalizes a final result line to a code-only form.
func newModemFinalError(final string) *ModemFinalError {
	final = strings.ToUpper(strings.TrimSpace(final))
	if match := modemErrorCode.FindStringSubmatch(final); match != nil {
		return &ModemFinalError{Final: "+" + match[1] + " ERROR: " + match[2]}
	}
	switch {
	case final == "ERROR":
		return &ModemFinalError{Final: "ERROR"}
	case strings.HasPrefix(final, "+CME ERROR"), strings.HasPrefix(final, "+CMS ERROR"):
		// Verbose error text is not needed to classify the failure.
		return &ModemFinalError{Final: final[:len("+CME ERROR")]}
	default:
		return &ModemFinalError{Final: "unrecognized final result"}
	}
}

func (e *ModemFinalError) class() string {
	if match := modemErrorCode.FindStringSubmatch(e.Final); match != nil {
		return strings.ToLower(match[1]) + "_error_" + match[2]
	}
	switch e.Final {
	case "ERROR":
		return "at_error"
	case "+CME ERROR", "+CMS ERROR":
		return strings.ToLower(e.Final[1:4]) + "_error_text"
	}
	return "at_final_other"
}

// safeAPDUCause reduces an APDU exchange failure to the parts that are safe to
// keep in an error chain: a status word, an AT final result code, a command
// timeout, or a context error. Command text and response data never survive.
func safeAPDUCause(err error) error {
	var status *APDUStatusError
	var final *ModemFinalError
	var command *modem.CommandError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &status):
		return status
	case errors.As(err, &final):
		return final
	case errors.As(err, &command):
		return newModemFinalError(command.Final)
	case errors.Is(err, modem.ErrCommandTimeout):
		return modem.ErrCommandTimeout
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	}
	return nil
}

func withSafeAPDUCause(base, err error) error {
	if cause := safeAPDUCause(err); cause != nil {
		return fmt.Errorf("%w: %w", base, cause)
	}
	return base
}

// SafeAPDUDetail names a SIM or modem failure for logs using only a status
// word or an AT error code. It returns "" when the error carries no such
// evidence.
func SafeAPDUDetail(err error) string {
	var status *APDUStatusError
	var final *ModemFinalError
	var command *modem.CommandError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrEC20AKAMACFailure):
		return "aka_mac_failure"
	case errors.Is(err, ErrEC20SIMNotReady):
		return "sim_not_ready"
	case errors.Is(err, ErrEC20IdentityChanged):
		return "identity_changed"
	case errors.As(err, &status):
		return fmt.Sprintf("apdu_sw_%04x", status.SW)
	case errors.As(err, &final):
		return final.class()
	case errors.As(err, &command):
		return newModemFinalError(command.Final).class()
	case errors.Is(err, modem.ErrCommandTimeout):
		return "command_timeout"
	}
	return ""
}
