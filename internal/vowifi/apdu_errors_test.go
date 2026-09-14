package vowifi

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"vocat/internal/modem"
)

func TestSafeAPDUDetailKeepsOnlyStatusAndErrorCodes(t *testing.T) {
	t.Parallel()
	const command = `AT+CSIM=76,"00880081221000010203040506070809"`
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "mac_failure", err: ErrEC20AKAMACFailure, want: "aka_mac_failure"},
		{name: "sim_not_ready", err: fmt.Errorf("check: %w", ErrEC20SIMNotReady), want: "sim_not_ready"},
		{name: "identity_changed", err: ErrEC20IdentityChanged, want: "identity_changed"},
		{name: "status_word", err: fmt.Errorf("%w: %w", ErrEC20AKAResponse, &APDUStatusError{SW: 0x6985}), want: "apdu_sw_6985"},
		{name: "cme_code", err: fmt.Errorf("wrapped: %w", &modem.CommandError{Command: command, Final: "+CME ERROR: 14"}), want: "cme_error_14"},
		{name: "cms_code", err: &modem.CommandError{Command: command, Final: "+CMS ERROR: 302"}, want: "cms_error_302"},
		{name: "plain_error", err: &modem.CommandError{Command: command, Final: "ERROR"}, want: "at_error"},
		{name: "verbose_cme", err: &modem.CommandError{Command: command, Final: "+CME ERROR: SIM busy"}, want: "cme_error_text"},
		{name: "command_timeout", err: fmt.Errorf("%w: %s", modem.ErrCommandTimeout, command), want: "command_timeout"},
		{name: "unclassified", err: errors.New(command), want: ""},
	} {
		if got := SafeAPDUDetail(test.err); got != test.want {
			t.Errorf("%s: SafeAPDUDetail = %q, want %q", test.name, got, test.want)
		}
	}
}

func TestEC20BasicChannelAKAKeepsOnlySafeFailureDetail(t *testing.T) {
	t.Parallel()
	var challenge AKAChallenge
	for index := range challenge.RAND {
		challenge.RAND[index] = byte(0xa0 + index)
		challenge.AUTN[index] = byte(0x50 + index)
	}
	authAPDU := buildUSIMAuthenticateAPDU(challenge)
	authHex := strings.ToUpper(hex.EncodeToString(authAPDU))
	authCommand := fmt.Sprintf(`AT+CSIM=%d,"%s"`, len(authAPDU)*2, authHex)
	selectApplication := `AT+CSIM=24,"00A4040407A0000000871002"`
	selected := ec20TranscriptStep{command: selectApplication, lines: []string{`+CSIM: 4,"9000"`}}

	for _, test := range []struct {
		name    string
		steps   []ec20TranscriptStep
		wantErr error
		detail  string
	}{
		{
			name: "authenticate_cme_error",
			steps: []ec20TranscriptStep{selected, {
				command: authCommand, sensitive: true, final: "+CME ERROR: 14",
				err: &modem.CommandError{Command: authCommand, Final: "+CME ERROR: 14"},
			}},
			wantErr: ErrEC20AKACommand,
			detail:  "cme_error_14",
		},
		{
			name: "authenticate_command_timeout",
			steps: []ec20TranscriptStep{selected, {
				command: authCommand, sensitive: true,
				err: fmt.Errorf("%w: %s", modem.ErrCommandTimeout, authCommand),
			}},
			wantErr: ErrEC20AKACommand,
			detail:  "command_timeout",
		},
		{
			name: "authenticate_status_word",
			steps: []ec20TranscriptStep{selected, {
				command: authCommand, sensitive: true, lines: []string{`+CSIM: 4,"6985"`},
			}},
			wantErr: ErrEC20AKAResponse,
			detail:  "apdu_sw_6985",
		},
		{
			name:   "select_status_word",
			steps:  []ec20TranscriptStep{{command: selectApplication, lines: []string{`+CSIM: 4,"6985"`}}},
			detail: "apdu_sw_6985",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			steps := append(identityTranscriptSteps("234150123456789"),
				ec20TranscriptStep{command: "AT+CCID", lines: []string{"+CCID: 8944101234567890123"}},
				ec20TranscriptStep{command: "AT+CUAD", lines: []string{`+CUAD: 22,"61094F07A0000000871002"`}},
				ec20TranscriptStep{command: `AT+CCHO="A0000000871002"`, err: errors.New("unsupported"), final: "ERROR"},
				selected,
				ec20TranscriptStep{command: "AT+CCID", lines: []string{"+CCID: 8944101234567890123"}},
			)
			transcript := &ec20Transcript{t: t, steps: append(steps, test.steps...)}
			adapter, err := NewEC20Adapter(transcript, EC20AdapterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			identity, err := adapter.ReadIdentity(context.Background(), "ec20-1")
			if err != nil {
				t.Fatalf("ReadIdentity: %v", err)
			}
			if _, err := adapter.CheckReady(context.Background(), identity); err != nil {
				t.Fatalf("CheckReady: %v", err)
			}
			_, err = adapter.Authenticate(context.Background(), identity, challenge)
			if err == nil {
				t.Fatal("Authenticate succeeded")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Authenticate error = %v, want %v", err, test.wantErr)
			}
			if got := SafeAPDUDetail(err); got != test.detail {
				t.Fatalf("detail = %q, want %q (error %v)", got, test.detail, err)
			}
			if message := err.Error(); strings.Contains(message, authHex[10:42]) || strings.Contains(message, "AT+CSIM") {
				t.Fatalf("error text carries APDU material: %q", message)
			}
			transcript.assertDone()
		})
	}
}
