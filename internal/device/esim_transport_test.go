package device

import (
	"testing"
	"vocat/internal/modem"
	"vocat/internal/pcsc"
)

func TestESIMUsesATFollowsActualHardwareTransport(t *testing.T) {
	for _, test := range []struct {
		name      string
		candidate modem.Candidate
		transport string
		want      bool
	}{
		{name: "usb EC20 marked QMI still uses AT", candidate: modem.Candidate{ID: "4-2", QMIControl: "/dev/cdc-wdm0"}, transport: "qmi", want: true},
		{name: "native QMI uses UIM", candidate: modem.Candidate{ID: "wwan0", QMIControl: "/dev/wwan0qmi0"}, transport: "qmi", want: false},
		{name: "native explicit AT override", candidate: modem.Candidate{ID: "wwan0", QMIControl: "/dev/wwan0qmi0"}, transport: "at", want: true},
		{name: "PCSC is not AT", candidate: modem.Candidate{ID: "reader", HardwareKind: pcsc.HardwareKind}, transport: "qmi", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &managedDevice{candidate: test.candidate, discovered: true, backend: "qmi", esimTransport: test.transport}
			manager := &Manager{started: true, devices: map[string]*managedDevice{test.candidate.ID: state}}
			inspector, ok := any(manager).(interface{ ESIMUsesAT(string) (bool, error) })
			if !ok {
				t.Fatal("missing actual eSIM transport query")
			}
			got, err := inspector.ESIMUsesAT(test.candidate.ID)
			if err != nil || got != test.want {
				t.Fatalf("ESIMUsesAT = %v, %v; want %v", got, err, test.want)
			}
		})
	}
}
