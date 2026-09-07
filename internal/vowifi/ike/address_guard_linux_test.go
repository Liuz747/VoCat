//go:build linux

package ike

import (
	"errors"
	"net"
	"testing"
)

func TestInnerAddressReservationPreventsCrossLineCollision(t *testing.T) {
	config := ChildSAConfig{Name: "first", InnerLocalIPv4: net.ParseIP("192.0.2.187")}
	release, e := reserveInnerAddresses(config)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	config.Name = "second"
	if other, e := reserveInnerAddresses(config); !errors.Is(e, ErrInnerAddressConflict) {
		if other != nil {
			other()
		}
		t.Fatalf("concurrent same address accepted: %v", e)
	}
	release()
	other, e := reserveInnerAddresses(config)
	if e != nil {
		t.Fatalf("reservation leaked after release: %v", e)
	}
	other()
}
func TestInnerAddressRejectsExistingHostAddress(t *testing.T) {
	release, e := reserveInnerAddresses(ChildSAConfig{Name: "new", InnerLocalIPv4: net.ParseIP("127.0.0.1")})
	if release != nil {
		release()
	}
	if !errors.Is(e, ErrInnerAddressConflict) {
		t.Fatalf("existing host address was accepted: %v", e)
	}
}
