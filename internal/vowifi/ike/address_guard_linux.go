//go:build linux

package ike

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

var ErrInnerAddressConflict = errors.New("ike: inner address already belongs to another session or host interface")
var addressReservations = struct {
	sync.Mutex
	ips map[string]bool
}{ips: make(map[string]bool)}

// Source policy routing cannot distinguish two lines with the same inner IP.
// Reserve addresses before creating any network objects. The process-local
// reservation also closes the race between two concurrent installations.
// A separately running process is detected through its assigned addresses;
// installation across unrelated processes is not an atomic host-wide lease.
func reserveInnerAddresses(config ChildSAConfig) (func(), error) {
	wanted := make(map[string]bool)
	for _, ip := range []net.IP{config.InnerLocalIPv4, config.InnerLocalIPv6} {
		if ip != nil {
			wanted[ip.String()] = true
		}
	}
	addressReservations.Lock()
	defer addressReservations.Unlock()
	for ip := range wanted {
		if addressReservations.ips[ip] {
			return nil, ErrInnerAddressConflict
		}
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("ike: inspect interface addresses: %w", err)
	}
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("ike: inspect interface addresses: %w", err)
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && wanted[ip.String()] {
				return nil, ErrInnerAddressConflict
			}
		}
	}
	for ip := range wanted {
		addressReservations.ips[ip] = true
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			addressReservations.Lock()
			defer addressReservations.Unlock()
			for ip := range wanted {
				delete(addressReservations.ips, ip)
			}
		})
	}, nil
}
