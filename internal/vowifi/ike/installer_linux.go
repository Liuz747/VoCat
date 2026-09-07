//go:build linux

package ike

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"vocat/internal/vowifi"
)

type linuxXFRMInstaller struct {
	ipCommand string
}

func defaultChildSAInstaller() ChildSAInstaller {
	return linuxChildSAInstallerRouter{ipCommand: "ip"}
}

type linuxChildSAInstallerRouter struct {
	ipCommand string
}

func (router linuxChildSAInstallerRouter) Install(ctx context.Context, config ChildSAConfig) (ChildSAHandle, error) {
	if config.ProxyMode == vowifi.ProxyModeSOCKS5 || config.UDPEncapsulation {
		return (linuxUserspaceInstaller{ipCommand: router.ipCommand}).Install(ctx, config)
	}
	return (linuxXFRMInstaller{ipCommand: router.ipCommand}).Install(ctx, config)
}

type linuxXFRMHandle struct {
	mu               sync.Mutex
	ipCommand        string
	config           ChildSAConfig
	reqid            string
	closed           bool
	releaseAddresses func()
	cleanup          []xfrmCleanup
}

// Cleanup records are added only after the corresponding create succeeds.
// A failed add may mean the object belongs to a different live session.
type xfrmCleanup struct {
	operation string
	arguments []string
}

func (*linuxXFRMHandle) DataplaneMode() string { return "xfrm" }

func (installer linuxXFRMInstaller) Install(ctx context.Context, config ChildSAConfig) (ChildSAHandle, error) {
	if config.ProxyMode == vowifi.ProxyModeSOCKS5 || config.UDPEncapsulation {
		return nil, errors.New("NAT-T and SOCKS5 require a user-space ESP/TUN installer using NATTPacketRelay; kernel XFRM cannot own the user-space UDP association")
	}
	if config.OuterLocal == nil || config.OuterRemote == nil {
		return nil, errors.New("outer IP addresses are required")
	}
	if config.InboundSPI == 0 || config.OutboundSPI == 0 {
		return nil, errors.New("ESP SPIs must be nonzero")
	}
	command := installer.ipCommand
	if command == "" {
		command = "ip"
	}
	if _, err := exec.LookPath(command); err != nil {
		return nil, errors.New("Linux iproute2 is required to install the CHILD_SA")
	}
	releaseAddresses, err := reserveInnerAddresses(config)
	if err != nil {
		return nil, err
	}
	handle := &linuxXFRMHandle{
		releaseAddresses: releaseAddresses,
		ipCommand:        command,
		config:           cloneChildSAConfig(config),
		reqid:            strconv.FormatUint(uint64(config.InboundSPI), 10),
	}
	if err := handle.install(ctx); err != nil {
		_ = handle.Close(context.Background())
		return nil, err
	}
	return handle, nil
}

func (handle *linuxXFRMHandle) install(ctx context.Context) error {
	config := handle.config
	if err := handle.run(ctx, "create tunnel interface", "link", "add", config.Name, "type", "dummy"); err != nil {
		return err
	}
	handle.cleanup = append(handle.cleanup, xfrmCleanup{
		operation: "delete tunnel interface", arguments: []string{"link", "delete", config.Name},
	})
	if config.InnerLocalIPv4 != nil {
		if err := handle.run(ctx, "assign tunnel IPv4 address", "address", "add", config.InnerLocalIPv4.String()+"/32", "dev", config.Name); err != nil {
			return err
		}
	}
	if config.InnerLocalIPv6 != nil {
		if err := handle.run(ctx, "assign tunnel IPv6 address", "-6", "address", "add", fmt.Sprintf("%s/%d", config.InnerLocalIPv6.String(), config.InnerIPv6Prefix), "dev", config.Name); err != nil {
			return err
		}
	}
	if err := handle.run(ctx, "enable tunnel interface", "link", "set", "dev", config.Name, "up"); err != nil {
		return err
	}
	outboundState := handle.stateArguments(
		config.OuterLocal, config.OuterRemote, config.OutboundSPI,
		config.OutboundEncKey, config.OutboundAuthKey,
	)
	if err := handle.run(ctx, "install outbound ESP state", append([]string{"xfrm", "state", "add"}, outboundState...)...); err != nil {
		return err
	}
	handle.cleanup = append(handle.cleanup, xfrmCleanup{
		operation: "delete outbound ESP state",
		arguments: []string{"xfrm", "state", "delete", "src", config.OuterLocal.String(), "dst", config.OuterRemote.String(), "proto", "esp", "spi", fmt.Sprintf("0x%08x", config.OutboundSPI)},
	})
	inboundState := handle.stateArguments(
		config.OuterRemote, config.OuterLocal, config.InboundSPI,
		config.InboundEncKey, config.InboundAuthKey,
	)
	if err := handle.run(ctx, "install inbound ESP state", append([]string{"xfrm", "state", "add"}, inboundState...)...); err != nil {
		return err
	}
	handle.cleanup = append(handle.cleanup, xfrmCleanup{
		operation: "delete inbound ESP state",
		arguments: []string{"xfrm", "state", "delete", "src", config.OuterRemote.String(), "dst", config.OuterLocal.String(), "proto", "esp", "spi", fmt.Sprintf("0x%08x", config.InboundSPI)},
	})
	for _, initiator := range config.InitiatorSelectors {
		for _, responder := range config.ResponderSelectors {
			if (initiator.StartIP.To4() == nil) != (responder.StartIP.To4() == nil) {
				continue
			}
			if err := handle.installPolicyPair(ctx, initiator, responder); err != nil {
				return err
			}
		}
	}
	return nil
}

func (handle *linuxXFRMHandle) installPolicyPair(
	ctx context.Context,
	initiator trafficSelector,
	responder trafficSelector,
) error {
	initiatorPrefix, err := selectorPrefix(initiator)
	if err != nil {
		return err
	}
	responderPrefix, err := selectorPrefix(responder)
	if err != nil {
		return err
	}
	if initiator.IPProtocol != responder.IPProtocol &&
		initiator.IPProtocol != 0 && responder.IPProtocol != 0 {
		return errors.New("negotiated traffic selectors use conflicting IP protocols")
	}
	protocol := initiator.IPProtocol
	if protocol == 0 {
		protocol = responder.IPProtocol
	}
	family := "-4"
	if initiator.StartIP.To4() == nil {
		family = "-6"
	}
	outbound := []string{
		family, "xfrm", "policy", "add",
		"src", initiatorPrefix, "dst", responderPrefix, "dir", "out",
	}
	inbound := []string{
		family, "xfrm", "policy", "add",
		"src", responderPrefix, "dst", initiatorPrefix, "dir", "in",
	}
	if protocol != 0 {
		outbound = append(outbound, "proto", strconv.Itoa(int(protocol)))
		inbound = append(inbound, "proto", strconv.Itoa(int(protocol)))
	}
	outbound, err = appendSelectorPorts(outbound, initiator, responder)
	if err != nil {
		return err
	}
	inbound, err = appendSelectorPorts(inbound, responder, initiator)
	if err != nil {
		return err
	}
	// Policy deletion identifies the exact selector, including protocol and
	// ports, but takes no transform template. Keep the installed selector instead
	// of reconstructing a broader one from the desired config during Close.
	outboundDelete := append([]string(nil), outbound...)
	inboundDelete := append([]string(nil), inbound...)
	outboundDelete[3] = "delete"
	inboundDelete[3] = "delete"
	outbound = append(outbound,
		"tmpl", "src", handle.config.OuterLocal.String(), "dst", handle.config.OuterRemote.String(),
		"proto", "esp", "mode", "tunnel", "reqid", handle.reqid,
	)
	inbound = append(inbound,
		"tmpl", "src", handle.config.OuterRemote.String(), "dst", handle.config.OuterLocal.String(),
		"proto", "esp", "mode", "tunnel", "reqid", handle.reqid,
	)
	if err := handle.run(ctx, "install outbound ESP policy", outbound...); err != nil {
		return err
	}
	handle.cleanup = append(handle.cleanup, xfrmCleanup{operation: "delete outbound ESP policy", arguments: outboundDelete})
	if err := handle.run(ctx, "install inbound ESP policy", inbound...); err != nil {
		return err
	}
	handle.cleanup = append(handle.cleanup, xfrmCleanup{operation: "delete inbound ESP policy", arguments: inboundDelete})
	return nil
}

func appendSelectorPorts(
	arguments []string,
	source trafficSelector,
	destination trafficSelector,
) ([]string, error) {
	appendPort := func(label string, start uint16, end uint16) error {
		if start == 0 && end == 65535 {
			return nil
		}
		if start != end {
			return fmt.Errorf("negotiated %s port range %d-%d cannot be represented safely by XFRM", label, start, end)
		}
		arguments = append(arguments, label, strconv.Itoa(int(start)))
		return nil
	}
	if err := appendPort("sport", source.StartPort, source.EndPort); err != nil {
		return nil, err
	}
	if err := appendPort("dport", destination.StartPort, destination.EndPort); err != nil {
		return nil, err
	}
	return arguments, nil
}

func selectorPrefix(selector trafficSelector) (string, error) {
	start := selector.StartIP
	end := selector.EndIP
	bits := 128
	if start4 := start.To4(); start4 != nil {
		start = start4
		end = end.To4()
		bits = 32
	} else {
		start = start.To16()
		end = end.To16()
	}
	if start == nil || end == nil || len(start) != len(end) {
		return "", errors.New("negotiated traffic selector IP range is invalid")
	}
	prefix := 0
	different := false
	for index := 0; index < len(start); index++ {
		for bit := 7; bit >= 0; bit-- {
			startBit := start[index] & (1 << bit)
			endBit := end[index] & (1 << bit)
			if !different && startBit == endBit {
				prefix++
				continue
			}
			different = true
			if startBit != 0 || endBit == 0 {
				return "", errors.New("negotiated traffic selector range is not a CIDR prefix")
			}
		}
	}
	network := &net.IPNet{IP: start, Mask: net.CIDRMask(prefix, bits)}
	return network.String(), nil
}

func (handle *linuxXFRMHandle) stateArguments(
	source net.IP,
	destination net.IP,
	spi uint32,
	encryptionKey []byte,
	integrityKey []byte,
) []string {
	arguments := []string{
		"src", source.String(),
		"dst", destination.String(),
		"proto", "esp",
		"spi", fmt.Sprintf("0x%08x", spi),
		"reqid", handle.reqid,
		"mode", "tunnel",
	}
	switch handle.config.Integrity {
	case "hmac-sha1-96":
		arguments = append(arguments, "auth-trunc", "hmac(sha1)", "0x"+hex.EncodeToString(integrityKey), "96")
	case "hmac-sha2-256-128":
		arguments = append(arguments, "auth-trunc", "hmac(sha256)", "0x"+hex.EncodeToString(integrityKey), "128")
	}
	arguments = append(arguments, "enc", "cbc(aes)", "0x"+hex.EncodeToString(encryptionKey))
	if handle.config.UDPEncapsulation {
		arguments = append(arguments, "encap", "espinudp", "4500", "4500", "0.0.0.0")
	}
	return arguments
}

func (handle *linuxXFRMHandle) run(ctx context.Context, operation string, arguments ...string) error {
	command := exec.CommandContext(ctx, handle.ipCommand, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("%s: %s", operation, message)
	}
	return nil
}

func (handle *linuxXFRMHandle) Close(ctx context.Context) error {
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return nil
	}
	handle.closed = true
	if handle.releaseAddresses != nil {
		defer handle.releaseAddresses()
	}
	var errs []error
	for index := len(handle.cleanup) - 1; index >= 0; index-- {
		cleanup := handle.cleanup[index]
		if err := handle.run(ctx, cleanup.operation, cleanup.arguments...); err != nil {
			errs = append(errs, err)
		}
	}
	handle.cleanup = nil
	return errors.Join(errs...)
}

func cloneChildSAConfig(config ChildSAConfig) ChildSAConfig {
	config.OuterLocal = append(net.IP(nil), config.OuterLocal...)
	config.OuterRemote = append(net.IP(nil), config.OuterRemote...)
	config.InnerLocalIPv4 = append(net.IP(nil), config.InnerLocalIPv4...)
	config.InnerLocalIPv6 = append(net.IP(nil), config.InnerLocalIPv6...)
	config.InboundEncKey = append([]byte(nil), config.InboundEncKey...)
	config.InboundAuthKey = append([]byte(nil), config.InboundAuthKey...)
	config.OutboundEncKey = append([]byte(nil), config.OutboundEncKey...)
	config.OutboundAuthKey = append([]byte(nil), config.OutboundAuthKey...)
	config.InitiatorSelectors = cloneTrafficSelectors(config.InitiatorSelectors)
	config.ResponderSelectors = cloneTrafficSelectors(config.ResponderSelectors)
	config.PCSCF = cloneIPs(config.PCSCF)
	config.DNS = cloneIPs(config.DNS)
	return config
}

func cloneTrafficSelectors(selectors []trafficSelector) []trafficSelector {
	result := append([]trafficSelector(nil), selectors...)
	for index := range result {
		result[index].StartIP = append(net.IP(nil), result[index].StartIP...)
		result[index].EndIP = append(net.IP(nil), result[index].EndIP...)
	}
	return result
}
