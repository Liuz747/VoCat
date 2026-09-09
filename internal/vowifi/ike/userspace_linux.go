//go:build linux

package ike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const userspaceTunnelMTU = 1380

// Policy rule priorities must stay strictly ahead of the built-in main rule at
// 32766, which leaves this window. userspace_linux_test.go pins the bounds.
const (
	userspaceRulePriorityBase = 10000
	userspaceRulePrioritySpan = 20000
)

// Reading `ip rule show` and running `ip rule add` is not atomic, so two lines
// installing at the same time can pick the same free slot. Hold this across both
// so the kernel's rule table stays the single source of truth; nothing is cached
// and so nothing leaks when a line goes away.
var rulePriorityInstall sync.Mutex

// Per-line data-plane buffers. The tunnel MTU is 1380, so an inner packet is
// at most 1380 bytes and an ESP-wrapped packet at most ~1432 (SPI+seq+IV+pad+
// ICV). These were 65535 each, which cost ~128 KiB of permanently resident
// heap per line for no benefit. The margins below are >2x the largest packet
// the negotiated MTU can produce; ReceiveESP returns io.ErrShortBuffer rather
// than truncating, so an undersized buffer would fail loudly, never silently.
const (
	userspaceTUNReadBuffer = 4096
	userspaceESPReadBuffer = 8192
)

const userspaceTunnelPollInterval = 100 * time.Millisecond

const openWrtFirewallEnsureInterval = 10 * time.Second

type linuxUserspaceInstaller struct {
	ipCommand string
}

type linuxUserspaceHandle struct {
	ipCommand        string
	nftCommand       string
	config           ChildSAConfig
	releaseAddresses func()
	tunnel           *espTunnel
	tun              *os.File
	tunFD            int
	relay            NATTPacketRelay

	runContext context.Context
	cancel     context.CancelFunc
	wait       sync.WaitGroup
	cancelOnce sync.Once
	closeOnce  sync.Once

	mu            sync.Mutex
	closed        bool
	terminalErr   error
	failures      chan error
	cleanup       []ipCleanupCommand
	firewallRules []openWrtFirewallRule
}

type ipCleanupCommand struct {
	operation string
	arguments []string
}

func (*linuxUserspaceHandle) DataplaneMode() string { return "userspace" }

func (installer linuxUserspaceInstaller) Install(
	ctx context.Context,
	config ChildSAConfig,
) (ChildSAHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.Relay == nil {
		return nil, errors.New("ike: user-space ESP requires a NAT-T packet relay")
	}
	if !config.UDPEncapsulation {
		return nil, errors.New("ike: user-space ESP relay requires negotiated UDP encapsulation")
	}
	if len(config.PCSCF) == 0 {
		return nil, errors.New("ike: user-space ESP requires at least one negotiated P-CSCF address")
	}
	if err := validateUserspaceRoutes(config); err != nil {
		return nil, err
	}
	command := strings.TrimSpace(installer.ipCommand)
	if command == "" {
		command = "ip"
	}
	if _, err := exec.LookPath(command); err != nil {
		return nil, errors.New("Linux iproute2 is required to configure the user-space CHILD_SA")
	}
	releaseAddresses, err := reserveInnerAddresses(config)
	if err != nil {
		return nil, err
	}
	installed := false
	defer func() {
		if !installed {
			releaseAddresses()
		}
	}()
	tunnel, err := newESPTunnel(config, nil)
	if err != nil {
		return nil, err
	}
	tun, actualName, err := openLinuxTUN(config.Name)
	if err != nil {
		return nil, err
	}
	config.Name = actualName
	runContext, cancel := context.WithCancel(context.Background())
	handle := &linuxUserspaceHandle{
		ipCommand:        command,
		nftCommand:       "nft",
		releaseAddresses: releaseAddresses,
		config:           cloneChildSAConfig(config),
		tunnel:           tunnel,
		tun:              tun,
		tunFD:            int(tun.Fd()),
		relay:            config.Relay,
		runContext:       runContext,
		cancel:           cancel,
		failures:         make(chan error, 1),
	}
	if err := handle.configure(ctx); err != nil {
		cancel()
		handle.cleanupNetwork(context.Background())
		_ = tun.Close()
		return nil, err
	}
	handle.wait.Add(3)
	go handle.copyTUNToRelay()
	go handle.copyRelayToTUN()
	go handle.maintainOpenWrtFirewall()
	installed = true
	return handle, nil
}

func openLinuxTUN(name string) (*os.File, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, "", errors.New("ike: TUN interface name is required")
	}
	request, err := unix.NewIfreq(name)
	if err != nil {
		return nil, "", fmt.Errorf("ike: invalid TUN interface name: %w", err)
	}
	request.SetUint16(uint16(unix.IFF_TUN | unix.IFF_NO_PI))
	descriptor, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", fmt.Errorf("ike: open /dev/net/tun: %w", err)
	}
	if err := unix.IoctlIfreq(descriptor, unix.TUNSETIFF, request); err != nil {
		_ = unix.Close(descriptor)
		return nil, "", fmt.Errorf("ike: create TUN interface: %w", err)
	}
	// A blocking TUN read is not guaranteed to wake when another goroutine
	// closes the descriptor on Linux. Keep the descriptor non-blocking and use
	// poll below so cancellation can always drain the data-plane workers before
	// the interface is released. Without this, a failed session can retain the
	// TUN forever and every automatic reconnect fails with EBUSY.
	if err := unix.SetNonblock(descriptor, true); err != nil {
		_ = unix.Close(descriptor)
		return nil, "", fmt.Errorf("ike: make TUN interface cancellable: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), "/dev/net/tun:"+request.Name())
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, "", errors.New("ike: create TUN file handle")
	}
	return file, request.Name(), nil
}

func validateUserspaceRoutes(config ChildSAConfig) error {
	if config.InnerLocalIPv4 == nil && config.InnerLocalIPv6 == nil {
		return errors.New("ike: user-space ESP requires an assigned inner address")
	}
	validLocal := func(ip net.IP) bool {
		return ip != nil &&
			!ip.IsUnspecified() &&
			!ip.IsMulticast() &&
			ipAllowedBySelectors(ip, config.InitiatorSelectors)
	}
	if config.InnerLocalIPv4 != nil && !validLocal(config.InnerLocalIPv4) {
		return errors.New("ike: assigned inner IPv4 address is outside initiator traffic selectors")
	}
	if config.InnerLocalIPv6 != nil && !validLocal(config.InnerLocalIPv6) {
		return errors.New("ike: assigned inner IPv6 address is outside initiator traffic selectors")
	}
	matchingFamily := false
	for _, pcscf := range config.PCSCF {
		if pcscf == nil || pcscf.IsUnspecified() || pcscf.IsMulticast() {
			return errors.New("ike: P-CSCF address is invalid")
		}
		if !ipAllowedBySelectors(pcscf, config.ResponderSelectors) {
			return fmt.Errorf("ike: P-CSCF %s is outside responder traffic selectors", pcscf)
		}
		if (pcscf.To4() != nil && config.InnerLocalIPv4 != nil) ||
			(pcscf.To4() == nil && pcscf.To16() != nil && config.InnerLocalIPv6 != nil) {
			matchingFamily = true
		}
	}
	if !matchingFamily {
		return errors.New("ike: no P-CSCF address matches an assigned inner address family")
	}
	return nil
}

func ipAllowedBySelectors(ip net.IP, selectors []trafficSelector) bool {
	for _, selector := range selectors {
		if ipWithinRange(ip, selector.StartIP, selector.EndIP) {
			return true
		}
	}
	return false
}

func (handle *linuxUserspaceHandle) configure(ctx context.Context) error {
	name := handle.config.Name
	if handle.config.InnerLocalIPv4 != nil {
		if err := handle.run(
			ctx,
			"assign TUN IPv4 address",
			"-4", "address", "add",
			handle.config.InnerLocalIPv4.String()+"/32",
			"dev", name,
			"noprefixroute",
		); err != nil {
			return err
		}
	}
	if handle.config.InnerLocalIPv6 != nil {
		prefix := handle.config.InnerIPv6Prefix
		if prefix == 0 || prefix > 128 {
			prefix = 128
		}
		if err := handle.run(
			ctx,
			"assign TUN IPv6 address",
			"-6", "address", "add",
			fmt.Sprintf("%s/%d", handle.config.InnerLocalIPv6.String(), prefix),
			"dev", name,
			"noprefixroute",
		); err != nil {
			return err
		}
	}
	if err := handle.run(
		ctx,
		"enable TUN interface",
		"link", "set", "dev", name,
		"mtu", strconv.Itoa(userspaceTunnelMTU),
		"up",
	); err != nil {
		return err
	}

	table, priority := userspaceRoutingIdentifiers(handle.config.InboundSPI)
	if handle.config.InnerLocalIPv4 != nil {
		if err := handle.configureFamily(
			ctx,
			"-4",
			handle.config.InnerLocalIPv4,
			handle.ipv4PCSCF(),
			32,
			table,
			priority,
		); err != nil {
			return err
		}
	}
	if handle.config.InnerLocalIPv6 != nil {
		if err := handle.configureFamily(
			ctx,
			"-6",
			handle.config.InnerLocalIPv6,
			handle.ipv6PCSCF(),
			128,
			table,
			priority,
		); err != nil {
			return err
		}
	}
	if err := handle.configureOpenWrtFirewall(ctx); err != nil {
		return err
	}
	return nil
}

// configureOpenWrtFirewall prepares fw4 access for the dynamic VoWiFi tunnel.
func (handle *linuxUserspaceHandle) configureOpenWrtFirewall(ctx context.Context) error {
	command := strings.TrimSpace(handle.nftCommand)
	if command == "" {
		command = "nft"
	}
	if _, err := exec.LookPath(command); err != nil {
		return nil
	}
	probe := exec.CommandContext(ctx, command, "list", "chain", "inet", "fw4", "input")
	if _, err := probe.CombinedOutput(); err != nil {
		// fw4 is OpenWrt-specific. Other Linux hosts retain their native input
		// policy and do not need this compatibility rule.
		return nil
	}
	handle.firewallRules = buildOpenWrtFirewallRules(handle.config)
	if len(handle.firewallRules) == 0 {
		return nil
	}
	if err := handle.ensureOpenWrtFirewall(ctx); err != nil {
		_ = handle.cleanupOpenWrtFirewall(context.Background())
		return fmt.Errorf("ike: permit IMS input on OpenWrt TUN: %w", err)
	}
	return nil
}

// ensureOpenWrtFirewall installs any managed fw4 rules missing after a reload.
func (handle *linuxUserspaceHandle) ensureOpenWrtFirewall(ctx context.Context) error {
	if len(handle.firewallRules) == 0 {
		return nil
	}
	command := strings.TrimSpace(handle.nftCommand)
	if command == "" {
		command = "nft"
	}
	list := exec.CommandContext(ctx, command, "-a", "list", "chain", "inet", "fw4", "input")
	output, err := list.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect fw4 input chain: %s", commandErrorText(output, err))
	}
	current := string(output)
	comments := firewallRuleComments(handle.firewallRules)
	for _, nftHandle := range staleOpenWrtFirewallRuleHandles(current, handle.config.Name, comments) {
		remove := exec.CommandContext(ctx, command, "delete", "rule", "inet", "fw4", "input", "handle", nftHandle)
		if output, err := remove.CombinedOutput(); err != nil {
			return fmt.Errorf("remove stale fw4 rule %s: %s", nftHandle, commandErrorText(output, err))
		}
	}
	for _, rule := range handle.firewallRules {
		if strings.Contains(current, `comment "`+rule.comment+`"`) {
			continue
		}
		install := exec.CommandContext(ctx, command, rule.arguments...)
		if output, err := install.CombinedOutput(); err != nil {
			return fmt.Errorf("install %s: %s", rule.comment, commandErrorText(output, err))
		}
	}
	return nil
}

// maintainOpenWrtFirewall restores managed rules while the tunnel remains open.
func (handle *linuxUserspaceHandle) maintainOpenWrtFirewall() {
	defer handle.wait.Done()
	if len(handle.firewallRules) == 0 {
		return
	}
	ticker := time.NewTicker(openWrtFirewallEnsureInterval)
	defer ticker.Stop()
	for {
		select {
		case <-handle.runContext.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(handle.runContext, 5*time.Second)
			_ = handle.ensureOpenWrtFirewall(ctx)
			cancel()
		}
	}
}

// commandErrorText combines command output and its execution error for logs.
func commandErrorText(output []byte, err error) string {
	message := strings.TrimSpace(string(output))
	if message == "" && err != nil {
		message = err.Error()
	}
	return message
}

// cleanupOpenWrtFirewall removes only the fw4 rules owned by this tunnel.
func (handle *linuxUserspaceHandle) cleanupOpenWrtFirewall(ctx context.Context) error {
	if len(handle.firewallRules) == 0 {
		return nil
	}
	command := strings.TrimSpace(handle.nftCommand)
	if command == "" {
		command = "nft"
	}
	list := exec.CommandContext(ctx, command, "-a", "list", "chain", "inet", "fw4", "input")
	output, err := list.CombinedOutput()
	if err != nil {
		// A firewall reload may temporarily remove fw4; there is then nothing
		// owned by this session left to delete.
		return nil
	}
	handles := nftRuleHandles(string(output), firewallRuleComments(handle.firewallRules))
	var errs []error
	for _, nftHandle := range handles {
		remove := exec.CommandContext(ctx, command, "delete", "rule", "inet", "fw4", "input", "handle", nftHandle)
		if output, err := remove.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Errorf("remove fw4 rule %s: %s", nftHandle, commandErrorText(output, err)))
		}
	}
	handle.firewallRules = nil
	return errors.Join(errs...)
}

// installSourceRule picks the first free rule priority at or after the
// SPI-derived one and installs the rule while holding rulePriorityInstall, so
// two lines racing on the same slot cannot both win. Returns the priority used.
func (handle *linuxUserspaceHandle) installSourceRule(
	ctx context.Context,
	family string,
	preferred uint32,
	localPrefix string,
	tableValue string,
) (string, error) {
	rulePriorityInstall.Lock()
	defer rulePriorityInstall.Unlock()
	chosen, err := handle.reserveRulePriority(ctx, family, preferred)
	if err != nil {
		return "", err
	}
	priorityValue := strconv.FormatUint(uint64(chosen), 10)
	if chosen != preferred {
		slog.Default().Info(
			"policy rule priority taken, using next free slot",
			"category", "vowifi",
			"family", family,
			"preferred", preferred,
			"chosen", chosen,
		)
	}
	ruleArguments := []string{
		family, "rule", "add",
		"priority", priorityValue,
		"from", localPrefix,
		"lookup", tableValue,
	}
	if err := handle.run(ctx, "install fail-closed source rule", ruleArguments...); err != nil {
		return "", err
	}
	return priorityValue, nil
}

func (handle *linuxUserspaceHandle) configureFamily(
	ctx context.Context,
	family string,
	local net.IP,
	pcscf []net.IP,
	bits int,
	table uint32,
	priority uint32,
) error {
	if len(pcscf) == 0 {
		return nil
	}
	tableValue := strconv.FormatUint(uint64(table), 10)
	localPrefix := fmt.Sprintf("%s/%d", local.String(), bits)
	if err := handle.requireUnusedRoutingTable(ctx, family, tableValue); err != nil {
		return err
	}
	priorityValue, err := handle.installSourceRule(ctx, family, priority, localPrefix, tableValue)
	if err != nil {
		return err
	}
	handle.recordCleanup(
		"remove fail-closed source rule",
		family, "rule", "delete",
		"priority", priorityValue,
		"from", localPrefix,
		"lookup", tableValue,
	)

	unreachableArguments := []string{
		family, "route", "add",
		"table", tableValue,
		"unreachable", "default",
	}
	if err := handle.run(ctx, "install fail-closed route", unreachableArguments...); err != nil {
		return err
	}
	handle.recordCleanup(
		"remove fail-closed route",
		family, "route", "delete",
		"table", tableValue,
		"unreachable", "default",
	)

	for _, address := range pcscf {
		hostPrefix := fmt.Sprintf("%s/%d", address.String(), bits)
		routeArguments := []string{
			family, "route", "add",
			"table", tableValue,
			hostPrefix,
			"dev", handle.config.Name,
			"src", local.String(),
		}
		if err := handle.run(ctx, "install P-CSCF host route", routeArguments...); err != nil {
			return err
		}
		handle.recordCleanup(
			"remove P-CSCF host route",
			family, "route", "delete",
			"table", tableValue,
			hostPrefix,
			"dev", handle.config.Name,
			"src", local.String(),
		)
	}
	return nil
}

func userspaceRoutingIdentifiers(spi uint32) (table uint32, priority uint32) {
	table = spi
	if table <= 255 {
		table |= 0x80000000
	}
	// Linux evaluates policy rules from the lowest numeric priority upward.
	// The built-in main/default rules are 32766/32767, so a full-width SPI
	// used directly as the priority would usually run too late and leak the
	// inner source through the host's default route. Keep a SPI-derived slot
	// strictly ahead of main; requireUnusedRoutingSlot rejects collisions.
	priority = userspaceRulePriorityBase + spi%userspaceRulePrioritySpan
	return table, priority
}

// The rule priority window is narrow (it must stay ahead of main/32766), so a
// SPI-derived slot collides by the birthday bound long before the host runs out
// of room: ~15% at 80 concurrent lines, ~63% at 200. A collision used to fail
// the whole session install. reserveRulePriority instead treats the SPI-derived
// value as a starting point and walks forward to the first free slot, so N lines
// only fail once the window is genuinely exhausted.
func (handle *linuxUserspaceHandle) reserveRulePriority(
	ctx context.Context,
	family string,
	preferred uint32,
) (uint32, error) {
	used, err := handle.usedRulePriorities(ctx, family)
	if err != nil {
		return 0, err
	}
	offset := (preferred - userspaceRulePriorityBase) % userspaceRulePrioritySpan
	for step := uint32(0); step < userspaceRulePrioritySpan; step++ {
		candidate := userspaceRulePriorityBase + (offset+step)%userspaceRulePrioritySpan
		if !used[candidate] {
			return candidate, nil
		}
	}
	return 0, fmt.Errorf(
		"ike: no free policy rule priority in [%d,%d)",
		userspaceRulePriorityBase,
		userspaceRulePriorityBase+userspaceRulePrioritySpan,
	)
}

// usedRulePriorities lists the policy rule priorities currently installed on the
// host for one address family, including rules left behind by other processes.
func (handle *linuxUserspaceHandle) usedRulePriorities(
	ctx context.Context,
	family string,
) (map[uint32]bool, error) {
	ruleCommand := exec.CommandContext(ctx, handle.ipCommand, family, "rule", "show")
	ruleOutput, err := ruleCommand.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(ruleOutput))
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("ike: inspect policy rules: %s", message)
	}
	used := make(map[uint32]bool)
	for _, line := range strings.Split(string(ruleOutput), "\n") {
		trimmed := strings.TrimSpace(line)
		index := strings.Index(trimmed, ":")
		if index <= 0 {
			continue
		}
		value, err := strconv.ParseUint(trimmed[:index], 10, 32)
		if err != nil {
			continue
		}
		used[uint32(value)] = true
	}
	return used, nil
}

func (handle *linuxUserspaceHandle) requireUnusedRoutingTable(
	ctx context.Context,
	family string,
	table string,
) error {
	routeCommand := exec.CommandContext(
		ctx,
		handle.ipCommand,
		family, "-j", "route", "show", "table", "all",
	)
	routeOutput, routeErr := routeCommand.CombinedOutput()
	if routeErr != nil {
		message := strings.TrimSpace(string(routeOutput))
		if message == "" {
			message = routeErr.Error()
		}
		return fmt.Errorf("ike: inspect routing table %s: %s", table, message)
	}
	var routes []map[string]any
	if err := json.Unmarshal(routeOutput, &routes); err != nil {
		return fmt.Errorf("ike: parse Linux routing table inventory: %w", err)
	}
	for _, route := range routes {
		value, exists := route["table"]
		if !exists {
			continue
		}
		if routingTableValue(value) == table {
			return fmt.Errorf("ike: routing table %s is already in use", table)
		}
	}

	ruleCommand := exec.CommandContext(ctx, handle.ipCommand, family, "rule", "show")
	ruleOutput, err := ruleCommand.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(ruleOutput))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("ike: inspect policy rules: %s", message)
	}
	for _, line := range strings.Split(string(ruleOutput), "\n") {
		if containsAdjacentFields(strings.Fields(line), "lookup", table) {
			return fmt.Errorf("ike: routing table %s is already in use", table)
		}
	}
	return nil
}

func routingTableValue(value any) string {
	switch typed := value.(type) {
	case float64:
		if typed >= 0 && typed <= float64(^uint32(0)) {
			return strconv.FormatUint(uint64(typed), 10)
		}
	case string:
		return typed
	}
	return ""
}

func containsAdjacentFields(fields []string, first string, second string) bool {
	for index := 0; index+1 < len(fields); index++ {
		if fields[index] == first && fields[index+1] == second {
			return true
		}
	}
	return false
}

func (handle *linuxUserspaceHandle) ipv4PCSCF() []net.IP {
	var result []net.IP
	seen := make(map[string]struct{})
	for _, address := range handle.config.PCSCF {
		if address.To4() != nil {
			if _, duplicate := seen[address.String()]; duplicate {
				continue
			}
			result = append(result, append(net.IP(nil), address...))
			seen[address.String()] = struct{}{}
		}
	}
	return result
}

func (handle *linuxUserspaceHandle) ipv6PCSCF() []net.IP {
	var result []net.IP
	seen := make(map[string]struct{})
	for _, address := range handle.config.PCSCF {
		if address.To4() == nil && address.To16() != nil {
			if _, duplicate := seen[address.String()]; duplicate {
				continue
			}
			result = append(result, append(net.IP(nil), address...))
			seen[address.String()] = struct{}{}
		}
	}
	return result
}

func (handle *linuxUserspaceHandle) run(
	ctx context.Context,
	operation string,
	arguments ...string,
) error {
	command := exec.CommandContext(ctx, handle.ipCommand, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("ike: %s: %s", operation, message)
	}
	return nil
}

func (handle *linuxUserspaceHandle) recordCleanup(operation string, arguments ...string) {
	handle.cleanup = append(handle.cleanup, ipCleanupCommand{
		operation: operation,
		arguments: append([]string(nil), arguments...),
	})
}

func (handle *linuxUserspaceHandle) copyTUNToRelay() {
	defer handle.wait.Done()
	buffer := make([]byte, userspaceTUNReadBuffer)
	for {
		count, err := readTUNPacket(handle.runContext, handle.tunFD, buffer)
		if err != nil {
			if handle.runContext.Err() == nil && !errors.Is(err, os.ErrClosed) {
				handle.fail(fmt.Errorf("ike: read TUN packet: %w", err))
			}
			return
		}
		protected, err := handle.tunnel.seal(buffer[:count])
		if err != nil {
			// The kernel may emit IPv6 DAD/link-local traffic when the TUN is
			// brought up, and local processes may attempt unrelated routes.
			// Traffic-selector enforcement is a filter, not a session failure.
			if errors.Is(err, errESPPolicyDrop) {
				continue
			}
			handle.fail(err)
			return
		}
		if err := handle.relay.SendESP(handle.runContext, protected); err != nil {
			if handle.runContext.Err() == nil {
				handle.fail(fmt.Errorf("ike: relay outbound ESP: %w", err))
			}
			return
		}
	}
}

func (handle *linuxUserspaceHandle) copyRelayToTUN() {
	defer handle.wait.Done()
	buffer := make([]byte, userspaceESPReadBuffer)
	for {
		count, err := handle.relay.ReceiveESP(handle.runContext, buffer)
		if err != nil {
			if handle.runContext.Err() == nil {
				handle.fail(fmt.Errorf("ike: relay inbound ESP: %w", err))
			}
			return
		}
		cleartext, err := handle.tunnel.open(buffer[:count])
		if err != nil {
			// Invalid ICVs, replays, malformed padding, and packets outside the
			// negotiated selectors are untrusted network input. Drop them
			// without allowing a forged datagram to tear down the CHILD_SA.
			continue
		}
		if err := writeTUNPacket(handle.runContext, handle.tunFD, cleartext); err != nil {
			if handle.runContext.Err() == nil && !errors.Is(err, os.ErrClosed) {
				handle.fail(fmt.Errorf("ike: write TUN packet: %w", err))
			}
			return
		}
	}
}

func readTUNPacket(ctx context.Context, descriptor int, buffer []byte) (int, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ready, err := pollTUN(ctx, descriptor, unix.POLLIN)
		if err != nil {
			return 0, err
		}
		if !ready {
			continue
		}
		count, err := unix.Read(descriptor, buffer)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			continue
		}
		return count, err
	}
}

func writeTUNPacket(ctx context.Context, descriptor int, packet []byte) error {
	for written := 0; written < len(packet); {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := pollTUN(ctx, descriptor, unix.POLLOUT)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		count, err := unix.Write(descriptor, packet[written:])
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			continue
		}
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.New("ike: zero-length TUN write")
		}
		written += count
	}
	return nil
}

func pollTUN(ctx context.Context, descriptor int, events int16) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	poll := []unix.PollFd{{Fd: int32(descriptor), Events: events}}
	count, err := unix.Poll(poll, int(userspaceTunnelPollInterval/time.Millisecond))
	if errors.Is(err, unix.EINTR) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
		return false, os.ErrClosed
	}
	return poll[0].Revents&events != 0, nil
}

func (handle *linuxUserspaceHandle) fail(err error) {
	handle.mu.Lock()
	notify := false
	if handle.terminalErr == nil {
		handle.terminalErr = err
		notify = true
	}
	handle.mu.Unlock()
	if notify {
		select {
		case handle.failures <- err:
		default:
		}
	}
	handle.cancelRun()
}

func (handle *linuxUserspaceHandle) Failures() <-chan error {
	return handle.failures
}

func (handle *linuxUserspaceHandle) cancelRun() {
	handle.cancelOnce.Do(func() {
		handle.cancel()
	})
}

func (handle *linuxUserspaceHandle) closeTUN() {
	handle.closeOnce.Do(func() {
		_ = handle.tun.Close()
	})
}

func (handle *linuxUserspaceHandle) Close(ctx context.Context) error {
	handle.mu.Lock()
	if handle.closed {
		handle.mu.Unlock()
		return nil
	}
	handle.closed = true
	handle.mu.Unlock()

	handle.cancelRun()
	// Workers use a non-blocking, polled TUN descriptor and therefore leave on
	// cancellation without requiring a cross-goroutine close. Wait first so no
	// blocked syscall can retain the interface after Close returns.
	handle.wait.Wait()
	cleanupErr := handle.cleanupNetwork(ctx)
	handle.closeTUN()
	if handle.releaseAddresses != nil {
		handle.releaseAddresses()
	}
	// A terminal data-plane error is delivered exactly once through Failures.
	// Close reports only teardown errors so the orchestrator does not record
	// the same runtime cause again as a cleanup failure.
	return cleanupErr
}

func (handle *linuxUserspaceHandle) cleanupNetwork(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var errs []error
	if err := handle.cleanupOpenWrtFirewall(ctx); err != nil {
		errs = append(errs, err)
	}
	for index := len(handle.cleanup) - 1; index >= 0; index-- {
		item := handle.cleanup[index]
		command := exec.CommandContext(ctx, handle.ipCommand, item.arguments...)
		if output, err := command.CombinedOutput(); err != nil {
			message := strings.TrimSpace(string(output))
			if message == "" {
				message = err.Error()
			}
			errs = append(errs, fmt.Errorf("ike: %s: %s", item.operation, message))
		}
	}
	handle.cleanup = nil
	return errors.Join(errs...)
}

var _ ChildSAInstaller = linuxUserspaceInstaller{}
var _ ChildSAHandle = (*linuxUserspaceHandle)(nil)
var _ DataplaneEvidence = (*linuxUserspaceHandle)(nil)
var _ DataplaneFailureNotifier = (*linuxUserspaceHandle)(nil)
