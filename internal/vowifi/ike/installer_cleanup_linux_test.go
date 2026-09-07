//go:build linux

package ike

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// fakeXFRMIP records commands and simulates an already-owned interface plus
// an optional failed operation. It never invokes ip or changes host networking.
func fakeXFRMIP(t *testing.T, failAt int) (string, func() [][]string) {
	t.Helper()
	directory := t.TempDir()
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
	logfile := filepath.Join(directory, "commands")
	script := `#!/bin/sh
cd ` + quote(directory) + `
n=0
if [ -f count ]; then n=$(cat count); fi
n=$((n + 1))
printf '%s' "$n" > count
printf '%s\t' "$@" >> commands
printf '\n' >> commands
if [ "$n" -eq ` + strconv.Itoa(failAt) + ` ]; then exit 1; fi
if [ "$1 $2" = 'link add' ]; then
 if [ -f link ]; then exit 2; fi
 : > link
fi
if [ "$1 $2" = 'link delete' ]; then rm -f link; fi
exit 0
`
	command := filepath.Join(directory, "fake-ip")
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return command, func() [][]string {
		t.Helper()
		data, err := os.ReadFile(logfile)
		if err != nil {
			t.Fatal(err)
		}
		var commands [][]string
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			commands = append(commands, strings.Fields(line))
		}
		return commands
	}
}

func cleanupXFRMConfig() ChildSAConfig {
	return ChildSAConfig{
		Name: "same-name", OuterLocal: net.ParseIP("192.0.2.10"), OuterRemote: net.ParseIP("192.0.2.20"),
		InnerLocalIPv4: net.ParseIP("198.51.100.201"), InboundSPI: 0x101, OutboundSPI: 0x202,
		InitiatorSelectors: []trafficSelector{{IPProtocol: 17, StartIP: net.ParseIP("198.51.100.201"), EndIP: net.ParseIP("198.51.100.201"), StartPort: 5060, EndPort: 5060}},
		ResponderSelectors: []trafficSelector{{IPProtocol: 17, StartIP: net.ParseIP("203.0.113.20"), EndIP: net.ParseIP("203.0.113.20"), StartPort: 5062, EndPort: 5062}},
	}
}
func deleteCommands(commands [][]string) [][]string {
	var result [][]string
	for _, command := range commands {
		for _, arg := range command {
			if arg == "delete" {
				result = append(result, command)
				break
			}
		}
	}
	return result
}
func TestXFRMFailedSameNameInstallDoesNotDeleteExistingSession(t *testing.T) {
	command, commands := fakeXFRMIP(t, 0)
	installer := linuxXFRMInstaller{ipCommand: command}
	firstConfig := cleanupXFRMConfig()
	first, err := installer.Install(context.Background(), firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	before := len(commands())
	secondConfig := cleanupXFRMConfig()
	secondConfig.InnerLocalIPv4 = net.ParseIP("198.51.100.202")
	secondConfig.InitiatorSelectors[0].StartIP = secondConfig.InnerLocalIPv4
	secondConfig.InitiatorSelectors[0].EndIP = secondConfig.InnerLocalIPv4
	secondConfig.InboundSPI++
	secondConfig.OutboundSPI++
	if second, err := installer.Install(context.Background(), secondConfig); err == nil {
		second.Close(context.Background())
		t.Fatal("duplicate interface was accepted")
	}
	if got := deleteCommands(commands()[before:]); len(got) != 0 {
		t.Fatalf("failed link creation deleted existing objects: %v", got)
	}
}
func TestXFRMPartialInstallDeletesOnlyCreatedObjectsInReverseOrder(t *testing.T) {
	link := []string{"link", "delete", "same-name"}
	outState := strings.Fields("xfrm state delete src 192.0.2.10 dst 192.0.2.20 proto esp spi 0x00000202")
	inState := strings.Fields("xfrm state delete src 192.0.2.20 dst 192.0.2.10 proto esp spi 0x00000101")
	outPolicy := strings.Fields("-4 xfrm policy delete src 198.51.100.201/32 dst 203.0.113.20/32 dir out proto 17 sport 5060 dport 5062")
	for _, test := range []struct {
		failAt int
		want   [][]string
	}{
		{2, [][]string{link}},
		{4, [][]string{link}},
		{5, [][]string{outState, link}},
		{6, [][]string{inState, outState, link}},
		{7, [][]string{outPolicy, inState, outState, link}},
	} {
		t.Run(fmt.Sprintf("failure_at_%d", test.failAt), func(t *testing.T) {
			command, commands := fakeXFRMIP(t, test.failAt)
			config := cleanupXFRMConfig()
			if handle, err := (linuxXFRMInstaller{ipCommand: command}).Install(context.Background(), config); err == nil {
				handle.Close(context.Background())
				t.Fatal("injected failure ignored")
			}
			if got := deleteCommands(commands()); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("cleanup=%v; want=%v", got, test.want)
			}
			// Failed initialization also releases only its own inner-address reservation.
			release, err := reserveInnerAddresses(config)
			if err != nil {
				t.Fatalf("address reservation leaked: %v", err)
			}
			release()
		})
	}
}
func TestXFRMCloseDeletesExactPoliciesInReverseOrderOnce(t *testing.T) {
	command, commands := fakeXFRMIP(t, 0)
	handle, err := (linuxXFRMInstaller{ipCommand: command}).Install(context.Background(), cleanupXFRMConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		strings.Fields("-4 xfrm policy delete src 203.0.113.20/32 dst 198.51.100.201/32 dir in proto 17 sport 5062 dport 5060"),
		strings.Fields("-4 xfrm policy delete src 198.51.100.201/32 dst 203.0.113.20/32 dir out proto 17 sport 5060 dport 5062"),
		strings.Fields("xfrm state delete src 192.0.2.20 dst 192.0.2.10 proto esp spi 0x00000101"),
		strings.Fields("xfrm state delete src 192.0.2.10 dst 192.0.2.20 proto esp spi 0x00000202"),
		strings.Fields("link delete same-name"),
	}
	if got := deleteCommands(commands()); !reflect.DeepEqual(got, want) {
		t.Fatalf("cleanup=%v; want=%v", got, want)
	}
	before := len(commands())
	if err := handle.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(commands()) != before {
		t.Fatal("second Close repeated deletion")
	}
}
