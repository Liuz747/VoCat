package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/multisim"
	vowifiruntime "vocat/internal/vowifi/runtime"
)

func TestNodeEnsureCreatesFirstGroupAndPreservesReadySingle(t *testing.T) {
	for _, keepSingle := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-group", true: "ready-single"}[keepSingle], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			s, ds, reader := phoneHardwareFixture(t, 1)
			id := ds.entries[0].Snapshot.IMEI
			old := multisim.Profile{ICCID: "8900000000000000001", AID: "A0000005591010"}
			next := multisim.Profile{ICCID: "8900000000000000002", AID: old.AID}
			reader.cards[ds.entries[0].ID] = device.EsimInfo{AID: old.AID, Profiles: []device.EsimProfile{{ICCID: old.ICCID}, {ICCID: next.ICCID}}}
			// Previously stopped profiles must not be resurrected with a first ensure.
			if _, err := s.database.SaveMultiSIMConfig(ctx, store.MultiSIMConfig{DeviceID: id, Enabled: false, Profiles: []store.MultiSIMProfile{{ICCID: "8999999999999999999", AID: old.AID}}}); err != nil {
				t.Fatal(err)
			}
			s.multisim = multisim.New(multisim.Options{Factory: func(_ context.Context, _ multisim.Config, p multisim.Profile, sid string) (*vowifi.Orchestrator, error) {
				return downloadTestOrchestrator(p, sid, downloadTestTunnel{}, downloadTestIMS{})
			}})
			defer s.multisim.Close(context.Background())
			if keepSingle {
				s.vowifi = vowifiruntime.New(vowifiruntime.Options{Factory: func(_ context.Context, sid string) (*vowifi.Orchestrator, error) {
					return downloadTestOrchestrator(old, sid, downloadTestTunnel{}, downloadTestIMS{})
				}})
				defer s.vowifi.Close(context.Background())
				if err := s.vowifi.Ensure(ctx, id); err != nil {
					t.Fatal(err)
				}
				if _, err := s.vowifi.RequestEnabled(id, true); err != nil {
					t.Fatal(err)
				}
				if !s.awaitTunnel(ctx, id, old.ICCID, true) {
					t.Fatal("single never ready")
				}
			}
			result, failure := s.ensureTunnel(ctx, nodemqtt.Command{Target: &nodemqtt.Target{Device: id, Slot: id, ICCID: next.ICCID, BindingVersion: 1}, Params: json.RawMessage(`{}`)})
			if failure != nil {
				t.Fatalf("ensure failed: %+v", failure)
			}
			if result.(map[string]any)["state"] != "registered" {
				t.Fatalf("result=%v", result)
			}
			cfg, err := s.database.MultiSIMConfig(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if keepSingle {
				want = 2
			}
			if !cfg.Enabled || len(cfg.Profiles) != want {
				t.Fatalf("config=%+v", cfg)
			}
			if keepSingle && !s.awaitTunnel(ctx, id, old.ICCID, true) {
				t.Fatal("existing single profile lost")
			}
			again, failure := s.ensureTunnel(ctx, nodemqtt.Command{Target: &nodemqtt.Target{Device: id, Slot: id, ICCID: next.ICCID, BindingVersion: 1}, Params: json.RawMessage(`{}`)})
			if failure != nil || again.(map[string]any)["changed"] != false {
				t.Fatalf("ready ensure not idempotent: %v %+v", again, failure)
			}
		})
	}
}
