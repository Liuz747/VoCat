package main

import (
	"testing"

	"vocat/internal/vowifi/multisim"
)

func TestPlanOwnedProfileDeletion(t *testing.T) {
	a := multisim.Profile{ICCID: "8944100000000000001", AID: "A000"}
	b := multisim.Profile{ICCID: "8944100000000000002", AID: "A000"}
	c := multisim.Profile{ICCID: "8944100000000000003", AID: "A000"}

	t.Run("inactive profile needs no switch", func(t *testing.T) {
		plan, err := planOwnedProfileDeletion(c.ICCID, a.ICCID, a, []multisim.Profile{a, b})
		if err != nil || plan.switchTo != nil || plan.restoreTo != nil {
			t.Fatalf("plan=%+v err=%v", plan, err)
		}
	})
	t.Run("active profile switches to a remaining line first", func(t *testing.T) {
		plan, err := planOwnedProfileDeletion(a.ICCID, a.ICCID, b, []multisim.Profile{a, b})
		if err != nil || plan.switchTo == nil || plan.switchTo.ICCID != b.ICCID || plan.restoreTo != nil {
			t.Fatalf("plan=%+v err=%v", plan, err)
		}
	})
	t.Run("restore target moves to a remaining line", func(t *testing.T) {
		plan, err := planOwnedProfileDeletion(a.ICCID, b.ICCID, a, []multisim.Profile{b})
		if err != nil || plan.switchTo != nil || plan.restoreTo == nil || plan.restoreTo.ICCID != b.ICCID {
			t.Fatalf("plan=%+v err=%v", plan, err)
		}
	})
	t.Run("no replacement refuses", func(t *testing.T) {
		if _, err := planOwnedProfileDeletion(a.ICCID, a.ICCID, a, []multisim.Profile{a}); err == nil {
			t.Fatal("deleting the only usable profile must be refused")
		}
	})
}
