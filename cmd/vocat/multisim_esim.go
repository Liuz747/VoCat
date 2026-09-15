package main

import (
	"context"
	"errors"

	"vocat/internal/vowifi/multisim"
)

var errMultiSIMReaderNotLendable = errors.New("multisim: the group's reader is not ready for card maintenance")

// WithOwnedReader lends a running group's reader to eUICC maintenance such as
// an inventory read, a download or a rename. The operation holds the broker's
// transaction token, the one every line's profile switch and AKA takes, and
// addresses the physical reader the group is bound to.
func (bridge *multiSIMIntegration) WithOwnedReader(ctx context.Context, deviceID string, operation func(context.Context, string) error) error {
	return bridge.withOwnedCard(ctx, deviceID, func(ctx context.Context, _ *multiSIMReader, physicalID string) error {
		return operation(ctx, physicalID)
	})
}

// DeleteOwnedProfile deletes a profile from the card a running group owns. An
// enabled profile cannot be deleted, so the card is first switched to a
// remaining line; the next AKA of any line re-reads the live profile and
// switches as needed. A group whose restore target is the deleted profile is
// retargeted, so stopping the group later cannot try to enable a missing one.
func (bridge *multiSIMIntegration) DeleteOwnedProfile(ctx context.Context, deviceID, iccid string, fallback []multisim.Profile, deleteProfile func(context.Context, string) error) error {
	return bridge.withOwnedCard(ctx, deviceID, func(ctx context.Context, reader *multiSIMReader, physicalID string) error {
		active, err := reader.backend.ActiveICCID(ctx)
		if err != nil {
			return err
		}
		bridge.mu.Lock()
		original := reader.original
		bridge.mu.Unlock()
		plan, err := planOwnedProfileDeletion(iccid, active, original, fallback)
		if err != nil {
			return err
		}
		if plan.switchTo != nil {
			if err := reader.backend.SwitchProfile(ctx, *plan.switchTo); err != nil {
				return err
			}
			bridge.logger.Info("multisim card switched away before profile deletion",
				"device_id", deviceID, "profile_suffix", multiSIMProfileSuffix(plan.switchTo.ICCID))
		}
		if err := deleteProfile(ctx, physicalID); err != nil {
			return err
		}
		if plan.restoreTo != nil {
			bridge.mu.Lock()
			reader.original = *plan.restoreTo
			bridge.mu.Unlock()
		}
		bridge.logger.Info("multisim profile deleted while the group runs",
			"device_id", deviceID, "profile_suffix", multiSIMProfileSuffix(iccid), "restore_retargeted", plan.restoreTo != nil)
		return nil
	})
}

func (bridge *multiSIMIntegration) withOwnedCard(ctx context.Context, deviceID string, operation func(context.Context, *multiSIMReader, string) error) error {
	reader := bridge.activeReader(deviceID)
	if reader == nil || reader.broker == nil || reader.backend == nil {
		return errMultiSIMReaderNotLendable
	}
	return reader.broker.Exclusive(ctx, func(ctx context.Context) error {
		// Restore or a re-attachment may have taken the reader while this
		// request queued for the token.
		if bridge.activeReader(deviceID) != reader {
			return errMultiSIMReaderNotLendable
		}
		physical, err := (multiSIMPinnedAT{mapper: bridge.mapper, deviceID: deviceID, binding: reader.backend.binding}).physical(ctx, deviceID)
		if err != nil {
			return err
		}
		if err := bridge.inventory.WaitESIMProfileRecovery(ctx, physical.ID); err != nil {
			return err
		}
		return operation(ctx, reader, physical.ID)
	})
}

type ownedProfileDeletion struct {
	switchTo  *multisim.Profile
	restoreTo *multisim.Profile
}

// planOwnedProfileDeletion decides what must move off a profile before it is
// deleted: the card when it is the enabled profile, and the group's restore
// target when it points at it. Both move to the first remaining profile.
func planOwnedProfileDeletion(iccid, active string, original multisim.Profile, fallback []multisim.Profile) (ownedProfileDeletion, error) {
	var replacement *multisim.Profile
	for i := range fallback {
		if fallback[i].ICCID != "" && fallback[i].ICCID != iccid {
			candidate := fallback[i]
			replacement = &candidate
			break
		}
	}
	plan := ownedProfileDeletion{}
	if (active == iccid || original.ICCID == iccid) && replacement == nil {
		return plan, errors.New("multisim: no other profile to move to before deleting this one")
	}
	if active == iccid {
		plan.switchTo = replacement
	}
	if original.ICCID == iccid {
		plan.restoreTo = replacement
	}
	return plan, nil
}

func multiSIMProfileSuffix(iccid string) string {
	if len(iccid) <= 4 {
		return iccid
	}
	return iccid[len(iccid)-4:]
}
