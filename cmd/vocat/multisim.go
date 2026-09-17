package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/ims"
	"vocat/internal/vowifi/integration"
	"vocat/internal/vowifi/multisim"
	vowifiruntime "vocat/internal/vowifi/runtime"
)

type multiSIMIntegration struct {
	database  *store.Store
	devices   *device.Manager
	inventory multiSIMInventory
	singles   *vowifiruntime.Manager
	logger    *slog.Logger
	mapper    integration.ATMapper
	mu        sync.Mutex
	readers   map[string]*multiSIMReader
	// refused maps a group to the physical reader whose card it refused.
	refused map[string]string
	closing atomic.Bool
}

// multiSIMInventory is the part of the device manager a group needs to prove
// which eUICC sits in a reader and to fence USSD off that reader.
type multiSIMInventory interface {
	ESIMUsesAT(id string) (bool, error)
	ESIMListProfiles(ctx context.Context, id string) (device.EsimInfo, error)
	ESIMInventory(ctx context.Context, id string) ([]device.EsimInventoryEntry, error)
	WaitESIMProfileRecovery(ctx context.Context, id string) error
	SuspendUSSD(ctx context.Context, id string) (func(), error)
}

var errMultiSIMReaderReleased = errors.New("multisim: group released its reader")

var errMultiSIMReaderAbsent = errors.New("multisim: the group's modem is not present")

var errMultiSIMDifferentCard = errors.New("multisim: the modem now carries a different eUICC; refusing to re-attach its reader")

// followCardDuringRestore lets a group that is being disabled or re-saved
// reach its card when the modem came back at another USB position. Through the
// stale binding restore could never touch the card, and the group would hold
// the reader until the process restarts. It runs under the broker's token.
func (bridge *multiSIMIntegration) followCardDuringRestore(ctx context.Context, deviceID string, reader *multiSIMReader) error {
	current, err := bridge.mapper.Get(deviceID)
	if err != nil || current.ID == reader.backend.binding.ID() {
		// Absent or unchanged: restore's own card access decides.
		return nil
	}
	moveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := bridge.moveReader(moveCtx, deviceID, reader, current.ID, true); err != nil {
		return err
	}
	bridge.logger.Info("multisim reader re-attached for restore", "device_id", deviceID, "physical_id", current.ID)
	return nil
}

// multiSIMCardEID names the eUICC behind the storage a group uses.
// GetProfilesInfo carries no EID, so it comes from the chip inventory. With
// several storages and no known AID it refuses to guess.
func multiSIMCardEID(entries []device.EsimInventoryEntry, aid string) string {
	aid = strings.TrimSpace(aid)
	for _, entry := range entries {
		if aid != "" && strings.EqualFold(strings.TrimSpace(entry.Info.AID), aid) {
			return strings.TrimSpace(entry.Info.EID)
		}
	}
	if aid == "" && len(entries) == 1 {
		return strings.TrimSpace(entries[0].Info.EID)
	}
	return ""
}

// multiSIMBinding is the physical reader a running group addresses. Prepare
// sets it; reattachReader may move it, but only after proving that the modem at
// the new USB position carries the eUICC the group owns.
type multiSIMBinding struct {
	mu sync.Mutex
	id string
}

func newMultiSIMBinding(id string) *multiSIMBinding { return &multiSIMBinding{id: id} }

func (b *multiSIMBinding) ID() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.id
}

func (b *multiSIMBinding) set(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.id = id
}

// multiSIMStartupConcurrency bounds how many lines of one group run their
// setup (identity read, EAP-AKA, IMS AKA) at the same time. Every line shares
// one physical eUICC and a profile switch costs 2-5 s, so a wider window only
// makes AKA exchanges time out behind each other (broker RequestTimeout 30 s).
const multiSIMStartupConcurrency = 2

// multiSIMLineOptions marks an orchestrator as one line of a multi-tunnel
// group: SMS storage and phone records use the physical device ID, and setup
// queues on the group's startup gate. A nil *multiSIMLineOptions is a
// single-line device.
type multiSIMLineOptions struct {
	physicalDeviceID string
	admission        vowifi.Admission
}

type multiSIMReader struct {
	backend *multiSIMBackend
	broker  *multisim.AuthBroker
	gate    *multisim.StartupGate
	// eid identifies the card the group owns. A reader is only re-attached to
	// a modem presenting this same EID.
	eid              string
	released         bool
	original         multisim.Profile
	resumeSingle     bool
	radio            vowifi.RadioSnapshot
	radioSaved       bool
	resumeUSSD       func()
	singleMaintained bool
	// watchCancel stops the background card-liveness probe when the group
	// releases the reader.
	watchCancel context.CancelFunc
}

func (bridge *multiSIMIntegration) rejectReaderAliases(ctx context.Context, configuredID, physicalID string) error {
	configs, err := bridge.database.ListDevices(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range configs {
		if candidate.ID == configuredID {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		other, err := bridge.mapper.Get(candidate.ID)
		if err == nil && other.ID == physicalID {
			return errors.New("multisim: another device configuration refers to this physical reader")
		}
	}
	return nil
}

type multiSIMBackend struct {
	binding *multiSIMBinding
	*vowifi.EC20Adapter
	deviceID string
	mapper   integration.ATMapper
	devices  *device.Manager
}

func (backend *multiSIMBackend) ActiveICCID(ctx context.Context) (string, error) {
	physical, err := (multiSIMPinnedAT{mapper: backend.mapper, deviceID: backend.deviceID, binding: backend.binding}).physical(ctx, backend.deviceID)
	if err != nil {
		return "", err
	}
	if err := backend.devices.WaitESIMProfileRecovery(ctx, physical.ID); err != nil {
		return "", err
	}
	identity, err := backend.ReadIdentity(ctx, backend.deviceID)
	return identity.ICCID, err
}

func (backend *multiSIMBackend) SwitchProfile(ctx context.Context, profile multisim.Profile) error {
	physical, err := (multiSIMPinnedAT{mapper: backend.mapper, deviceID: backend.deviceID, binding: backend.binding}).physical(ctx, backend.deviceID)
	if err != nil {
		return err
	}
	err = backend.devices.ESIMSwitchProfile(ctx, physical.ID, profile.ICCID, profile.AID)
	// The broker retains its transaction lock until this method returns.
	// Finish a committed recovery even if the network caller timed out. If the
	// barrier itself times out, ActiveICCID waits again before any next work.
	recoveryCtx, cancelRecovery := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	recoveryErr := backend.devices.WaitESIMProfileRecovery(recoveryCtx, physical.ID)
	cancelRecovery()
	if recoveryErr != nil {
		return recoveryErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		// EnableProfile may commit before the modem rejects its GET RESPONSE.
		// Never resend the mutation: establish the actual resulting identity.
		if err = verifyMultiSIMSwitch(ctx, err, profile.ICCID, time.Second, func(probeCtx context.Context) (vowifi.SIMIdentity, bool, error) {
			if flightErr := backend.EnterVoWiFiRFOff(probeCtx, backend.deviceID); flightErr != nil {
				return vowifi.SIMIdentity{}, false, flightErr
			}
			identity, identityErr := backend.ReadIdentity(probeCtx, backend.deviceID)
			if identityErr != nil || identity.ICCID != profile.ICCID {
				return identity, false, identityErr
			}
			ready, readyErr := backend.CheckReady(probeCtx, identity)
			return identity, ready.Ready, readyErr
		}); err != nil {
			return err
		}
	}
	if err := backend.EnterVoWiFiRFOff(ctx, backend.deviceID); err != nil {
		return err
	}
	live, err := backend.ActiveICCID(ctx)
	if err != nil {
		return err
	}
	if live != profile.ICCID {
		return multisim.ErrIdentityMismatch
	}
	return nil
}

func verifyMultiSIMSwitch(ctx context.Context, cause error, expected string, interval time.Duration, probe func(context.Context) (vowifi.SIMIdentity, bool, error)) error {
	if cause == nil {
		return nil
	}
	message := strings.ToUpper(cause.Error())
	if !strings.Contains(message, "AT+CSIM") || !strings.Contains(message, "C00000") || !strings.Contains(message, "+CME ERROR: 0") {
		return cause
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var previous vowifi.SIMIdentity
	stable := 0
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		identity, ready, err := probe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			stable = 0
			continue
		}
		if identity.ICCID != expected {
			return multisim.ErrIdentityMismatch
		}
		if !ready {
			stable = 0
			continue
		}
		if stable > 0 && (previous.IMSI != identity.IMSI || previous.IMEI != identity.IMEI) {
			stable = 0
		}
		previous = identity
		stable++
		if stable == 2 {
			return nil
		}
	}
}

// Each subscription shares an already protected reader. Its orchestrator must
// never restore the RF state when one sibling reconnects or closes.
type multiSIMLineAdapter struct{ *multisim.ProfileAdapter }

func (*multiSIMLineAdapter) Snapshot(context.Context, string) (vowifi.RadioSnapshot, error) {
	return vowifi.RadioSnapshot{OperatingMode: 4, PureAirplanePolicy: true}, nil
}
func (*multiSIMLineAdapter) EnterVoWiFiRFOff(context.Context, string) error              { return nil }
func (*multiSIMLineAdapter) StopCellularData(context.Context, string) error              { return nil }
func (*multiSIMLineAdapter) Restore(context.Context, string, vowifi.RadioSnapshot) error { return nil }

type physicalProxyResolver struct {
	store    *store.Store
	deviceID string
}

func (resolver physicalProxyResolver) Resolve(ctx context.Context, request vowifi.ProxyRequest) (vowifi.ProxyRoute, error) {
	request.DeviceID = resolver.deviceID
	return (integration.ProxyResolver{Store: resolver.store}).Resolve(ctx, request)
}

func physicalIMSSMS(message ims.ReceivedSMS, deviceID string) ims.ReceivedSMS {
	message.DeviceID = deviceID
	return message
}

func newMultiSIMIntegration(database *store.Store, devices *device.Manager, singles *vowifiruntime.Manager, logger *slog.Logger) (*multiSIMIntegration, *multisim.Manager) {
	bridge := &multiSIMIntegration{database: database, devices: devices, inventory: devices, singles: singles, logger: logger,
		mapper: integration.ATMapper{Store: database, Devices: devices}, readers: make(map[string]*multiSIMReader)}
	manager := multisim.New(multisim.Options{Logger: logger.With("category", "multisim"),
		// One line's Enable now includes queueing behind the startup gate; a
		// 20-profile group needs several minutes of reader time in total.
		OperationTimeout: 6 * time.Minute, CleanupTimeout: 60 * time.Second,
		RetryInitial: 5 * time.Second, RetryMaximum: 2 * time.Minute,
		PrepareRetryable: multiSIMPrepareRetryable,
		CardHealth:       bridge.cardHealth,
		Prepare:          bridge.prepare, Restore: bridge.restore, Factory: bridge.factory, Verify: bridge.verify})
	return bridge, manager
}

// multiSIMPrepareRetryable reports whether prepare failed only because the
// reader is not usable yet. Right after a restart prepare can run before
// hardware discovery has found the reader, and a module that re-enumerates
// during prepare stops matching the reader it reserved; keep retrying (5 s
// doubling to 60 s) instead of requiring the configuration to be saved again.
func multiSIMPrepareRetryable(err error) bool {
	return errors.Is(err, device.ErrNotFound) || errors.Is(err, errReaderRemapped)
}

// verify admits profiles added to a running group: they must exist on the
// eUICC the group already owns. The inventory read takes the shared reader
// lock, so it queues behind any authentication in flight instead of
// interleaving APDUs with it.
func (bridge *multiSIMIntegration) verify(ctx context.Context, config multisim.Config, added []multisim.Profile) error {
	bridge.mu.Lock()
	reader := bridge.readers[config.DeviceID]
	bridge.mu.Unlock()
	if reader == nil || reader.backend == nil {
		return errors.New("multisim: reader is not prepared")
	}
	physical, err := (multiSIMPinnedAT{mapper: bridge.mapper, deviceID: config.DeviceID, binding: reader.backend.binding}).physical(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	inventory, err := bridge.inventory.ESIMListProfiles(ctx, physical.ID)
	if err != nil {
		return err
	}
	available := make(map[string]bool, len(inventory.Profiles))
	for _, profile := range inventory.Profiles {
		available[profile.ICCID] = true
	}
	for _, profile := range added {
		if !available[profile.ICCID] {
			return errors.New("multisim: selected profile is not present on this reader")
		}
		if profile.AID != "" && inventory.AID != "" && !strings.EqualFold(profile.AID, inventory.AID) {
			return errors.New("multisim: selected profiles must belong to the same eUICC")
		}
	}
	return nil
}

// A blank eUICC can report an ICCID for its placeholder, which is not an
// installed profile. Restore must select a real profile after the first write.
func multiSIMRestoreTarget(active string, inventory device.EsimInfo, selected []multisim.Profile) multisim.Profile {
	available := make(map[string]bool, len(inventory.Profiles))
	for _, profile := range inventory.Profiles {
		available[profile.ICCID] = true
	}
	if active != "" && available[active] {
		return multisim.Profile{ICCID: active, AID: inventory.AID}
	}
	for _, profile := range selected {
		if available[profile.ICCID] {
			return multisim.Profile{ICCID: profile.ICCID, AID: inventory.AID}
		}
	}
	return multisim.Profile{}
}

func (bridge *multiSIMIntegration) prepare(ctx context.Context, config multisim.Config) error {
	stored, err := bridge.database.Device(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	if stored.DeviceType != store.DeviceTypePCIeEC20EC25 {
		return errors.New("multisim: this release supports EC20/EC25 AT readers only")
	}
	physical, err := bridge.mapper.Get(config.DeviceID)
	if err != nil {
		return err
	}
	if err := bridge.rejectReaderAliases(ctx, config.DeviceID, physical.ID); err != nil {
		return err
	}
	usesAT, err := bridge.devices.ESIMUsesAT(physical.ID)
	if err != nil {
		return err
	}
	if !usesAT {
		return errors.New("multisim: eUICC authentication requires the AT reader transport")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	binding := newMultiSIMBinding(physical.ID)
	pinned := multiSIMPinnedAT{mapper: bridge.mapper, deviceID: config.DeviceID, binding: binding}
	adapter, err := vowifi.NewEC20Adapter(pinned, vowifi.EC20AdapterOptions{
		PureAirplanePolicy:  func(string) bool { return stored.VoWiFiEnabled },
		RestoreCellularData: stored.NetworkEnabled,
	})
	if err != nil {
		return err
	}
	reader := &multiSIMReader{backend: &multiSIMBackend{EC20Adapter: adapter, deviceID: config.DeviceID, binding: binding, mapper: bridge.mapper, devices: bridge.devices}, resumeSingle: stored.VoWiFiEnabled}
	// Reserve before any single-line, RF, or profile mutation. A failed Prepare
	// leaves this reservation for Restore; an alias rejected here owns nothing.
	if err := bridge.reserveReader(config.DeviceID, reader); err != nil {
		return err
	}
	reader.resumeUSSD, err = bridge.devices.SuspendUSSD(ctx, physical.ID)
	if err != nil {
		return err
	}
	if err := bridge.singles.BeginMaintenance(config.DeviceID); err != nil {
		return err
	}
	reader.singleMaintained = true
	if _, err := bridge.singles.RequestEnabled(config.DeviceID, false); err != nil && !errors.Is(err, vowifi.ErrCleanupIncomplete) {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := bridge.singles.State(config.DeviceID)
		if err != nil {
			return err
		}
		if !state.Enabled && !state.Active && state.Phase == vowifi.PhaseIdle {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	if err := bridge.singles.WaitCleanup(ctx, config.DeviceID); err != nil {
		return err
	}
	reader.radio, err = adapter.Snapshot(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	reader.radioSaved = true
	if err := adapter.EnterVoWiFiRFOff(ctx, config.DeviceID); err != nil {
		return err
	}
	physical, err = pinned.physical(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	inventory, err := bridge.devices.ESIMListProfiles(ctx, physical.ID)
	if err != nil {
		return err
	}
	// The EID lets reattachReader follow this card if the modem re-enumerates
	// at another USB position. Without it the group still runs, pinned as before.
	if chips, chipErr := bridge.inventory.ESIMInventory(ctx, physical.ID); chipErr == nil {
		reader.eid = multiSIMCardEID(chips, inventory.AID)
	}
	if reader.eid == "" {
		bridge.logger.Warn("multisim reader EID unavailable; the group cannot follow its card to another USB position", "device_id", config.DeviceID)
	}
	identity, err := adapter.ReadIdentity(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	reader.original = multiSIMRestoreTarget(identity.ICCID, inventory, config.Profiles)
	available := make(map[string]bool)
	for _, profile := range inventory.Profiles {
		available[profile.ICCID] = true
	}
	for _, profile := range config.Profiles {
		if !available[profile.ICCID] {
			return errors.New("multisim: selected profile is not present on this reader")
		}
		if profile.AID != "" && inventory.AID != "" && !strings.EqualFold(profile.AID, inventory.AID) {
			return errors.New("multisim: selected profiles must belong to the same eUICC")
		}
	}
	reader.broker, err = multisim.NewAuthBroker(multisim.BrokerOptions{DeviceID: config.DeviceID, Backend: reader.backend, RequestTimeout: 30 * time.Second, Logger: bridge.logger.With("category", "multisim", "device_id", config.DeviceID)})
	reader.gate = multisim.NewStartupGate(multiSIMStartupConcurrency)
	if err == nil {
		// A group-owned modem is skipped by the snapshot poller, so nothing
		// else would notice the card dying until a line needed an
		// authentication hours later.
		watchCtx, cancel := context.WithCancel(context.Background())
		reader.watchCancel = cancel
		go reader.broker.WatchCard(watchCtx, multisim.CardProbeInterval)
	}
	return err
}

// reattachReader keeps a running group attached to its card when the modem
// re-enumerates. A modem knocked off a hub comes back as a new USB device,
// often at another position and therefore under another physical ID. Bound to
// the vanished ID, the group fails every authentication, and every line on the
// card drops at the next network re-challenge, hours later (2026-09-14).
//
// The binding follows the configuration only when the modem the configuration
// now resolves to presents the EID recorded by Prepare and no other group owns
// it, and it moves under the broker's transaction token so no APDU exchange
// spans two modems. Every attempt also keeps RF off: a power-cycled EC20 boots
// with RF on, and the lifecycle event that would say so is best effort.
func (bridge *multiSIMIntegration) reattachReader(ctx context.Context, deviceID string) error {
	reader := bridge.activeReader(deviceID)
	if reader == nil {
		return nil
	}
	if _, err := bridge.mapper.Get(deviceID); err != nil {
		// Still absent; the next event or sweep tries again.
		return errMultiSIMReaderAbsent
	}
	return reader.broker.Exclusive(ctx, func(ctx context.Context) error {
		// Restore may have begun while this attempt queued for the token.
		if bridge.activeReader(deviceID) != reader {
			return nil
		}
		current, err := bridge.mapper.Get(deviceID)
		if err != nil {
			return errMultiSIMReaderAbsent
		}
		previous := reader.backend.binding.ID()
		if current.ID != previous {
			moveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := bridge.moveReader(moveCtx, deviceID, reader, current.ID, false)
			cancel()
			if errors.Is(err, errMultiSIMReaderReleased) {
				return nil
			}
			if err != nil {
				return err
			}
			bridge.logger.Info("multisim reader re-attached", "device_id", deviceID,
				"previous_physical_id", previous, "physical_id", current.ID)
		} else if reader.eid == "" {
			// Prepare could not read the chip, for example during a profile
			// recovery. Learn the EID while this binding is still the proven one.
			chipCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			chips, chipErr := bridge.inventory.ESIMInventory(chipCtx, current.ID)
			cancel()
			if chipErr == nil {
				reader.eid = multiSIMCardEID(chips, reader.original.AID)
			}
		}
		// A profile recovery soft-resets the modem (CFUN=0, then back); turning
		// RF off in the middle of it would fight the recovery.
		waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
		err = bridge.inventory.WaitESIMProfileRecovery(waitCtx, current.ID)
		cancelWait()
		if err != nil {
			return fmt.Errorf("multisim: eSIM profile recovery still running: %w", err)
		}
		// A modem whose AT port is not ready yet must not hold the token for
		// long; the next sweep tries again.
		rfCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		return reader.backend.EnterVoWiFiRFOff(rfCtx, deviceID)
	})
}

// activeReader returns the group's reader, or nil when no group owns the
// device, the group is still preparing, or its restore has begun.
func (bridge *multiSIMIntegration) activeReader(deviceID string) *multiSIMReader {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	reader := bridge.readers[deviceID]
	if reader == nil || reader.released || reader.backend == nil || reader.broker == nil {
		return nil
	}
	return reader
}

// moveReader proves the modem at physicalID carries the group's card, then
// binds the group to it. It runs under the broker's transaction token.
func (bridge *multiSIMIntegration) moveReader(ctx context.Context, deviceID string, reader *multiSIMReader, physicalID string, restoring bool) error {
	if reader.eid == "" {
		return errors.New("multisim: no EID was recorded for this group; refusing to re-attach its reader")
	}
	if err := bridge.rejectReaderAliases(ctx, deviceID, physicalID); err != nil {
		return err
	}
	usesAT, err := bridge.inventory.ESIMUsesAT(physicalID)
	if err != nil {
		return err
	}
	if !usesAT {
		return errors.New("multisim: eUICC authentication requires the AT reader transport")
	}
	if bridge.readerOwnedByOther(deviceID, physicalID) {
		return errors.New("multisim: physical reader is already owned by another configuration")
	}
	if bridge.cardRefused(deviceID, physicalID) {
		return errMultiSIMDifferentCard
	}
	chips, err := bridge.inventory.ESIMInventory(ctx, physicalID)
	if err != nil {
		return err
	}
	if !strings.EqualFold(multiSIMCardEID(chips, reader.original.AID), reader.eid) {
		bridge.rememberRefusal(deviceID, physicalID)
		return errMultiSIMDifferentCard
	}
	resume, err := bridge.inventory.SuspendUSSD(ctx, physicalID)
	if err != nil {
		return err
	}
	bridge.mu.Lock()
	if bridge.readers[deviceID] != reader || (reader.released && !restoring) {
		bridge.mu.Unlock()
		resume()
		return errMultiSIMReaderReleased
	}
	previousResume := reader.resumeUSSD
	reader.resumeUSSD = resume
	reader.backend.binding.set(physicalID)
	delete(bridge.refused, deviceID)
	bridge.mu.Unlock()
	if previousResume != nil {
		previousResume()
	}
	return nil
}

// A refused card stays refused until its modem re-enumerates. Reading the chip
// occupies the reader, so a sweep must not repeat the read every minute.
func (bridge *multiSIMIntegration) cardRefused(deviceID, physicalID string) bool {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.refused[deviceID] == physicalID
}

func (bridge *multiSIMIntegration) rememberRefusal(deviceID, physicalID string) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.refused == nil {
		bridge.refused = make(map[string]string)
	}
	bridge.refused[deviceID] = physicalID
}

// forgetRefusals clears refusals of a reader that just re-enumerated: its card
// may have been swapped while it was away.
func (bridge *multiSIMIntegration) forgetRefusals(physicalID string) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	for deviceID, refused := range bridge.refused {
		if refused == physicalID {
			delete(bridge.refused, deviceID)
		}
	}
}

// readerOwnedByOther reports whether another running group is bound to
// physicalID and is still configured for it. A binding whose configuration now
// resolves elsewhere is stale: two modems that swapped positions would
// otherwise refuse each other forever.
func (bridge *multiSIMIntegration) readerOwnedByOther(deviceID, physicalID string) bool {
	bridge.mu.Lock()
	var holders []string
	for id, other := range bridge.readers {
		if id != deviceID && other.backend != nil && other.backend.binding.ID() == physicalID {
			holders = append(holders, id)
		}
	}
	bridge.mu.Unlock()
	for _, id := range holders {
		if current, err := bridge.mapper.Get(id); err == nil && current.ID == physicalID {
			return true
		}
	}
	return false
}

// watchReaders re-attaches running groups whose modem has come back and keeps
// their RF off. Lifecycle events are best effort (a slow subscriber drops
// them), so they only bring the next periodic sweep forward.
func (bridge *multiSIMIntegration) watchReaders(ctx context.Context, events <-chan device.DeviceLifecycleEvent, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := make(map[string]string)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if !event.Present {
				continue
			}
			bridge.forgetRefusals(event.ID)
		case <-ticker.C:
		}
		bridge.reattachAll(ctx, failures)
	}
}

// reattachAll makes one attempt per running group. A failure is logged when it
// first appears or changes, not on every sweep.
func (bridge *multiSIMIntegration) reattachAll(ctx context.Context, failures map[string]string) {
	bridge.mu.Lock()
	deviceIDs := make([]string, 0, len(bridge.readers))
	for deviceID := range bridge.readers {
		deviceIDs = append(deviceIDs, deviceID)
	}
	bridge.mu.Unlock()
	running := make(map[string]bool, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		running[deviceID] = true
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := bridge.reattachReader(attemptCtx, deviceID)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errMultiSIMReaderAbsent) {
			// Neither a failure nor a recovery: wait for the modem.
			continue
		}
		message := ""
		if err != nil {
			message = err.Error()
		}
		if message == failures[deviceID] {
			continue
		}
		if message == "" {
			delete(failures, deviceID)
			bridge.logger.Info("multisim reader re-attach recovered", "device_id", deviceID)
			continue
		}
		failures[deviceID] = message
		bridge.logger.Warn("multisim reader re-attach failed", "device_id", deviceID, "error", err)
	}
	for deviceID := range failures {
		if !running[deviceID] {
			delete(failures, deviceID)
		}
	}
}

func (bridge *multiSIMIntegration) factory(ctx context.Context, config multisim.Config, profile multisim.Profile, sessionID string) (*vowifi.Orchestrator, error) {
	bridge.mu.Lock()
	reader := bridge.readers[config.DeviceID]
	bridge.mu.Unlock()
	if reader == nil || reader.broker == nil {
		return nil, errors.New("multisim: reader is not prepared")
	}
	adapter, err := reader.broker.ForProfile(profile)
	if err != nil {
		return nil, err
	}
	deviceConfig, err := bridge.lineDeviceConfig(ctx, config.DeviceID, profile, sessionID)
	if err != nil {
		return nil, err
	}
	return newVoWiFiOrchestrator(deviceConfig, bridge.database, &multiSIMLineAdapter{adapter}, bridge.logger, nil,
		&multiSIMLineOptions{physicalDeviceID: config.DeviceID, admission: reader.gate})
}

func (bridge *multiSIMIntegration) restore(ctx context.Context, config multisim.Config) error {
	bridge.mu.Lock()
	reader := bridge.readers[config.DeviceID]
	bridge.mu.Unlock()
	if reader == nil {
		return nil
	}
	// From here on the reader belongs to this restore; reattachReader leaves it
	// alone, and the card and radio work below waits for any attempt in flight.
	bridge.mu.Lock()
	reader.released = true
	watchCancel := reader.watchCancel
	bridge.mu.Unlock()
	if watchCancel != nil {
		// The card probe would otherwise compete for the token restore needs.
		watchCancel()
	}
	if reader.singleMaintained {
		if err := bridge.singles.WaitCleanup(ctx, config.DeviceID); err != nil {
			return err
		}
	}
	restoreCard := func(ctx context.Context) error {
		if reader.broker != nil {
			if err := bridge.followCardDuringRestore(ctx, config.DeviceID, reader); err != nil {
				return err
			}
		}
		if reader.original.ICCID != "" {
			current, err := reader.backend.ActiveICCID(ctx)
			if err != nil {
				return err
			}
			if current != reader.original.ICCID {
				if err := reader.backend.SwitchProfile(ctx, reader.original); err != nil {
					return err
				}
			}
		}
		if !reader.resumeSingle && reader.radioSaved {
			if err := reader.backend.Restore(ctx, config.DeviceID, reader.radio); err != nil {
				return err
			}
			// EC20Adapter consumes its checkpoint on success. Later retries only
			// need to finish the single-line handoff, not restore that checkpoint.
			reader.radioSaved = false
		}
		return nil
	}
	if reader.broker != nil {
		if err := reader.broker.Exclusive(ctx, restoreCard); err != nil {
			return err
		}
	} else if err := restoreCard(ctx); err != nil {
		return err
	}
	bridge.singles.EndMaintenance(config.DeviceID)
	if !bridge.closing.Load() {
		if _, err := bridge.singles.RequestEnabled(config.DeviceID, reader.resumeSingle); err != nil {
			return err
		}
	}
	// reattachReader swaps resumeUSSD under bridge.mu when the group follows its
	// card to another modem.
	bridge.mu.Lock()
	resumeUSSD := reader.resumeUSSD
	reader.resumeUSSD = nil
	bridge.mu.Unlock()
	if resumeUSSD != nil {
		resumeUSSD()
	}
	bridge.mu.Lock()
	if reader != nil && reader.watchCancel != nil {
		reader.watchCancel()
	}
	delete(bridge.readers, config.DeviceID)
	delete(bridge.refused, config.DeviceID)
	bridge.mu.Unlock()
	return nil
}

func multiSIMRuntimeConfig(config store.MultiSIMConfig) multisim.Config {
	result := multisim.Config{DeviceID: config.DeviceID, Enabled: config.Enabled}
	for _, profile := range config.Profiles {
		result.Profiles = append(result.Profiles, multisim.Profile{ICCID: profile.ICCID, AID: profile.AID, Name: profile.Name})
	}
	return result
}

func startConfiguredMultiSIM(ctx context.Context, database *store.Store, manager *multisim.Manager, logger *slog.Logger) error {
	configs, err := database.ListMultiSIMConfigs(ctx)
	if err != nil {
		return err
	}
	for _, config := range configs {
		if !config.Enabled {
			continue
		}
		if err := manager.Apply(ctx, multiSIMRuntimeConfig(config)); err != nil {
			logger.Error("multi-tunnel startup rejected", "device_id", config.DeviceID, "error", err)
		}
	}
	return nil
}

// A persisted enabled mode also suppresses ordinary startup before the group
// manager exists. Read failures fail closed instead of launching both modes.
func configuredMultiSIM(ctx context.Context, database *store.Store, deviceID string) bool {
	config, err := database.MultiSIMConfig(ctx, deviceID)
	return config.Enabled || (err != nil && !errors.Is(err, store.ErrNotFound))
}

type multiSIMOwner interface{ Owns(string) bool }

func multiSIMOwnsConfig(ctx context.Context, database *store.Store, deviceID string, owners ...multiSIMOwner) bool {
	for _, owner := range owners {
		if owner != nil && owner.Owns(deviceID) {
			return true
		}
	}
	return configuredMultiSIM(ctx, database, deviceID)
}

func multiSIMOwnsPhysical(ctx context.Context, database *store.Store, devices *device.Manager, physicalID string, owners ...multiSIMOwner) bool {
	configs, err := database.ListDevices(ctx)
	if err != nil {
		return true
	}
	mapper := integration.ATMapper{Store: database, Devices: devices}
	for _, config := range configs {
		if !multiSIMOwnsConfig(ctx, database, config.ID, owners...) {
			continue
		}
		physical, err := mapper.Get(config.ID)
		if err == nil && physical.ID == physicalID {
			return true
		}
	}
	return false
}

func closeMultiSIM(bridge *multiSIMIntegration, manager *multisim.Manager) error {
	bridge.closing.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		return fmt.Errorf("close multi-tunnel groups: %w", err)
	}
	return nil
}

// reserveReader makes ownership independent of user-configured aliases.
func (bridge *multiSIMIntegration) reserveReader(deviceID string, reader *multiSIMReader) error {
	if reader == nil || reader.backend == nil || reader.backend.binding.ID() == "" {
		return errors.New("multisim: physical reader identity is required")
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.readers == nil {
		bridge.readers = make(map[string]*multiSIMReader)
	}
	if bridge.readers[deviceID] != nil {
		return errors.New("multisim: reader has not finished its previous restore")
	}
	for _, existing := range bridge.readers {
		if existing.backend.binding.ID() == reader.backend.binding.ID() {
			return errors.New("multisim: physical reader is already owned by another configuration")
		}
	}
	bridge.readers[deviceID] = reader
	return nil
}

// multiSIMPinnedAT always sends to the reader the group is bound to. Checking a
// mapping and then calling ATMapper.ExecuteAT would resolve a second time and
// could redirect an APDU after a configuration edit or modem re-enumeration.
// Here every command validates the mapping against the binding, then
// addresses that validated physical ID directly. A mid-transaction remapping
// can only fail the next command; only reattachReader moves the binding.
type multiSIMPinnedAT struct {
	mapper   integration.ATMapper
	deviceID string
	binding  *multiSIMBinding
}

// errReaderRemapped means the configured device now resolves to a different
// physical reader than the one reserved, as after a modem re-enumeration.
var errReaderRemapped = errors.New("multisim: configured device no longer maps to the reserved physical reader")

func (p multiSIMPinnedAT) physical(ctx context.Context, id string) (device.Device, error) {
	if err := ctx.Err(); err != nil {
		return device.Device{}, err
	}
	physicalID := p.binding.ID()
	if id != p.deviceID || physicalID == "" {
		return device.Device{}, errors.New("multisim: physical reader binding is invalid")
	}
	current, err := p.mapper.Get(id)
	if err != nil {
		return device.Device{}, err
	}
	if current.ID != physicalID {
		return device.Device{}, errReaderRemapped
	}
	if err := ctx.Err(); err != nil {
		return device.Device{}, err
	}
	return current, nil
}

func (p multiSIMPinnedAT) ExecuteAT(ctx context.Context, id, command string) (modem.Response, error) {
	current, err := p.physical(ctx, id)
	if err != nil {
		return modem.Response{}, err
	}
	return p.mapper.Devices.ExecuteAT(ctx, current.ID, command)
}
func (p multiSIMPinnedAT) ExecuteSensitiveAT(ctx context.Context, id, command string) (modem.Response, error) {
	current, err := p.physical(ctx, id)
	if err != nil {
		return modem.Response{}, err
	}
	return p.mapper.Devices.ExecuteSensitiveAT(ctx, current.ID, command)
}
func (p multiSIMPinnedAT) BeginUICCTransaction(ctx context.Context, id string) (context.Context, func(), error) {
	current, err := p.physical(ctx, id)
	if err != nil {
		return ctx, nil, err
	}
	transactions, ok := p.mapper.Devices.(vowifi.EC20UICCTransactions)
	if !ok {
		return ctx, nil, errors.New("multisim: physical reader lacks transaction locking")
	}
	transaction, release, err := transactions.BeginUICCTransaction(ctx, current.ID)
	if err != nil {
		return ctx, nil, err
	}
	again, err := p.physical(transaction, id)
	if err == nil && again.ID != current.ID {
		err = errors.New("multisim: reader binding moved while opening a transaction")
	}
	if err != nil {
		release()
		return ctx, nil, err
	}
	return transaction, release, nil
}
func (p multiSIMPinnedAT) ReadSIMMetadata(ctx context.Context, id string) (vowifi.SIMMetadata, error) {
	current, err := p.physical(ctx, id)
	if err != nil {
		return vowifi.SIMMetadata{}, err
	}
	if current.Snapshot == nil {
		return vowifi.SIMMetadata{}, nil
	}
	return vowifi.SIMMetadata{SPN: strings.TrimSpace(current.Snapshot.SPN), GID1: strings.TrimSpace(current.Snapshot.GID1), GID2: strings.TrimSpace(current.Snapshot.GID2)}, nil
}

func (bridge *multiSIMIntegration) lineDeviceConfig(ctx context.Context, deviceID string, profile multisim.Profile, sessionID string) (store.Device, error) {
	config, err := bridge.database.Device(ctx, deviceID)
	if err != nil {
		return store.Device{}, err
	}
	config.ID = sessionID
	// Match newVoWiFiOrchestrator's IMS default, rather than inheriting the APN
	// of the unrelated profile currently projected onto the physical device.
	config.APN = "ims"
	policy, err := bridge.database.CardPolicy(ctx, profile.ICCID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return store.Device{}, err
	}
	if err == nil && strings.TrimSpace(policy.APN) != "" {
		config.APN = policy.APN
	}
	return config, nil
}

// cardHealth reports the shared reader's liveness for a device, or the zero
// value when no group owns it.
func (bridge *multiSIMIntegration) cardHealth(deviceID string) multisim.CardHealth {
	bridge.mu.Lock()
	reader := bridge.readers[deviceID]
	bridge.mu.Unlock()
	if reader == nil || reader.broker == nil {
		return multisim.CardHealth{}
	}
	return reader.broker.CardHealth()
}
