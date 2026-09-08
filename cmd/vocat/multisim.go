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
	database *store.Store
	devices  *device.Manager
	singles  *vowifiruntime.Manager
	logger   *slog.Logger
	mapper   integration.ATMapper
	mu       sync.Mutex
	readers  map[string]*multiSIMReader
	closing  atomic.Bool
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
	backend          *multiSIMBackend
	broker           *multisim.AuthBroker
	gate             *multisim.StartupGate
	original         multisim.Profile
	resumeSingle     bool
	radio            vowifi.RadioSnapshot
	radioSaved       bool
	resumeUSSD       func()
	singleMaintained bool
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
	physicalID string
	*vowifi.EC20Adapter
	deviceID string
	mapper   integration.ATMapper
	devices  *device.Manager
}

func (backend *multiSIMBackend) ActiveICCID(ctx context.Context) (string, error) {
	physical, err := (multiSIMPinnedAT{mapper: backend.mapper, deviceID: backend.deviceID, physicalID: backend.physicalID}).physical(ctx, backend.deviceID)
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
	physical, err := (multiSIMPinnedAT{mapper: backend.mapper, deviceID: backend.deviceID, physicalID: backend.physicalID}).physical(ctx, backend.deviceID)
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
	bridge := &multiSIMIntegration{database: database, devices: devices, singles: singles, logger: logger,
		mapper: integration.ATMapper{Store: database, Devices: devices}, readers: make(map[string]*multiSIMReader)}
	manager := multisim.New(multisim.Options{Logger: logger.With("category", "multisim"),
		// One line's Enable now includes queueing behind the startup gate; a
		// 20-profile group needs several minutes of reader time in total.
		OperationTimeout: 6 * time.Minute, CleanupTimeout: 60 * time.Second,
		RetryInitial: 5 * time.Second, RetryMaximum: 2 * time.Minute,
		Prepare: bridge.prepare, Restore: bridge.restore, Factory: bridge.factory})
	return bridge, manager
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
	pinned := multiSIMPinnedAT{mapper: bridge.mapper, deviceID: config.DeviceID, physicalID: physical.ID}
	adapter, err := vowifi.NewEC20Adapter(pinned, vowifi.EC20AdapterOptions{
		PureAirplanePolicy:  func(string) bool { return stored.VoWiFiEnabled },
		RestoreCellularData: stored.NetworkEnabled,
	})
	if err != nil {
		return err
	}
	reader := &multiSIMReader{backend: &multiSIMBackend{EC20Adapter: adapter, deviceID: config.DeviceID, physicalID: physical.ID, mapper: bridge.mapper, devices: bridge.devices}, resumeSingle: stored.VoWiFiEnabled}
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
	identity, err := adapter.ReadIdentity(ctx, config.DeviceID)
	if err != nil {
		return err
	}
	reader.original = multisim.Profile{ICCID: identity.ICCID, AID: inventory.AID}
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
	return err
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
	if reader.singleMaintained {
		if err := bridge.singles.WaitCleanup(ctx, config.DeviceID); err != nil {
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
	bridge.singles.EndMaintenance(config.DeviceID)
	if !bridge.closing.Load() {
		if _, err := bridge.singles.RequestEnabled(config.DeviceID, reader.resumeSingle); err != nil {
			return err
		}
	}
	if reader.resumeUSSD != nil {
		reader.resumeUSSD()
		reader.resumeUSSD = nil
	}
	bridge.mu.Lock()
	delete(bridge.readers, config.DeviceID)
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
	if reader == nil || reader.backend == nil || reader.backend.physicalID == "" {
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
		if existing.backend.physicalID == reader.backend.physicalID {
			return errors.New("multisim: physical reader is already owned by another configuration")
		}
	}
	bridge.readers[deviceID] = reader
	return nil
}

// multiSIMPinnedAT always sends to the reader reserved by Prepare. Checking a
// mapping and then calling ATMapper.ExecuteAT would resolve a second time and
// could redirect an APDU after a configuration edit or modem re-enumeration.
// Here every command validates the mapping, then addresses the fixed physical
// ID directly. A mid-transaction remapping can only fail the next command.
type multiSIMPinnedAT struct {
	mapper               integration.ATMapper
	deviceID, physicalID string
}

func (p multiSIMPinnedAT) physical(ctx context.Context, id string) (device.Device, error) {
	if err := ctx.Err(); err != nil {
		return device.Device{}, err
	}
	if id != p.deviceID || p.physicalID == "" {
		return device.Device{}, errors.New("multisim: physical reader binding is invalid")
	}
	current, err := p.mapper.Get(id)
	if err != nil {
		return device.Device{}, err
	}
	if current.ID != p.physicalID {
		return device.Device{}, errors.New("multisim: configured device no longer maps to the reserved physical reader")
	}
	if err := ctx.Err(); err != nil {
		return device.Device{}, err
	}
	return current, nil
}

func (p multiSIMPinnedAT) ExecuteAT(ctx context.Context, id, command string) (modem.Response, error) {
	if _, err := p.physical(ctx, id); err != nil {
		return modem.Response{}, err
	}
	return p.mapper.Devices.ExecuteAT(ctx, p.physicalID, command)
}
func (p multiSIMPinnedAT) ExecuteSensitiveAT(ctx context.Context, id, command string) (modem.Response, error) {
	if _, err := p.physical(ctx, id); err != nil {
		return modem.Response{}, err
	}
	return p.mapper.Devices.ExecuteSensitiveAT(ctx, p.physicalID, command)
}
func (p multiSIMPinnedAT) BeginUICCTransaction(ctx context.Context, id string) (context.Context, func(), error) {
	if _, err := p.physical(ctx, id); err != nil {
		return ctx, nil, err
	}
	transactions, ok := p.mapper.Devices.(vowifi.EC20UICCTransactions)
	if !ok {
		return ctx, nil, errors.New("multisim: physical reader lacks transaction locking")
	}
	transaction, release, err := transactions.BeginUICCTransaction(ctx, p.physicalID)
	if err != nil {
		return ctx, nil, err
	}
	if _, err := p.physical(transaction, id); err != nil {
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
