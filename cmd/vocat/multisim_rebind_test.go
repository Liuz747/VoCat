package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/integration"
	"vocat/internal/vowifi/multisim"
)

// rebindTestAT models modems that keep their CFUN state per physical ID, so a
// test can see which reader actually received a command.
type rebindTestAT struct {
	mu       sync.Mutex
	entries  []device.Device
	cfun     map[string]int
	calls    []string
	listHook func()
	lists    int
}

func (d *rebindTestAT) Get(id string) (device.Device, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, entry := range d.entries {
		if entry.ID == id {
			return entry, nil
		}
	}
	return device.Device{}, device.ErrNotFound
}

func (d *rebindTestAT) List() []device.Device {
	d.mu.Lock()
	entries := append([]device.Device(nil), d.entries...)
	hook := d.listHook
	d.lists++
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	return entries
}

func (d *rebindTestAT) ExecuteAT(_ context.Context, id, command string) (modem.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, id+":"+command)
	switch command {
	case "AT+CFUN?":
		return modem.Response{Lines: []string{fmt.Sprintf("+CFUN: %d", d.cfun[id])}, Final: "OK"}, nil
	case "AT+CFUN=4":
		d.cfun[id] = 4
	}
	return modem.Response{Final: "OK"}, nil
}

func (d *rebindTestAT) ExecuteSensitiveAT(ctx context.Context, id, command string) (modem.Response, error) {
	return d.ExecuteAT(ctx, id, command)
}

func (d *rebindTestAT) BeginUICCTransaction(ctx context.Context, _ string) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (d *rebindTestAT) setDiscovered(id string, discovered bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for index := range d.entries {
		if d.entries[index].ID == id {
			d.entries[index].Discovered = discovered
		}
	}
}

func (d *rebindTestAT) setCFUN(id string, mode int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfun[id] = mode
}

func (d *rebindTestAT) setListHook(hook func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.listHook = hook
}

func (d *rebindTestAT) listCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lists
}

func (d *rebindTestAT) callsTo(id string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var result []string
	for _, call := range d.calls {
		if strings.HasPrefix(call, id+":") {
			result = append(result, strings.TrimPrefix(call, id+":"))
		}
	}
	return result
}

// rebindTestInventory mirrors the device manager: GetProfilesInfo carries no
// EID, only the chip inventory does.
type rebindTestInventory struct {
	mu         sync.Mutex
	cards      map[string]device.EsimInfo
	recovering map[string]bool
	listed     []string
	chipReads  int
	suspended  []string
}

func (i *rebindTestInventory) ESIMUsesAT(string) (bool, error) { return true, nil }

func (i *rebindTestInventory) ESIMListProfiles(_ context.Context, id string) (device.EsimInfo, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.listed = append(i.listed, id)
	card, ok := i.cards[id]
	if !ok {
		return device.EsimInfo{}, device.ErrNotFound
	}
	card.EID = ""
	return card, nil
}

func (i *rebindTestInventory) ESIMInventory(_ context.Context, id string) ([]device.EsimInventoryEntry, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.chipReads++
	card, ok := i.cards[id]
	if !ok {
		return nil, device.ErrNotFound
	}
	return []device.EsimInventoryEntry{{Info: card}}, nil
}

func (i *rebindTestInventory) WaitESIMProfileRecovery(ctx context.Context, id string) error {
	i.mu.Lock()
	busy := i.recovering[id]
	i.mu.Unlock()
	if !busy {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (i *rebindTestInventory) SuspendUSSD(_ context.Context, id string) (func(), error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.suspended = append(i.suspended, id)
	return func() {}, nil
}

func (i *rebindTestInventory) setCard(id string, card device.EsimInfo) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cards[id] = card
}

func (i *rebindTestInventory) setRecovering(id string, recovering bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.recovering[id] = recovering
}

func (i *rebindTestInventory) chipReadCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.chipReads
}

func (i *rebindTestInventory) suspendedIDs() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.suspended...)
}

// lockedBuffer lets a test read logs written by a watcher goroutine.
type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

const (
	rebindTestIMEI  = "868922057217040"
	rebindTestEID   = "89086030202200000026000057000001"
	rebindTestAID   = "A0000005591010FFFFFFFF8900000100"
	rebindTestICCID = "8901260000000000001"
	rebindOtherEID  = "89086030202200000026000057999999"
)

type rebindFixture struct {
	database  *store.Store
	bridge    *multiSIMIntegration
	at        *rebindTestAT
	inventory *rebindTestInventory
	mapper    integration.ATMapper
	logs      *lockedBuffer
	reader    *multiSIMReader
}

func newRebindWorld(t *testing.T, entries []device.Device) *rebindFixture {
	t.Helper()
	database := newRegionTestStore(t)
	at := &rebindTestAT{cfun: map[string]int{}, entries: entries}
	for _, entry := range entries {
		at.cfun[entry.ID] = 1
	}
	mapper := integration.ATMapper{Store: database, Devices: at}
	inventory := &rebindTestInventory{cards: map[string]device.EsimInfo{}, recovering: map[string]bool{}}
	logs := &lockedBuffer{}
	bridge := &multiSIMIntegration{database: database, logger: slog.New(slog.NewJSONHandler(logs, nil)), mapper: mapper,
		inventory: inventory, readers: map[string]*multiSIMReader{}}
	return &rebindFixture{database: database, bridge: bridge, at: at, inventory: inventory, mapper: mapper, logs: logs}
}

// addGroup registers a running group for config, bound to boundID.
func (f *rebindFixture) addGroup(t *testing.T, config store.Device, boundID, eid string) *multiSIMReader {
	t.Helper()
	if err := f.database.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	binding := newMultiSIMBinding(boundID)
	adapter, err := vowifi.NewEC20Adapter(multiSIMPinnedAT{mapper: f.mapper, deviceID: config.ID, binding: binding}, vowifi.EC20AdapterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	backend := &multiSIMBackend{EC20Adapter: adapter, deviceID: config.ID, binding: binding, mapper: f.mapper}
	broker, err := multisim.NewAuthBroker(multisim.BrokerOptions{DeviceID: config.ID, Backend: backend, Logger: regionTestLogger()})
	if err != nil {
		t.Fatal(err)
	}
	reader := &multiSIMReader{backend: backend, broker: broker, eid: eid}
	f.bridge.mu.Lock()
	f.bridge.readers[config.ID] = reader
	f.bridge.mu.Unlock()
	return reader
}

// newRebindFixture reproduces 2026-09-14: group 333 owns the reader at
// usb-old, the modem was knocked off the hub, and the same modem with the same
// card has come back at a different USB position (usb-new) with RF on.
func newRebindFixture(t *testing.T) *rebindFixture {
	t.Helper()
	f := newRebindWorld(t, []device.Device{
		{ID: "usb-old", Discovered: false, Candidate: modem.Candidate{USBPath: "1-1.4.4.7.1"}},
		{ID: "usb-new", Discovered: true, Candidate: modem.Candidate{USBPath: "1-1.4.4.6.3", ATPort: modem.Port{Path: "/dev/new"}}, Snapshot: &device.Snapshot{IMEI: rebindTestIMEI}},
	})
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindTestEID, AID: rebindTestAID, Profiles: []device.EsimProfile{{ICCID: rebindTestICCID}}})
	f.reader = f.addGroup(t, store.Device{ID: "333", Name: "modem", ModemIMEI: rebindTestIMEI, USBPath: "1-1.4.4.7.1"}, "usb-old", rebindTestEID)
	return f
}

func (f *rebindFixture) markReleased() {
	f.bridge.mu.Lock()
	defer f.bridge.mu.Unlock()
	f.reader.released = true
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestMultiSIMReaderFollowsSameCardToNewUSBPosition(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)

	if err := f.bridge.reattachReader(ctx, "333"); err != nil {
		t.Fatalf("re-attach refused the same card at a new position: %v", err)
	}
	if got := f.reader.backend.binding.ID(); got != "usb-new" {
		t.Fatalf("reader still bound to %q", got)
	}
	if !contains(f.at.callsTo("usb-new"), "AT+CFUN=4") {
		t.Fatalf("RF was not turned off on the re-attached modem: %v", f.at.callsTo("usb-new"))
	}
	if got := f.inventory.suspendedIDs(); len(got) != 1 || got[0] != "usb-new" {
		t.Fatalf("USSD not suspended on the new reader: %v", got)
	}
	executor := multiSIMPinnedAT{mapper: f.mapper, deviceID: "333", binding: f.reader.backend.binding}
	if _, err := executor.ExecuteAT(ctx, "333", "AT"); err != nil {
		t.Fatalf("pinned executor cannot reach the re-attached reader: %v", err)
	}
}

func TestMultiSIMCardEIDComesFromTheStorageTheGroupUses(t *testing.T) {
	other := device.EsimInventoryEntry{Info: device.EsimInfo{EID: "EID-OTHER", AID: "A0000005591010FFFFFFFF8900000200"}}
	used := device.EsimInventoryEntry{Info: device.EsimInfo{EID: rebindTestEID, AID: rebindTestAID}}
	if got := multiSIMCardEID([]device.EsimInventoryEntry{other, used}, strings.ToLower(rebindTestAID)); got != rebindTestEID {
		t.Fatalf("picked %q for the storage the group uses", got)
	}
	if got := multiSIMCardEID([]device.EsimInventoryEntry{used}, ""); got != rebindTestEID {
		t.Fatalf("single storage without a known AID gave %q", got)
	}
	if got := multiSIMCardEID([]device.EsimInventoryEntry{other, used}, ""); got != "" {
		t.Fatalf("guessed %q among several storages", got)
	}
	if got := multiSIMCardEID(nil, rebindTestAID); got != "" {
		t.Fatalf("empty inventory gave %q", got)
	}
}

func TestMultiSIMReaderLearnsMissingEIDWhileBindingIsValid(t *testing.T) {
	f := newRebindFixture(t)
	// Prepare could not read the chip (for example during a profile recovery).
	f.reader.backend.binding.set("usb-new")
	f.reader.eid = ""

	if err := f.bridge.reattachReader(context.Background(), "333"); err != nil {
		t.Fatal(err)
	}
	if f.reader.eid != rebindTestEID {
		t.Fatalf("EID still %q; the group could never follow its card", f.reader.eid)
	}
}

func TestMultiSIMReaderRefusesDifferentCardAtNewPosition(t *testing.T) {
	f := newRebindFixture(t)
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})

	if err := f.bridge.reattachReader(context.Background(), "333"); err == nil {
		t.Fatal("re-attached a group to a different eUICC")
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMRefusedCardIsNotReadAgainEverySweep(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})

	for attempt := 0; attempt < 3; attempt++ {
		if err := f.bridge.reattachReader(ctx, "333"); err == nil {
			t.Fatalf("attempt %d re-attached a different eUICC", attempt)
		}
	}
	// Reading the chip occupies the reader; a refusal stands until the modem
	// re-enumerates, so later sweeps must not read it again.
	if got := f.inventory.chipReadCount(); got != 1 {
		t.Fatalf("refused card read %d times", got)
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMRefusalForgottenWhenModemReappears(t *testing.T) {
	f := newRebindFixture(t)
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})
	if err := f.bridge.reattachReader(context.Background(), "333"); err == nil {
		t.Fatal("re-attached a different eUICC")
	}

	// Someone puts the right card back and re-seats the modem.
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindTestEID, AID: rebindTestAID})
	if err := f.bridge.reattachReader(context.Background(), "333"); err == nil {
		t.Fatal("refusal was not remembered before the modem re-enumerated")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan device.DeviceLifecycleEvent, 1)
	go f.bridge.watchReaders(ctx, events, time.Hour)
	events <- device.DeviceLifecycleEvent{ID: "usb-new", Present: true}
	waitForBinding(t, f, "usb-new")
}

func TestMultiSIMReaderRefusesRebindWithoutRecordedEID(t *testing.T) {
	f := newRebindFixture(t)
	f.reader.eid = ""

	if err := f.bridge.reattachReader(context.Background(), "333"); err == nil {
		t.Fatal("re-attached without proof that the card is the one the group owns")
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMReaderRefusesPositionOwnedByAnotherGroup(t *testing.T) {
	f := newRebindFixture(t)
	// Group 444 is configured for the modem at usb-new and still bound to it.
	f.addGroup(t, store.Device{ID: "444", Name: "other", ATPort: "/dev/new"}, "usb-new", "EID-444")

	if err := f.bridge.reattachReader(context.Background(), "333"); err == nil {
		t.Fatal("re-attached onto a reader another group owns")
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMReadersThatSwappedPositionsBothReattach(t *testing.T) {
	ctx := context.Background()
	f := newRebindWorld(t, []device.Device{
		{ID: "usb-a", Discovered: true, Candidate: modem.Candidate{USBPath: "1-1.4.4.7.1"}, Snapshot: &device.Snapshot{IMEI: "860000000000002"}},
		{ID: "usb-b", Discovered: true, Candidate: modem.Candidate{USBPath: "1-1.4.4.7.2"}, Snapshot: &device.Snapshot{IMEI: "860000000000001"}},
	})
	f.inventory.setCard("usb-a", device.EsimInfo{EID: "EID-2", AID: rebindTestAID})
	f.inventory.setCard("usb-b", device.EsimInfo{EID: "EID-1", AID: rebindTestAID})
	first := f.addGroup(t, store.Device{ID: "first", Name: "first", ModemIMEI: "860000000000001"}, "usb-a", "EID-1")
	second := f.addGroup(t, store.Device{ID: "second", Name: "second", ModemIMEI: "860000000000002"}, "usb-b", "EID-2")

	if err := f.bridge.reattachReader(ctx, "first"); err != nil {
		t.Fatalf("first group refused the position its stale neighbour no longer uses: %v", err)
	}
	if err := f.bridge.reattachReader(ctx, "second"); err != nil {
		t.Fatalf("second group refused: %v", err)
	}
	if first.backend.binding.ID() != "usb-b" || second.backend.binding.ID() != "usb-a" {
		t.Fatalf("bindings after swap: first=%q second=%q", first.backend.binding.ID(), second.backend.binding.ID())
	}
}

func TestMultiSIMReaderStaysPutWhileModemIsAbsent(t *testing.T) {
	f := newRebindFixture(t)
	f.at.setDiscovered("usb-new", false)

	if err := f.bridge.reattachReader(context.Background(), "333"); !errors.Is(err, errMultiSIMReaderAbsent) {
		t.Fatalf("absent modem reported %v", err)
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMSweepTurnsRFOffOnModemPowerCycledInPlace(t *testing.T) {
	f := newRebindFixture(t)
	f.reader.backend.binding.set("usb-new")
	f.at.setCFUN("usb-new", 1)

	// No lifecycle event arrived (dropped); the sweep alone must notice.
	if err := f.bridge.reattachReader(context.Background(), "333"); err != nil {
		t.Fatal(err)
	}
	if !contains(f.at.callsTo("usb-new"), "AT+CFUN=4") {
		t.Fatalf("RF left on after the modem power-cycled in place: %v", f.at.callsTo("usb-new"))
	}
	if got := f.inventory.suspendedIDs(); len(got) != 0 {
		t.Fatalf("same-position return suspended USSD a second time: %v", got)
	}
}

func TestMultiSIMSweepOnlyReadsRFStateWhenAlreadyOff(t *testing.T) {
	f := newRebindFixture(t)
	f.reader.backend.binding.set("usb-new")
	f.at.setCFUN("usb-new", 4)

	if err := f.bridge.reattachReader(context.Background(), "333"); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.at.callsTo("usb-new") {
		if call != "AT+CFUN?" {
			t.Fatalf("sweep changed an undisturbed modem: %v", f.at.callsTo("usb-new"))
		}
	}
	if f.inventory.chipReadCount() != 0 || len(f.inventory.listed) != 0 {
		t.Fatal("sweep read the eUICC of an undisturbed modem")
	}
}

func TestMultiSIMSweepLeavesRFAloneDuringProfileRecovery(t *testing.T) {
	f := newRebindFixture(t)
	f.reader.backend.binding.set("usb-new")
	f.at.setCFUN("usb-new", 0) // mid soft reset
	f.inventory.setRecovering("usb-new", true)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := f.bridge.reattachReader(ctx, "333"); err == nil {
		t.Fatal("sweep reported success while a profile recovery was still running")
	}
	if contains(f.at.callsTo("usb-new"), "AT+CFUN=4") {
		t.Fatal("sweep flipped RF in the middle of a profile recovery")
	}
}

func TestMultiSIMReaderWaitsForInFlightAuthentication(t *testing.T) {
	f := newRebindFixture(t)
	held := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = f.reader.broker.Exclusive(context.Background(), func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	waiting, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := f.bridge.reattachReader(waiting, "333")
	close(release)
	<-finished
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("re-attach did not wait for the authentication in flight: %v", err)
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMReaderLeftAloneOnceRestoreBegins(t *testing.T) {
	f := newRebindFixture(t)
	f.markReleased()

	if err := f.bridge.reattachReader(context.Background(), "333"); err != nil {
		t.Fatal(err)
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMReaderLeftAloneWhenGroupReleasedWhileQueued(t *testing.T) {
	f := newRebindFixture(t)
	held := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = f.reader.broker.Exclusive(context.Background(), func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	// The first modem lookup happens after the pre-token membership check, so
	// once it fires the attempt can only proceed through the token.
	var once sync.Once
	resolved := make(chan struct{})
	f.at.setListHook(func() { once.Do(func() { close(resolved) }) })
	result := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		result <- f.bridge.reattachReader(ctx, "333")
	}()
	<-resolved
	f.bridge.mu.Lock()
	delete(f.bridge.readers, "333")
	f.bridge.mu.Unlock()
	close(release)
	<-finished
	if err := <-result; err != nil {
		t.Fatalf("re-attach of a released group reported %v", err)
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMRestoreFollowsCardToNewPosition(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)
	f.markReleased()

	// The group is being disabled while its modem sits at a new position:
	// restore must still reach the card, or it holds the reader forever.
	if err := f.reader.broker.Exclusive(ctx, func(ctx context.Context) error {
		return f.bridge.followCardDuringRestore(ctx, "333", f.reader)
	}); err != nil {
		t.Fatalf("restore could not follow its card: %v", err)
	}
	if got := f.reader.backend.binding.ID(); got != "usb-new" {
		t.Fatalf("restore still bound to %q", got)
	}
	if got := f.inventory.suspendedIDs(); len(got) != 1 || got[0] != "usb-new" {
		t.Fatalf("USSD not fenced on the reader restore will use: %v", got)
	}
}

func TestMultiSIMRestoreDoesNotFollowADifferentCard(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)
	f.markReleased()
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})

	if err := f.reader.broker.Exclusive(ctx, func(ctx context.Context) error {
		return f.bridge.followCardDuringRestore(ctx, "333", f.reader)
	}); err == nil {
		t.Fatal("restore followed its configuration onto a different eUICC")
	}
	assertRebindUntouched(t, f)
}

func TestMultiSIMWatchReattachesWhenModemReappears(t *testing.T) {
	f := newRebindFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan device.DeviceLifecycleEvent, 1)
	go f.bridge.watchReaders(ctx, events, time.Hour)

	events <- device.DeviceLifecycleEvent{ID: "usb-new", Present: true}
	waitForBinding(t, f, "usb-new")
}

func TestMultiSIMWatchPeriodicallyReattachesWithoutEvents(t *testing.T) {
	f := newRebindFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Lifecycle events are best effort and dropped when a subscriber is slow.
	go f.bridge.watchReaders(ctx, make(chan device.DeviceLifecycleEvent), 10*time.Millisecond)

	waitForBinding(t, f, "usb-new")
}

func TestMultiSIMWatchLogsPersistentRefusalOnce(t *testing.T) {
	f := newRebindFixture(t)
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.bridge.watchReaders(ctx, nil, time.Millisecond)
	}()
	// Every attempt resolves the modem at least twice; wait for several sweeps.
	deadline := time.Now().Add(5 * time.Second)
	for f.at.listCount() < 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if f.at.listCount() < 10 {
		t.Fatalf("only %d modem lookups ran", f.at.listCount())
	}
	if got := strings.Count(f.logs.String(), "multisim reader re-attach failed"); got != 1 {
		t.Fatalf("persistent refusal logged %d times", got)
	}
}

func TestMultiSIMWatchDoesNotReportAbsentModemAsRecovered(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)
	f.inventory.setCard("usb-new", device.EsimInfo{EID: rebindOtherEID, AID: rebindTestAID})
	failures := map[string]string{}

	f.bridge.reattachAll(ctx, failures)
	f.at.setDiscovered("usb-new", false)
	f.bridge.reattachAll(ctx, failures)
	f.at.setDiscovered("usb-new", true)
	f.bridge.reattachAll(ctx, failures)

	logs := f.logs.String()
	if got := strings.Count(logs, "multisim reader re-attach failed"); got != 1 {
		t.Fatalf("refusal logged %d times across an absence", got)
	}
	if strings.Contains(logs, "multisim reader re-attach recovered") {
		t.Fatal("an absent modem was reported as a recovery")
	}
}

func TestMultiSIMVerifyChecksReaderMapping(t *testing.T) {
	ctx := context.Background()
	f := newRebindFixture(t)
	added := []multisim.Profile{{ICCID: rebindTestICCID}}

	// Still bound to the vanished reader: the modem now at usb-new is not the
	// one this group proved, so its profile list must not admit anything.
	if err := f.bridge.verify(ctx, multisim.Config{DeviceID: "333"}, added); err == nil {
		t.Fatal("admitted profiles from a reader the group is not bound to")
	}
	if len(f.inventory.listed) != 0 {
		t.Fatalf("listed profiles on an unverified reader: %v", f.inventory.listed)
	}

	f.reader.backend.binding.set("usb-new")
	if err := f.bridge.verify(ctx, multisim.Config{DeviceID: "333"}, added); err != nil {
		t.Fatalf("bound reader rejected a profile it carries: %v", err)
	}
}

func assertRebindUntouched(t *testing.T, f *rebindFixture) {
	t.Helper()
	if got := f.reader.backend.binding.ID(); got != "usb-old" {
		t.Fatalf("binding moved to %q", got)
	}
	if calls := f.at.callsTo("usb-new"); len(calls) != 0 {
		t.Fatalf("refused re-attach still sent commands: %v", calls)
	}
	if got := f.inventory.suspendedIDs(); len(got) != 0 {
		t.Fatalf("refused re-attach suspended USSD: %v", got)
	}
}

func waitForBinding(t *testing.T, f *rebindFixture, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.reader.backend.binding.ID() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("reader never re-attached to %q, still %q", want, f.reader.backend.binding.ID())
}
