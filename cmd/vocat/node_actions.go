package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/singleflight"
	"vocat/internal/device"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/integration"
	"vocat/internal/vowifi/multisim"
	vowifiruntime "vocat/internal/vowifi/runtime"
)

type nodeActionService struct {
	database           *store.Store
	devices            *device.Manager
	esim               nodeESIMController
	readCard           func(context.Context, string, func(context.Context, string) error) error
	vowifi             *vowifiruntime.Manager
	multisim           *multisim.Manager
	cards              *multiSIMIntegration
	mapper             integration.ATMapper
	nodeMu             sync.RWMutex
	node               string
	maxPayload         int
	revisions          sync.Map
	cacheMu            sync.Mutex
	phoneCache         phoneCollection
	snapshots          map[string]phoneSnapshot
	inventorySnapshots map[string]nodeInventorySnapshot
	phoneGroup         singleflight.Group
	locks              sync.Map
}

type nodeResourceLock struct{ token chan struct{} }

type phoneCollection struct {
	items       []nodePhoneRecord
	generatedAt time.Time
}

type phoneSnapshot struct {
	collection        phoneCollection
	expiresAt         time.Time
	pageSize          int
	includeEmptySlots bool
}

type nodeActionTarget struct {
	Device         string  `json:"device"`
	Slot           string  `json:"slot"`
	Phone          *string `json:"phone"`
	BindingVersion int     `json:"binding_version"`
	ICCID          string  `json:"iccid"`
}

type nodePhoneRecord struct {
	Target       nodeActionTarget `json:"target"`
	Available    bool             `json:"available"`
	TunnelState  string           `json:"tunnel_state"`
	Reason       *string          `json:"reason"`
	ObservedAt   string           `json:"observed_at"`
	AID          string           `json:"-"`
	ProfileState string           `json:"-"`
}

func newNodeActionService(database *store.Store, devices *device.Manager, vowifi *vowifiruntime.Manager, multi *multisim.Manager, cards *multiSIMIntegration) *nodeActionService {
	service := &nodeActionService{
		esim:     devices,
		database: database, devices: devices, vowifi: vowifi, multisim: multi, cards: cards,
		mapper:    integration.ATMapper{Store: database, Devices: devices},
		snapshots: make(map[string]phoneSnapshot),
	}
	service.readCard = service.withReader
	return service
}

// setIdentity records the configured node id and payload ceiling. Execute()
// never receives the settings, but node.describe has to report this node's
// outbox depth (keyed by node) and its payload limit.
func (service *nodeActionService) setIdentity(node string, maxPayloadBytes int) {
	service.nodeMu.Lock()
	service.node = strings.TrimSpace(node)
	service.maxPayload = maxPayloadBytes
	service.nodeMu.Unlock()
}

func (service *nodeActionService) nodeName() string {
	service.nodeMu.RLock()
	defer service.nodeMu.RUnlock()
	return service.node
}

// NodeDescriptor is the single source of truth behind both node.hello (emitted
// by the runtime on connect) and node.describe. sms_text_chars mirrors what
// sendSMS actually enforces; advertising anything else would publish a limit
// this node does not have.
func (service *nodeActionService) NodeDescriptor() nodemqtt.Descriptor {
	return nodemqtt.Descriptor{
		NodeType:               "vocat_ec20",
		Capabilities:           nodeCapabilities,
		SMSTextChars:           nodeSMSTextChars,
		InventoryPageSize:      100,
		CardOperationsParallel: 1,
	}
}

// revision hands out a monotonic version per resource, as the protocol's
// tunnel/slot records require. It is process-local: a restart starts the
// sequence again, which the spec permits because only the ordering within a
// connection matters to the server.
func (service *nodeActionService) revision(key string) int {
	value, _ := service.revisions.LoadOrStore(key, new(atomic.Int64))
	return int(value.(*atomic.Int64).Add(1))
}

// withReader runs an eUICC operation against the device's card and hands it the
// PHYSICAL device id — device.Manager is keyed by discovery id, not by the
// configured record id. When a multi-tunnel group owns the reader the operation
// borrows the broker's transaction token, the same one every line's profile
// switch and AKA takes, so a node command cannot collide with a running line.
func (service *nodeActionService) withReader(ctx context.Context, deviceID string, operation func(context.Context, string) error) error {
	if service.cards != nil && service.multisim != nil && service.multisim.Owns(deviceID) {
		return service.cards.WithOwnedReader(ctx, deviceID, operation)
	}
	entry, err := service.mapper.Get(deviceID)
	if err != nil {
		return err
	}
	return operation(ctx, entry.ID)
}

// normalizeICCID drops the padding nibble an ICCID carries when it is read back
// from a 10-byte EF_ICCID field. Ported from the SimHubMq fork's
// internal/simpool, which this port does not otherwise depend on.
func normalizeICCID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 1 && (value[len(value)-1] == 'f' || value[len(value)-1] == 'F') {
		candidate := value[:len(value)-1]
		for _, character := range candidate {
			if character < '0' || character > '9' {
				return value
			}
		}
		return candidate
	}
	return value
}

func (service *nodeActionService) NodeMQTTHealth(ctx context.Context) nodemqtt.HealthSnapshot {
	result := nodemqtt.HealthSnapshot{Health: "ok"}
	configs, err := service.database.ListDevices(ctx)
	if err != nil {
		result.Health = "error"
		return result
	}
	// Presence comes from the device manager's live view — the same source the
	// dashboard uses. The fork asked the device_runtime table, which nothing
	// populates on this build, so every module counted as offline.
	for _, config := range configs {
		if entry, resolveErr := service.mapper.Get(config.ID); resolveErr == nil && entry.Discovered {
			result.DevicesOnline++
		} else {
			result.DevicesOffline++
		}
		if service.multisim != nil && service.multisim.Owns(config.ID) {
			for _, line := range service.multisim.State(config.ID).Lines {
				if line.State.IMSReady {
					result.RegisteredTunnels++
				}
			}
		} else if service.vowifi != nil {
			if state, stateErr := service.vowifi.State(config.ID); stateErr == nil && state.IMSReady {
				result.RegisteredTunnels++
			}
		}
	}
	// An idle module is the normal state on this host: most carry a blank eUICC
	// and sit at CFUN=4 by design, so "any device offline" would pin the node
	// to degraded forever. Degraded means work this node is supposed to be
	// doing is not happening — judge it by the enabled multi-SIM groups.
	expected, stalled := 0, 0
	if groups, groupErr := service.database.ListMultiSIMConfigs(ctx); groupErr == nil {
		for _, group := range groups {
			if !group.Enabled {
				continue
			}
			expected++
			if service.multisim == nil || !service.multisim.Owns(group.DeviceID) ||
				service.multisim.State(group.DeviceID).Phase != "running" {
				stalled++
			}
		}
	}
	if stalled > 0 || (expected > 0 && result.RegisteredTunnels == 0) {
		result.Health = "degraded"
	}
	return result
}

func (service *nodeActionService) Execute(ctx context.Context, command nodemqtt.Command, progress func(string, *int)) (any, *nodemqtt.ActionError) {
	switch command.Action {
	case "node.describe":
		return service.describeNode(ctx, command)
	case "inventory.get":
		return service.getInventory(ctx, command)
	case "phones.list":
		return service.listPhones(ctx, command)
	case "phones.check":
		return service.checkPhone(ctx, command)
	case "slot.refresh":
		return service.refreshSlot(ctx, command)
	case "esim.profiles.list":
		return service.listCardProfiles(ctx, command)
	case "esim.profile.download":
		return service.downloadProfile(ctx, command, progress)
	case "esim.profile.enable":
		return service.enableProfile(ctx, command)
	case "esim.profile.delete":
		return service.deleteProfile(ctx, command)
	case "tunnel.ensure":
		return service.ensureTunnel(ctx, command)
	case "tunnel.stop":
		return service.stopTunnel(ctx, command)
	case "tunnel.reconnect":
		return service.reconnectTunnel(ctx, command)
	case "sms.send":
		return service.sendSMS(ctx, command)
	case "task.get":
		return service.getTask(ctx, command)
	default:
		return nil, reject("UNSUPPORTED_ACTION", "节点未实现该业务动作")
	}
}

func (service *nodeActionService) listPhones(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if command.Target != nil {
		return nil, reject("INVALID_ARGUMENT", "phones.list 不允许 target")
	}
	var params struct {
		PageSize          int    `json:"page_size"`
		Cursor            string `json:"cursor"`
		IncludeEmptySlots bool   `json:"include_empty_slots"`
	}
	if err := decodeNodeParams(command.Params, &params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if params.PageSize == 0 {
		params.PageSize = 50
	}
	if params.PageSize < 1 || params.PageSize > 100 {
		return nil, reject("INVALID_ARGUMENT", "page_size 必须在 1 到 100 之间")
	}

	collection, offset, cursorErr := service.phonePage(ctx, params.Cursor, params.PageSize, params.IncludeEmptySlots)
	if cursorErr != nil {
		return nil, reject("CURSOR_EXPIRED", cursorErr.Error())
	}
	if params.Cursor == "" {
		var failure *nodemqtt.ActionError
		collection, failure = service.collectLivePhones(ctx)
		if failure != nil {
			return nil, failure
		}
		// PhoneRecord consumers require a real ICCID. Blank modules belong to
		// inventory.get; preserve the operator's diagnostic view as an opt-in.
		if !params.IncludeEmptySlots {
			profiles := make([]nodePhoneRecord, 0, len(collection.items))
			for _, item := range collection.items {
				if item.Target.ICCID != "" {
					profiles = append(profiles, item)
				}
			}
			collection.items = profiles
		}
	}
	end := offset + params.PageSize
	if end > len(collection.items) {
		end = len(collection.items)
	}
	items := collection.items[offset:end]
	var next any
	complete := end >= len(collection.items)
	if !complete {
		token := nodeCursorToken()
		if params.Cursor != "" {
			token = strings.SplitN(params.Cursor, ".", 2)[0]
		}
		service.cacheMu.Lock()
		if service.snapshots == nil {
			service.snapshots = make(map[string]phoneSnapshot)
		}
		service.snapshots[token] = phoneSnapshot{collection: collection, expiresAt: time.Now().Add(5 * time.Minute), pageSize: params.PageSize, includeEmptySlots: params.IncludeEmptySlots}
		service.cacheMu.Unlock()
		next = token + "." + strconv.Itoa(end)
	}
	return map[string]any{"source": "hardware", "generated_at": nodemqtt.FormatTime(collection.generatedAt), "items": items, "cursor": next, "complete": complete}, nil
}

func (service *nodeActionService) phonePage(_ context.Context, cursor string, pageSize int, includeEmptySlots bool) (phoneCollection, int, error) {
	if cursor == "" {
		return phoneCollection{}, 0, nil
	}
	parts := strings.SplitN(cursor, ".", 2)
	if len(parts) != 2 {
		return phoneCollection{}, 0, errors.New("分页游标无效")
	}
	offset, err := strconv.Atoi(parts[1])
	if err != nil || offset < 0 {
		return phoneCollection{}, 0, errors.New("分页游标无效")
	}
	service.cacheMu.Lock()
	defer service.cacheMu.Unlock()
	for key, snapshot := range service.snapshots {
		if time.Now().After(snapshot.expiresAt) {
			delete(service.snapshots, key)
		}
	}
	snapshot, ok := service.snapshots[parts[0]]
	if !ok || offset > len(snapshot.collection.items) {
		return phoneCollection{}, 0, errors.New("分页快照已失效")
	}
	if snapshot.pageSize != pageSize {
		return phoneCollection{}, 0, errors.New("后续分页的 page_size 必须与第一页一致")
	}
	if snapshot.includeEmptySlots != includeEmptySlots {
		return phoneCollection{}, 0, errors.New("后续分页的 include_empty_slots 必须与第一页一致")
	}
	return snapshot.collection, offset, nil
}

func (service *nodeActionService) checkPhone(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, true); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	collection, err := service.currentPhones(ctx)
	if err != nil {
		return nil, actionFailure(err, "POOL_OFFLINE")
	}
	record, lookupErr := service.resolveRecord(ctx, *command.Target, collection.items)
	if lookupErr != nil {
		return nil, lookupErr
	}
	return map[string]any{"source": "hardware", "target": record.Target, "available": record.Available, "tunnel_state": record.TunnelState, "reason": record.Reason, "observed_at": nodemqtt.FormatTime(time.Now())}, nil
}

func (service *nodeActionService) sendSMS(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, true); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	var params struct {
		To   string `json:"to"`
		Text string `json:"text"`
	}
	if err := decodeNodeParams(command.Params, &params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	params.To = strings.TrimSpace(params.To)
	if !nodemqtt.ValidateRecipient(params.To) {
		return nil, reject("INVALID_ARGUMENT", "短信收件号码格式无效")
	}
	if params.Text == "" || utf8.RuneCountInString(params.Text) > 4096 {
		return nil, reject("INVALID_ARGUMENT", "短信正文必须包含 1 到 4096 个字符")
	}
	record, config, resolveErr := service.resolveCommandTarget(ctx, *command.Target)
	if resolveErr != nil {
		return nil, resolveErr
	}
	release, lockErr := service.lockResource(ctx, "line:"+config.ID+":"+record.Target.ICCID)
	if lockErr != nil {
		return nil, reject("SLOT_BUSY", "目标号码正在执行其他操作")
	}
	defer release()
	if service.multisim != nil && service.multisim.Owns(config.ID) {
		state := service.multisim.State(config.ID)
		lineReady := false
		for _, line := range state.Lines {
			if strings.EqualFold(line.ICCID, record.Target.ICCID) {
				lineReady = line.State.IMSReady && line.State.SMSReady
				break
			}
		}
		if !lineReady {
			return nil, reject("TUNNEL_NOT_READY", "目标号码 IMS 短信通道未就绪")
		}
		result, identity, err := service.multisim.SendSMS(ctx, config.ID, record.Target.ICCID, vowifi.SMSSubmitRequest{Recipient: params.To, Text: params.Text})
		if err != nil {
			return nil, classifySideEffect(err, "短信发送结果无法确认")
		}
		if !result.AllPartsAccepted {
			return nil, failed("INTERNAL_ERROR", "短信未被 IMS 完整接受")
		}
		service.persistNodeSMS(ctx, command.ID, config, identity.ICCID, identity.IMSI, identity.PhoneNumber, params.To, params.Text, result.SubmittedAt, result.PartsTotal, "ims", map[string]any{"part_results": result.PartResults, "concat_reference": result.ConcatReference})
		return nil, nil
	}
	if service.vowifi != nil {
		state, err := service.vowifi.State(config.ID)
		if err == nil && strings.EqualFold(strings.TrimSpace(state.ICCID), record.Target.ICCID) && state.IMSReady && state.SMSReady {
			result, sendErr := service.vowifi.SendSMS(ctx, config.ID, vowifi.SMSSubmitRequest{Recipient: params.To, Text: params.Text})
			if sendErr != nil {
				return nil, classifySideEffect(sendErr, "短信发送结果无法确认")
			}
			if !result.AllPartsAccepted {
				return nil, failed("INTERNAL_ERROR", "短信未被 IMS 完整接受")
			}
			service.persistNodeSMS(ctx, command.ID, config, record.Target.ICCID, state.IMSI, derefPhone(record.Target.Phone), params.To, params.Text, result.SubmittedAt, result.PartsTotal, "ims", map[string]any{"part_results": result.PartResults, "concat_reference": result.ConcatReference})
			return nil, nil
		}
	}
	entry, err := service.devices.Get(config.ID)
	if err != nil || entry.Snapshot == nil || !strings.EqualFold(strings.TrimSpace(entry.Snapshot.ICCID), record.Target.ICCID) {
		return nil, reject("TUNNEL_NOT_READY", "目标号码没有可用的现有发送通道")
	}
	result, err := service.devices.SendSMS(ctx, config.ID, params.To, params.Text)
	if err != nil {
		return nil, classifySideEffect(err, "短信发送结果无法确认")
	}
	if !result.AllPartsAccepted {
		return nil, failed("INTERNAL_ERROR", "短信未被设备完整接受")
	}
	service.persistNodeSMS(ctx, command.ID, config, record.Target.ICCID, entry.Snapshot.IMSI, derefPhone(record.Target.Phone), params.To, params.Text, result.SubmittedAt, result.PartsTotal, "modem", map[string]any{"message_reference": result.MessageReference, "part_results": result.PartResults, "concat_reference": result.ConcatReference})
	return nil, nil
}

func (service *nodeActionService) downloadProfile(ctx context.Context, command nodemqtt.Command, progress func(string, *int)) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, false); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	var params struct {
		ActivationCode   string `json:"activation_code"`
		ConfirmationCode string `json:"confirmation_code,omitempty"`
	}
	if err := decodeNodeParams(command.Params, &params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if len([]rune(params.ActivationCode)) < 1 || len([]rune(params.ActivationCode)) > 4096 {
		return nil, reject("INVALID_ARGUMENT", "activation_code 长度必须在 1 到 4096 之间")
	}
	if len([]rune(params.ConfirmationCode)) > 256 {
		return nil, reject("INVALID_ARGUMENT", "confirmation_code 最多 256 个字符")
	}
	config, targetErr := service.validateDeviceTarget(ctx, *command.Target)
	if targetErr != nil {
		return nil, targetErr
	}
	release, lockErr := service.lockResource(ctx, "device:"+config.ID)
	if lockErr != nil {
		return nil, reject("SLOT_BUSY", "目标卡槽正在执行其他操作")
	}
	defer release()
	smdp, matchingID, parseErr := parseActivationCode(params.ActivationCode)
	if parseErr != nil {
		return nil, reject("INVALID_ARGUMENT", parseErr.Error())
	}
	controller := service.esimController(config)
	if controller == nil {
		return nil, reject("UNSUPPORTED_ACTION", "目标设备不支持 eSIM 下载")
	}
	var profile *device.EsimProfile
	var confirmationErr error
	err := service.readCard(ctx, config.ID, func(ctx context.Context, physicalID string) error {
		result, downloadErr := controller.ESIMDownloadProfile(ctx, physicalID, device.EsimDownloadParams{SMDP: smdp, MatchingID: matchingID, ConfirmationCode: params.ConfirmationCode, IMEI: config.ModemIMEI}, func(value device.EsimProgress) { pct := value.Pct; progress(value.Step, &pct) })
		if downloadErr != nil {
			return downloadErr
		}
		if result == nil || normalizeICCID(result.ICCID) == "" {
			confirmationErr = errors.New("downloaded ICCID unavailable")
			return nil
		}
		// Keep the same pinned reader and AKA transaction through readback.
		info, readErr := controller.ESIMListProfiles(ctx, physicalID)
		if readErr != nil {
			confirmationErr = readErr
			return nil
		}
		for _, candidate := range info.Profiles {
			if normalizeICCID(candidate.ICCID) == normalizeICCID(result.ICCID) {
				candidate.ICCID = normalizeICCID(candidate.ICCID)
				profile = &candidate
				break
			}
		}
		return nil
	})
	service.clearPhoneCache()
	if err != nil {
		return nil, classifySideEffect(err, "写卡结果无法确认")
	}
	if confirmationErr != nil || profile == nil {
		return nil, uncertain("DOWNLOAD_FAILED", "下载命令已完成，但无法核实新增 Profile；请读取卡片确认，不要重新下载")
	}
	state := nodeProfileState(*profile)
	// Download only installs the profile. A separate tunnel.ensure opts it
	// into the running group without restarting existing lines.
	return map[string]any{"binding_version": 1, "iccid": profile.ICCID, "written": true, "profile_state": state}, nil
}

func nodeProfileState(profile device.EsimProfile) string {
	state := strings.ToLower(strings.TrimSpace(profile.StateText))
	if state == "enabled" || state == "disabled" {
		return state
	}
	switch profile.State {
	case 0:
		return "disabled"
	case 1:
		return "enabled"
	default:
		return "unknown"
	}
}

func (service *nodeActionService) deleteProfile(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, true); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	record, config, resolveErr := service.resolveCommandTarget(ctx, *command.Target)
	if resolveErr != nil {
		return nil, resolveErr
	}
	release, lockErr := service.lockResource(ctx, "device:"+config.ID)
	if lockErr != nil {
		return nil, reject("SLOT_BUSY", "目标卡槽正在执行其他操作")
	}
	defer release()
	if record.Available || service.profileRuntimeActive(config.ID, record.Target.ICCID) {
		return nil, reject("PROFILE_IN_USE", "目标号码隧道仍在线，请先关闭隧道")
	}
	controller := service.esimController(config)
	if controller == nil {
		return nil, reject("UNSUPPORTED_ACTION", "目标设备不支持删除 eSIM Profile")
	}
	if _, err := controller.ESIMDeleteProfile(ctx, config.ID, record.Target.ICCID, record.AID); err != nil {
		return nil, classifySideEffect(err, "删除结果无法确认")
	}
	resultValue := map[string]any{"binding_version": 1, "iccid": record.Target.ICCID, "deleted": true}
	if err := service.removeDeletedProfile(ctx, config.ID, record.Target.ICCID); err != nil {
		service.clearPhoneCache()
		return nil, &nodemqtt.ActionError{State: "failed", Code: "INTERNAL_ERROR", Message: "Profile 已删除，但多隧道配置同步失败", Certainty: "known_failed", Result: resultValue}
	}
	service.clearPhoneCache()
	return resultValue, nil
}

// nodeCapabilities is what node.describe advertises. Keep it in step with the
// Execute switch above; the server uses it to decide what it may send.
// nodeSMSTextChars is the ceiling sendSMS enforces on params.text.
const nodeSMSTextChars = 4096

var nodeCapabilities = []string{
	"node.describe", "inventory.get", "phones.list", "phones.check", "slot.refresh",
	"esim.profiles.list", "esim.profile.download", "esim.profile.enable", "esim.profile.delete",
	"tunnel.ensure", "tunnel.stop", "tunnel.reconnect", "sms.send", "task.get",
}

func (service *nodeActionService) describeNode(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if command.Target != nil {
		return nil, reject("INVALID_ARGUMENT", "node.describe 不允许 target")
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	pending := 0
	if node := service.nodeName(); node != "" {
		if value, err := service.database.CountPendingNodeMQTTOutbox(ctx, node); err == nil {
			pending = value
		}
	}
	service.nodeMu.RLock()
	maxPayload := service.maxPayload
	service.nodeMu.RUnlock()
	if maxPayload <= 0 {
		maxPayload = 131072
	}
	return service.NodeDescriptor().Payload(maxPayload, pending), nil
}

func (service *nodeActionService) getTask(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if command.Target != nil {
		return nil, reject("INVALID_ARGUMENT", "task.get 不允许 target")
	}
	var params struct {
		ID string `json:"id"`
	}
	if err := decodeNodeParams(command.Params, &params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	wanted := strings.TrimSpace(params.ID)
	if wanted == "" {
		return nil, reject("INVALID_ARGUMENT", "params.id 必填")
	}
	if wanted == command.ID {
		return nil, reject("INVALID_ARGUMENT", "params.id 不能与本次查询 id 相同")
	}
	task, err := service.database.NodeMQTTTask(ctx, wanted)
	if errors.Is(err, store.ErrNotFound) {
		return nil, reject("TASK_NOT_FOUND", "未找到该任务")
	}
	if err != nil {
		return nil, failed("INTERNAL_ERROR", "读取任务失败")
	}
	record := map[string]any{
		"id": task.ID, "action": task.Action, "seq": task.LatestSeq,
		"time": nodemqtt.FormatTime(task.UpdatedAt), "state": task.State,
		"result": json.RawMessage("null"), "error": json.RawMessage("null"),
	}
	if len(task.Result) > 0 {
		record["result"] = task.Result
	}
	if len(task.Error) > 0 {
		record["error"] = task.Error
	}
	var request nodemqtt.Command
	if err := json.Unmarshal(task.RequestJSON, &request); err == nil && request.Target != nil {
		record["target"] = request.Target
	} else {
		record["target"] = nil
	}
	return map[string]any{"task": record}, nil
}

// slotRecord builds the protocol's SlotRecord, shared by inventory.get and
// slot.refresh. A device we cannot resolve or read is reported with its stored
// profiles and a non-ready state rather than failing the whole action.
// slotRecord builds the protocol's SlotRecord. live reads the card; bulk
// callers pass false so a sweep cannot contend with every running line.
func (service *nodeActionService) slotRecord(ctx context.Context, config store.Device, live bool) map[string]any {
	observed := time.Now().UTC()
	slot, slotErr := service.deviceSlot(config)
	record := map[string]any{
		"device": config.ID, "slot": slot, "binding_version": 1,
		"eid": nil, "deleted": false,
		"observed_at": nodemqtt.FormatTime(observed),
		"revision":    service.revision("slot:" + config.ID),
	}
	if slotErr != nil {
		record["slot"] = ""
		record["state"] = "pending"
		record["current_iccid"] = nil
		record["profiles"] = []any{}
		record["profiles_complete"] = false
		record["profiles_observed_at"] = nodemqtt.FormatTime(observed)
		record["tunnels"] = []any{}
		return record
	}
	profiles, listErr := service.deviceProfiles(ctx, config, live)
	record["profiles_complete"] = listErr == nil
	record["profiles_observed_at"] = nodemqtt.FormatTime(observed)
	var current *string
	items := make([]any, 0, len(profiles))
	for _, profile := range profiles {
		iccid := normalizeICCID(profile.ICCID)
		if iccid == "" {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(profile.StateText))
		if state != "enabled" && state != "disabled" {
			if profile.State == 1 {
				state = "enabled"
			} else {
				state = "disabled"
			}
		}
		if state == "enabled" {
			value := iccid
			current = &value
		}
		phone, _ := service.database.PhoneNumberForICCID(ctx, iccid)
		var phonePointer *string
		if nodemqtt.ValidE164(phone) {
			value := phone
			phonePointer = &value
		}
		items = append(items, map[string]any{
			"iccid": iccid, "name": firstNonEmpty(profile.Name, profile.ServiceProvider),
			"phone": phonePointer, "state": state,
		})
	}
	record["profiles"] = items
	record["current_iccid"] = current
	switch {
	case listErr != nil:
		record["state"] = "error"
	case len(items) == 0:
		record["state"] = "empty"
	default:
		record["state"] = "ready"
	}
	tunnels := make([]any, 0, len(items))
	if service.multisim != nil && service.multisim.Owns(config.ID) {
		for _, line := range service.multisim.State(config.ID).Lines {
			iccid := normalizeICCID(line.ICCID)
			state, _, _ := nodeTunnelState(line.State)
			tunnels = append(tunnels, map[string]any{
				"iccid": iccid, "state": state,
				"observed_at": nodemqtt.FormatTime(observed),
				"revision":    service.revision("tunnel:" + config.ID + ":" + iccid),
			})
		}
	}
	record["tunnels"] = tunnels
	return record
}

func (service *nodeActionService) refreshSlot(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, false); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	config, targetErr := service.validateDeviceTarget(ctx, *command.Target)
	if targetErr != nil {
		return nil, targetErr
	}
	service.clearPhoneCache()
	record := service.slotRecord(ctx, config, true)
	// Say where the profiles actually came from: a live read can fall back to
	// the stored list when the reader is busy, and claiming "hardware" then
	// would misreport the card.
	source := "registry"
	if complete, _ := record["profiles_complete"].(bool); complete {
		source = "hardware"
	}
	return map[string]any{"source": source, "slot": record}, nil
}

func (service *nodeActionService) listCardProfiles(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, false); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	config, targetErr := service.validateDeviceTarget(ctx, *command.Target)
	if targetErr != nil {
		return nil, targetErr
	}
	record := service.slotRecord(ctx, config, true)
	source := "registry"
	if complete, _ := record["profiles_complete"].(bool); complete {
		source = "hardware"
	}
	return map[string]any{
		"source": source, "binding_version": 1,
		"current_iccid": record["current_iccid"], "profiles": record["profiles"],
		"complete":    record["profiles_complete"],
		"observed_at": record["profiles_observed_at"],
		"revision":    service.revision("profiles:" + config.ID),
	}, nil
}

func (service *nodeActionService) enableProfile(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, true); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nil, reject("INVALID_ARGUMENT", err.Error())
	}
	record, config, resolveErr := service.resolveCommandTarget(ctx, *command.Target)
	if resolveErr != nil {
		return nil, resolveErr
	}
	release, lockErr := service.lockResource(ctx, "device:"+config.ID)
	if lockErr != nil {
		return nil, reject("SLOT_BUSY", "目标卡槽正在执行其他操作")
	}
	defer release()
	err := service.readCard(ctx, config.ID, func(ctx context.Context, physicalID string) error {
		return service.devices.ESIMSwitchProfile(ctx, physicalID, record.Target.ICCID, record.AID)
	})
	if err != nil {
		// The card refuses to enable a profile that is already enabled
		// ("profile is not currently disabled"). The requested end state then
		// already holds, and the protocol has no error for it — enabling the
		// current profile must be idempotent — so confirm against the card and
		// report success. Only a genuine failure is reported as one.
		if profiles, listErr := service.deviceProfiles(ctx, config, true); listErr == nil {
			for _, candidate := range profiles {
				if normalizeICCID(candidate.ICCID) != record.Target.ICCID {
					continue
				}
				state := strings.ToLower(strings.TrimSpace(candidate.StateText))
				if state == "enabled" || candidate.State == 1 {
					service.clearPhoneCache()
					return map[string]any{
						"binding_version": 1, "current_iccid": record.Target.ICCID, "profile_state": "enabled",
					}, nil
				}
				break
			}
		}
		return nil, classifySideEffect(err, "切换结果无法确认")
	}
	service.clearPhoneCache()
	return map[string]any{
		"binding_version": 1, "current_iccid": record.Target.ICCID, "profile_state": "enabled",
	}, nil
}

// tunnelResult is the shared reply of tunnel.ensure / stop / reconnect.
func (service *nodeActionService) tunnelResult(deviceID, iccid, state string, changed bool) map[string]any {
	return map[string]any{
		"binding_version": 1, "iccid": iccid, "state": state,
		"observed_at": nodemqtt.FormatTime(time.Now().UTC()),
		"revision":    service.revision("tunnel:" + deviceID + ":" + iccid),
		"changed":     changed,
	}
}

// awaitTunnel polls until the line reaches (or leaves) the registered state, or
// the action's own deadline expires. IMS registration is seconds-to-minutes
// work, so there is nothing faster to wait on here.
func (service *nodeActionService) awaitTunnel(ctx context.Context, deviceID, iccid string, want bool) bool {
	for {
		state, available, _, _ := service.tunnelStatus(deviceID, iccid)
		if want && available {
			return true
		}
		if !want && state == "stopped" {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
}

func (service *nodeActionService) tunnelTarget(ctx context.Context, command nodemqtt.Command) (nodePhoneRecord, store.Device, *nodemqtt.ActionError) {
	if err := nodemqtt.ValidateTarget(command.Target, true); err != nil {
		return nodePhoneRecord{}, store.Device{}, reject("INVALID_ARGUMENT", err.Error())
	}
	if err := requireEmptyParams(command.Params); err != nil {
		return nodePhoneRecord{}, store.Device{}, reject("INVALID_ARGUMENT", err.Error())
	}
	return service.resolveCommandTarget(ctx, *command.Target)
}

func (service *nodeActionService) ensureTunnel(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	record, config, resolveErr := service.tunnelTarget(ctx, command)
	if resolveErr != nil {
		return nil, resolveErr
	}
	iccid := record.Target.ICCID
	if _, available, _, _ := service.tunnelStatus(config.ID, iccid); available {
		return service.tunnelResult(config.ID, iccid, "registered", false), nil
	}
	if service.multisim == nil || !service.multisim.Owns(config.ID) {
		return nil, reject("UNSUPPORTED_ACTION", "该设备没有运行中的多隧道组")
	}
	release, lockErr := service.lockResource(ctx, "device:"+config.ID)
	if lockErr != nil {
		return nil, reject("SLOT_BUSY", "目标卡槽正在执行其他操作")
	}
	group := service.multisim.State(config.ID)
	if group.Phase != "running" || group.Busy {
		release()
		return nil, reject("SLOT_BUSY", "多隧道组尚未就绪")
	}
	exists := false
	for _, line := range group.Lines {
		if normalizeICCID(line.ICCID) == iccid {
			exists = true
			break
		}
	}
	var startErr error
	if exists {
		startErr = service.multisim.Reconnect(config.ID, iccid)
	} else {
		startErr = service.ensureProfileConfigured(ctx, config.ID, device.EsimProfile{ICCID: iccid, AID: record.AID})
	}
	release()
	service.clearPhoneCache()
	if err := startErr; err != nil {
		if errors.Is(err, multisim.ErrOperationInProgress) {
			return nil, reject("SLOT_BUSY", "多隧道组正在执行其他操作")
		}
		return nil, classifySideEffect(err, "隧道建立结果无法确认")
	}
	if !service.awaitTunnel(ctx, config.ID, iccid, true) {
		return nil, uncertain("TUNNEL_NOT_READY", "隧道已请求建立，但未在期限内确认注册")
	}
	return service.tunnelResult(config.ID, iccid, "registered", true), nil
}

func (service *nodeActionService) stopTunnel(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	record, config, resolveErr := service.tunnelTarget(ctx, command)
	if resolveErr != nil {
		return nil, resolveErr
	}
	iccid := record.Target.ICCID
	if state, _, _, _ := service.tunnelStatus(config.ID, iccid); state == "stopped" {
		return service.tunnelResult(config.ID, iccid, "stopped", false), nil
	}
	if service.multisim == nil || !service.multisim.Owns(config.ID) {
		return nil, reject("UNSUPPORTED_ACTION", "该设备没有运行中的多隧道组")
	}
	if err := service.multisim.Disconnect(config.ID, iccid); err != nil {
		if errors.Is(err, multisim.ErrOperationInProgress) {
			return nil, reject("SLOT_BUSY", "多隧道组正在执行其他操作")
		}
		return nil, classifySideEffect(err, "隧道停止结果无法确认")
	}
	if !service.awaitTunnel(ctx, config.ID, iccid, false) {
		return nil, uncertain("TUNNEL_STOP_UNCONFIRMED", "已请求停止，但未在期限内确认")
	}
	return service.tunnelResult(config.ID, iccid, "stopped", true), nil
}

func (service *nodeActionService) reconnectTunnel(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	record, config, resolveErr := service.tunnelTarget(ctx, command)
	if resolveErr != nil {
		return nil, resolveErr
	}
	iccid := record.Target.ICCID
	if service.multisim == nil || !service.multisim.Owns(config.ID) {
		return nil, reject("UNSUPPORTED_ACTION", "该设备没有运行中的多隧道组")
	}
	// Unlike ensure, reconnect always rebuilds, so an already-registered line is
	// torn down and established again.
	if err := service.multisim.Reconnect(config.ID, iccid); err != nil {
		if errors.Is(err, multisim.ErrOperationInProgress) {
			return nil, reject("SLOT_BUSY", "多隧道组正在执行其他操作")
		}
		return nil, classifySideEffect(err, "隧道重建结果无法确认")
	}
	if !service.awaitTunnel(ctx, config.ID, iccid, true) {
		return nil, uncertain("TUNNEL_NOT_READY", "隧道已请求重建，但未在期限内确认注册")
	}
	return service.tunnelResult(config.ID, iccid, "registered", true), nil
}

type nodeESIMController interface {
	ESIMListProfiles(context.Context, string) (device.EsimInfo, error)
	ESIMDownloadProfile(context.Context, string, device.EsimDownloadParams, func(device.EsimProgress)) (*device.EsimDownloadResult, error)
	ESIMDeleteProfile(context.Context, string, string, string) (*device.EsimDeleteResult, error)
}

func (service *nodeActionService) esimController(_ store.Device) nodeESIMController {
	return service.esim
}

func (service *nodeActionService) currentPhones(ctx context.Context) (phoneCollection, error) {
	service.cacheMu.Lock()
	if !service.phoneCache.generatedAt.IsZero() && time.Since(service.phoneCache.generatedAt) <= 30*time.Second {
		cached := clonePhoneCollection(service.phoneCache)
		service.cacheMu.Unlock()
		return cached, nil
	}
	service.cacheMu.Unlock()
	result := service.phoneGroup.DoChan("all", func() (any, error) {
		service.cacheMu.Lock()
		if !service.phoneCache.generatedAt.IsZero() && time.Since(service.phoneCache.generatedAt) <= 30*time.Second {
			cached := clonePhoneCollection(service.phoneCache)
			service.cacheMu.Unlock()
			return cached, nil
		}
		service.cacheMu.Unlock()
		refreshCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		collection, err := service.collectPhones(refreshCtx)
		if err != nil {
			return phoneCollection{}, err
		}
		service.cacheMu.Lock()
		service.phoneCache = clonePhoneCollection(collection)
		service.cacheMu.Unlock()
		return collection, nil
	})
	select {
	case <-ctx.Done():
		return phoneCollection{}, ctx.Err()
	case outcome := <-result:
		if outcome.Err != nil {
			return phoneCollection{}, outcome.Err
		}
		return outcome.Val.(phoneCollection), nil
	}
}

func (service *nodeActionService) collectPhones(ctx context.Context) (phoneCollection, error) {
	configs, err := service.database.ListDevices(ctx)
	if err != nil {
		return phoneCollection{}, err
	}
	result := phoneCollection{generatedAt: time.Now().UTC(), items: []nodePhoneRecord{}}
	failures := 0
	for _, config := range configs {
		if err := ctx.Err(); err != nil {
			failures++
			result.items = append(result.items, service.cachedUnavailableRecords(config.ID, "设备资料刷新超时")...)
			continue
		}
		slot, slotErr := service.deviceSlot(config)
		if slotErr != nil {
			continue
		}
		release, lockErr := service.lockResource(ctx, "device:"+config.ID)
		if lockErr != nil {
			failures++
			result.items = append(result.items, service.cachedUnavailableRecords(config.ID, "设备正忙，资料未刷新")...)
			continue
		}
		profiles, listErr := service.deviceProfiles(ctx, config, false)
		release()
		if listErr != nil {
			failures++
			result.items = append(result.items, service.cachedUnavailableRecords(config.ID, "设备资料读取失败")...)
			continue
		}
		for _, profile := range profiles {
			iccid := normalizeICCID(profile.ICCID)
			if iccid == "" {
				continue
			}
			phone, _ := service.database.PhoneNumberForICCID(ctx, iccid)
			var phonePointer *string
			if nodemqtt.ValidE164(phone) {
				value := phone
				phonePointer = &value
			}
			tunnelState, available, reason, observed := service.tunnelStatus(config.ID, iccid)
			result.items = append(result.items, nodePhoneRecord{Target: nodeActionTarget{Device: config.ID, Slot: slot, Phone: phonePointer, BindingVersion: 1, ICCID: iccid}, Available: available, TunnelState: tunnelState, Reason: reason, ObservedAt: nodemqtt.FormatTime(observed), AID: profile.AID, ProfileState: profile.StateText})
		}
	}
	if len(result.items) == 0 && failures > 0 {
		return phoneCollection{}, fmt.Errorf("phone inventory unavailable on %d device(s)", failures)
	}
	result.generatedAt = time.Now().UTC()
	return result, nil
}

func (service *nodeActionService) cachedUnavailableRecords(deviceID, message string) []nodePhoneRecord {
	service.cacheMu.Lock()
	defer service.cacheMu.Unlock()
	var result []nodePhoneRecord
	for _, record := range service.phoneCache.items {
		if record.Target.Device != deviceID {
			continue
		}
		copy := record
		copy.Available = false
		copy.Reason = &message
		result = append(result, copy)
	}
	return result
}

// deviceProfiles returns a device's profile list. live reads the card, which
// takes the group's reader transaction; bulk callers must pass false, or a
// sweep over every configured device would contend with each running line's
// AKA and profile switches.
func (service *nodeActionService) deviceProfiles(ctx context.Context, config store.Device, live bool) ([]device.EsimProfile, error) {
	controller := service.esimController(config)
	if live && controller != nil {
		var info device.EsimInfo
		err := service.readCard(ctx, config.ID, func(ctx context.Context, physicalID string) error {
			var readErr error
			info, readErr = controller.ESIMListProfiles(ctx, physicalID)
			return readErr
		})
		if err == nil {
			// Card operations and multisim.Config use the ISD-R address,
			// not each profile's ISD-P application address.
			for index := range info.Profiles {
				info.Profiles[index].AID = info.AID
			}
			return info.Profiles, nil
		}
		// A restore or a re-attachment can take the borrowed reader back mid
		// request. That is not a card fault, so fall back to the stored profile
		// list instead of failing the whole action.
		if !errors.Is(err, errMultiSIMReaderNotLendable) {
			return nil, err
		}
	}
	if cfg, err := service.database.MultiSIMConfig(ctx, config.ID); err == nil {
		profiles := make([]device.EsimProfile, 0, len(cfg.Profiles))
		for _, profile := range cfg.Profiles {
			profiles = append(profiles, device.EsimProfile{ICCID: profile.ICCID, AID: profile.AID, Name: profile.Name})
		}
		return profiles, nil
	}
	return nil, errors.New("profile inventory unavailable")
}

func (service *nodeActionService) tunnelStatus(deviceID, iccid string) (string, bool, *string, time.Time) {
	now := time.Now().UTC()
	if service.multisim != nil && service.multisim.Owns(deviceID) {
		group := service.multisim.State(deviceID)
		for _, line := range group.Lines {
			if strings.EqualFold(line.ICCID, iccid) {
				state, available, reason := nodeTunnelState(line.State)
				return state, available, reason, now
			}
		}
		reason := "隧道未开启"
		return "stopped", false, &reason, now
	}
	if service.vowifi != nil {
		state, err := service.vowifi.State(deviceID)
		if err == nil && strings.EqualFold(strings.TrimSpace(state.ICCID), iccid) {
			tunnel, available, reason := nodeTunnelState(state)
			return tunnel, available, reason, now
		}
	}
	reason := "隧道未开启"
	return "stopped", false, &reason, now
}

func nodeTunnelState(state vowifi.State) (string, bool, *string) {
	if state.IMSReady && state.SMSReady {
		return "registered", true, nil
	}
	if state.IMSReady {
		reason := "短信发送通道未就绪"
		return "registered", false, &reason
	}
	var tunnel, message string
	switch {
	case state.Phase == vowifi.PhaseFailed:
		tunnel, message = "failed", firstNonEmpty(state.LastError, "隧道建立失败")
	case state.Attempt > 1 && state.Enabled:
		tunnel, message = "reconnecting", "正在重连"
	case state.Phase == vowifi.PhaseSIMReady || state.Phase == vowifi.PhaseAccessReady:
		tunnel, message = "authenticating", "正在鉴权"
	case state.Active || state.Enabled:
		tunnel, message = "starting", "隧道正在启动"
	default:
		tunnel, message = "stopped", "隧道未开启"
	}
	return tunnel, false, &message
}

func (service *nodeActionService) resolveCommandTarget(ctx context.Context, target nodemqtt.Target) (nodePhoneRecord, store.Device, *nodemqtt.ActionError) {
	config, targetErr := service.validateDeviceTarget(ctx, target)
	if targetErr != nil {
		return nodePhoneRecord{}, store.Device{}, targetErr
	}
	collection, err := service.currentPhones(ctx)
	if err != nil {
		return nodePhoneRecord{}, store.Device{}, actionFailure(err, "POOL_OFFLINE")
	}
	record, resolveErr := service.resolveRecord(ctx, target, collection.items)
	if resolveErr == nil || target.ICCID == "" || (resolveErr.Code != "PROFILE_NOT_FOUND" && resolveErr.Code != "PHONE_NOT_FOUND") {
		return record, config, resolveErr
	}
	// A downloaded profile need not be part of the desired running group.
	// Explicit ICCID operations read the card before declaring it missing.
	profiles, readErr := service.deviceProfiles(ctx, config, true)
	if readErr != nil {
		return nodePhoneRecord{}, config, actionFailure(readErr, "POOL_OFFLINE")
	}
	var records []nodePhoneRecord
	for _, profile := range profiles {
		iccid := normalizeICCID(profile.ICCID)
		if iccid != normalizeICCID(target.ICCID) {
			continue
		}
		phone, _ := service.database.PhoneNumberForICCID(ctx, iccid)
		var phonePointer *string
		if nodemqtt.ValidE164(phone) {
			phonePointer = &phone
		}
		records = append(records, nodePhoneRecord{Target: nodeActionTarget{Device: config.ID, Slot: target.Slot, ICCID: iccid, Phone: phonePointer, BindingVersion: 1}, AID: profile.AID, ProfileState: nodeProfileState(profile)})
	}
	record, resolveErr = service.resolveRecord(ctx, target, records)
	return record, config, resolveErr
}

func (service *nodeActionService) resolveRecord(_ context.Context, target nodemqtt.Target, items []nodePhoneRecord) (nodePhoneRecord, *nodemqtt.ActionError) {
	var candidates []nodePhoneRecord
	for _, item := range items {
		if item.Target.Device != target.Device || item.Target.Slot != target.Slot {
			continue
		}
		if target.ICCID != "" && !strings.EqualFold(item.Target.ICCID, normalizeICCID(target.ICCID)) {
			continue
		}
		if target.Phone != nil && (item.Target.Phone == nil || *item.Target.Phone != strings.TrimSpace(*target.Phone)) {
			continue
		}
		candidates = append(candidates, item)
	}
	if len(candidates) == 0 {
		if target.Phone != nil {
			return nodePhoneRecord{}, reject("PHONE_NOT_FOUND", "当前槽位没有该手机号的可信匹配")
		}
		return nodePhoneRecord{}, reject("PROFILE_NOT_FOUND", "目标 Profile 不存在")
	}
	if len(candidates) > 1 {
		return nodePhoneRecord{}, reject("PHONE_AMBIGUOUS", "手机号对应多个 Profile，无法唯一定位")
	}
	if target.ICCID != "" && target.Phone != nil && (candidates[0].Target.Phone == nil || *candidates[0].Target.Phone != *target.Phone) {
		return nodePhoneRecord{}, reject("TARGET_MISMATCH", "phone 与 iccid 不指向同一 Profile")
	}
	return candidates[0], nil
}

func (service *nodeActionService) validateDeviceTarget(ctx context.Context, target nodemqtt.Target) (store.Device, *nodemqtt.ActionError) {
	config, err := service.database.Device(ctx, target.Device)
	if errors.Is(err, store.ErrNotFound) {
		return store.Device{}, reject("UNAUTHORIZED_TARGET", "当前节点不拥有目标设备")
	}
	if err != nil {
		return store.Device{}, failed("INTERNAL_ERROR", "读取目标设备失败")
	}
	slot, slotErr := service.deviceSlot(config)
	if slotErr != nil {
		return store.Device{}, reject("IDENTITY_PENDING", "设备槽位身份尚未核实")
	}
	if slot != target.Slot {
		return store.Device{}, reject("UNAUTHORIZED_TARGET", "target.slot 与设备绑定不一致")
	}
	return config, nil
}

// deviceSlot is the module IMEI: every device on this node is a USB EC20.
// The SimHubMq fork also accepted an MQTT card-pool slot index here; this node
// has no card-pool hardware, so that branch is gone.
func (service *nodeActionService) deviceSlot(config store.Device) (string, error) {
	if value := strings.TrimSpace(config.ModemIMEI); value != "" {
		return value, nil
	}
	entry, err := service.devices.Get(config.ID)
	if err != nil || entry.Snapshot == nil || strings.TrimSpace(entry.Snapshot.IMEI) == "" {
		return "", errors.New("IMEI unavailable")
	}
	return strings.TrimSpace(entry.Snapshot.IMEI), nil
}

func (service *nodeActionService) profileRuntimeActive(deviceID, iccid string) bool {
	if service.multisim != nil && service.multisim.Owns(deviceID) {
		for _, line := range service.multisim.State(deviceID).Lines {
			if strings.EqualFold(line.ICCID, iccid) && (line.State.Active || line.State.IMSReady) {
				return true
			}
		}
	}
	if service.vowifi != nil {
		if state, err := service.vowifi.State(deviceID); err == nil && strings.EqualFold(state.ICCID, iccid) && (state.Active || state.IMSReady) {
			return true
		}
	}
	return false
}

func (service *nodeActionService) ensureProfileConfigured(ctx context.Context, deviceID string, profile device.EsimProfile) error {
	cfg, err := service.database.MultiSIMConfig(ctx, deviceID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil || !cfg.Enabled {
		return err
	}
	previous := cfg
	previous.Profiles = append([]store.MultiSIMProfile(nil), cfg.Profiles...)
	found := false
	for _, existing := range cfg.Profiles {
		if normalizeICCID(existing.ICCID) == profile.ICCID {
			found = true
			break
		}
	}
	if !found {
		if strings.TrimSpace(profile.AID) == "" {
			return errors.New("downloaded profile AID is unavailable")
		}
		cfg.Profiles = append(cfg.Profiles, store.MultiSIMProfile{ICCID: profile.ICCID, AID: profile.AID, Name: firstNonEmpty(profile.Name, profile.ServiceProvider)})
		if _, err := service.database.SaveMultiSIMConfig(ctx, cfg); err != nil {
			return err
		}
	}
	// Our multisim manager has no AddProfile/RemoveProfile; it reconciles a
	// running group against a whole desired profile list, so re-Apply the saved
	// config and let profileDiff() add just the new line.
	if err := service.multisim.Apply(ctx, runtimeMultiSIMConfigForNode(cfg)); err != nil {
		if !found {
			rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if _, rollbackErr := service.database.SaveMultiSIMConfig(rollbackCtx, previous); rollbackErr != nil {
				return fmt.Errorf("group apply failed: %v; saved config rollback failed: %w", err, rollbackErr)
			}
		}
		return err
	}
	return nil
}

// runtimeMultiSIMConfigForNode mirrors the server package's
// runtimeMultiSIMConfig, which lives in another package.
func runtimeMultiSIMConfigForNode(v store.MultiSIMConfig) multisim.Config {
	out := multisim.Config{DeviceID: v.DeviceID, Enabled: v.Enabled, Profiles: make([]multisim.Profile, 0, len(v.Profiles))}
	for _, p := range v.Profiles {
		out.Profiles = append(out.Profiles, multisim.Profile{ICCID: p.ICCID, AID: p.AID, Name: p.Name})
	}
	return out
}

func (service *nodeActionService) removeDeletedProfile(ctx context.Context, deviceID, iccid string) error {
	cfg, err := service.database.MultiSIMConfig(ctx, deviceID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	profiles := cfg.Profiles[:0]
	for _, profile := range cfg.Profiles {
		if normalizeICCID(profile.ICCID) != iccid {
			profiles = append(profiles, profile)
		}
	}
	cfg.Profiles = profiles
	if len(profiles) == 0 {
		cfg.Enabled = false
	}
	if _, err := service.database.SaveMultiSIMConfig(ctx, cfg); err != nil {
		return err
	}
	if service.multisim != nil && service.multisim.Owns(deviceID) {
		if !cfg.Enabled {
			return service.multisim.Apply(ctx, multisim.Config{DeviceID: deviceID, Enabled: false})
		}
		return service.multisim.Apply(ctx, runtimeMultiSIMConfigForNode(cfg))
	}
	return nil
}

func (service *nodeActionService) persistNodeSMS(ctx context.Context, taskID string, config store.Device, iccid, imsi, localPhone, peer, body string, timestamp time.Time, parts int, source string, extraValue any) {
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	extra, _ := json.Marshal(extraValue)
	_, _ = service.database.SaveSMSMessage(ctx, store.SMSMessage{MessageID: "node-mqtt:" + taskID, DeviceID: config.ID, ModemIMEI: config.ModemIMEI, ICCID: iccid, IMSI: imsi, LocalPhone: localPhone, Peer: peer, Direction: "outbound", Body: body, Timestamp: timestamp, Status: "submitted", Source: source, PartsTotal: parts, DeliveryState: "accepted", Read: true, Extra: extra})
}

func (service *nodeActionService) clearPhoneCache() {
	service.cacheMu.Lock()
	service.phoneCache = phoneCollection{}
	service.cacheMu.Unlock()
}

func (service *nodeActionService) lockResource(ctx context.Context, key string) (func(), error) {
	created := &nodeResourceLock{token: make(chan struct{}, 1)}
	created.token <- struct{}{}
	value, _ := service.locks.LoadOrStore(key, created)
	lock := value.(*nodeResourceLock)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock.token:
		return func() { lock.token <- struct{}{} }, nil
	}
}
func clonePhoneCollection(value phoneCollection) phoneCollection {
	value.items = append([]nodePhoneRecord(nil), value.items...)
	return value
}
func derefPhone(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func decodeNodeParams(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("params contains multiple JSON values")
		}
		return err
	}
	return nil
}

func requireEmptyParams(payload json.RawMessage) error {
	var value map[string]json.RawMessage
	if err := decodeNodeParams(payload, &value); err != nil {
		return err
	}
	if len(value) != 0 {
		return errors.New("params 必须是空对象")
	}
	return nil
}
func parseActivationCode(value string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(value), "$")
	if len(parts) != 3 || !strings.EqualFold(parts[0], "LPA:1") || strings.TrimSpace(parts[1]) == "" || strings.TrimSpace(parts[2]) == "" {
		return "", "", errors.New("activation_code 必须使用 LPA:1$地址$匹配码 格式")
	}
	return strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2]), nil
}
func nodeCursorToken() string {
	var value [12]byte
	_, _ = rand.Read(value[:])
	return hex.EncodeToString(value[:])
}
func reject(code, message string) *nodemqtt.ActionError {
	return &nodemqtt.ActionError{State: "rejected", Code: code, Message: message, Certainty: "not_started"}
}
func failed(code, message string) *nodemqtt.ActionError {
	return &nodemqtt.ActionError{State: "failed", Code: code, Message: message, Certainty: "known_failed"}
}
func uncertain(code, message string) *nodemqtt.ActionError {
	return &nodemqtt.ActionError{State: "uncertain", Code: code, Message: message, Certainty: "unknown"}
}
func actionFailure(err error, fallback string) *nodemqtt.ActionError {
	if errors.Is(err, context.DeadlineExceeded) {
		return failed("POOL_OFFLINE", "设备现场读取超时")
	}
	return failed(fallback, sanitizeNodeError(err))
}
func classifySideEffect(err error, uncertainMessage string) *nodemqtt.ActionError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
		return uncertain("RESULT_UNCERTAIN", uncertainMessage)
	}
	return failed("INTERNAL_ERROR", sanitizeNodeError(err))
}
func sanitizeNodeError(err error) string {
	if err == nil {
		return "操作失败"
	}
	value := err.Error()
	if len(value) > 300 {
		value = value[:300]
	}
	return value
}
