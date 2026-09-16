package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"vocat/internal/device"
	"vocat/internal/nodemqtt"
	"vocat/internal/store"
)

type nodeFreshProfileReader interface {
	ESIMListProfilesFresh(context.Context, string) (device.EsimInfo, error)
}

// collectLivePhones preserves the PhoneRecord schema: one row per real profile,
// including disabled/unknown-number profiles. Confirmed blank cards add no rows.
func (service *nodeActionService) collectLivePhones(ctx context.Context) (phoneCollection, *nodemqtt.ActionError) {
	result := phoneCollection{items: []nodePhoneRecord{}}
	reader, ok := service.esim.(nodeFreshProfileReader)
	if !ok || service.readCard == nil {
		return result, failed("POOL_OFFLINE", "现场读卡服务不可用")
	}
	inventory, failure := service.collectInventory(ctx)
	if failure != nil {
		return result, failure
	}
	identities := make(map[string]device.Device)
	for _, entry := range service.mapper.Devices.List() {
		if entry.Discovered && entry.Snapshot != nil && entry.Candidate.HardwareKind != "pcsc" && strings.EqualFold(entry.Candidate.VendorID, "2c7c") {
			identities[strings.TrimSpace(entry.Snapshot.IMEI)] = entry
		}
	}
	for _, raw := range inventory.items {
		row := raw.(map[string]any)
		id := row["device"].(string)
		imei := row["slot"].(string)
		if _, err := service.database.Device(ctx, id); err != nil {
			return result, reject("IDENTITY_PENDING", "设备尚未登记或绑定发生变化："+imei)
		}
		identity, exists := identities[imei]
		if !exists {
			return result, reject("IDENTITY_PENDING", "模块身份尚未核实："+imei)
		}
		profiles, readFailure := service.readPhoneProfiles(ctx, reader, id, imei, identity)
		if readFailure != nil {
			return phoneCollection{}, readFailure
		}
		for _, profile := range profiles {
			iccid := normalizeICCID(profile.ICCID)
			if iccid == "" {
				return phoneCollection{}, failed("INTERNAL_ERROR", "卡内 Profile 缺少 ICCID："+imei)
			}
			phone, err := service.database.PhoneNumberForICCID(ctx, iccid)
			// Unknown phone numbers are valid: the real ICCID still identifies the row.
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return phoneCollection{}, failed("INTERNAL_ERROR", "读取号码关联失败")
			}
			var number *string
			if nodemqtt.ValidE164(phone) {
				number = &phone
			}
			tunnel, available, reason, observed := service.tunnelStatus(id, iccid)
			result.items = append(result.items, nodePhoneRecord{Target: nodeActionTarget{Device: id, Slot: imei, Phone: number, BindingVersion: 1, ICCID: iccid}, Available: available, TunnelState: tunnel, Reason: reason, ObservedAt: nodemqtt.FormatTime(observed), AID: profile.AID, ProfileState: profile.StateText})
		}
	}
	for imei, expected := range identities {
		actual, err := service.mapper.Devices.Get(expected.ID)
		if err != nil || !samePhoneDevice(actual, expected, imei) {
			return phoneCollection{}, reject("IDENTITY_PENDING", "读取期间模块发生重连："+imei)
		}
	}
	// A topology change during a sweep must not yield a successful mixed roster.
	current, failure := service.collectInventory(ctx)
	if failure != nil {
		return phoneCollection{}, failure
	}
	if len(current.items) != len(inventory.items) {
		return phoneCollection{}, reject("IDENTITY_PENDING", "读取期间模块列表发生变化，请重新查询")
	}
	for i, raw := range current.items {
		a, b := raw.(map[string]any), inventory.items[i].(map[string]any)
		if a["slot"] != b["slot"] || a["device"] != b["device"] {
			return phoneCollection{}, reject("IDENTITY_PENDING", "读取期间设备绑定发生变化，请重新查询")
		}
	}
	sort.Slice(result.items, func(i, j int) bool {
		a, b := result.items[i].Target, result.items[j].Target
		if a.Slot != b.Slot {
			return a.Slot < b.Slot
		}
		return a.ICCID < b.ICCID
	})
	result.generatedAt = time.Now().UTC()
	return result, nil
}

func (service *nodeActionService) readPhoneProfiles(ctx context.Context, reader nodeFreshProfileReader, id, imei string, expected device.Device) ([]device.EsimProfile, *nodemqtt.ActionError) {
	release, err := service.lockResource(ctx, "device:"+id)
	if err != nil {
		return nil, reject("SLOT_BUSY", "等待读卡操作锁失败："+imei)
	}
	defer release()
	var info device.EsimInfo
	identityChanged := false
	err = service.readCard(ctx, id, func(ctx context.Context, physicalID string) error {
		matches := func() bool {
			entry, e := service.mapper.Devices.Get(physicalID)
			return e == nil && samePhoneDevice(entry, expected, imei)
		}
		if !matches() {
			identityChanged = true
			return errors.New("module identity changed")
		}
		var readErr error
		info, readErr = reader.ESIMListProfilesFresh(ctx, physicalID)
		if !matches() {
			identityChanged = true
			return errors.New("module identity changed")
		}
		return readErr
	})
	if identityChanged {
		return nil, reject("IDENTITY_PENDING", "读取期间模块身份发生变化："+imei)
	}
	if err != nil {
		return nil, actionFailure(fmt.Errorf("读取模块 %s 的 Profile 失败: %w", imei, err), "INTERNAL_ERROR")
	}
	for i := range info.Profiles {
		info.Profiles[i].AID = info.AID
	}
	return info.Profiles, nil
}

func samePhoneDevice(actual, expected device.Device, imei string) bool {
	return actual.ID == expected.ID && actual.Candidate.USBGeneration == expected.Candidate.USBGeneration && actual.Discovered && actual.Snapshot != nil && strings.TrimSpace(actual.Snapshot.IMEI) == imei
}
