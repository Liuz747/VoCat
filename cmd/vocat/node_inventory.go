package main

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"vocat/internal/nodemqtt"
	"vocat/internal/pcsc"
	"vocat/internal/store"
)

type nodeInventorySnapshot struct {
	items       []any
	generatedAt time.Time
	expiresAt   time.Time
	pageSize    int
}

// Inventory enumerates physical modules independently of their SIM/profile and
// network settings. Profile metadata remains registry sourced; it must not be
// mistaken for a fresh, complete card read. Use esim.profiles.list for that.
func (service *nodeActionService) getInventory(ctx context.Context, command nodemqtt.Command) (any, *nodemqtt.ActionError) {
	if command.Target != nil {
		return nil, reject("INVALID_ARGUMENT", "inventory.get 不允许 target")
	}
	var params struct {
		PageSize int    `json:"page_size"`
		Cursor   string `json:"cursor"`
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
	var snapshot nodeInventorySnapshot
	var token string
	offset := 0
	if params.Cursor == "" {
		var failure *nodemqtt.ActionError
		snapshot, failure = service.collectInventory(ctx)
		if failure != nil {
			return nil, failure
		}
		snapshot.pageSize = params.PageSize
		snapshot.expiresAt = time.Now().Add(5 * time.Minute)
	} else {
		parts := strings.SplitN(params.Cursor, ".", 2)
		if len(parts) != 2 {
			return nil, reject("CURSOR_EXPIRED", "分页游标无效")
		}
		parsed, err := strconv.Atoi(parts[1])
		if err != nil || parsed < 0 {
			return nil, reject("CURSOR_EXPIRED", "分页游标无效")
		}
		offset = parsed
		token = parts[0]
		service.cacheMu.Lock()
		cached, ok := service.inventorySnapshots[token]
		service.cacheMu.Unlock()
		if !ok || time.Now().After(cached.expiresAt) || cached.pageSize != params.PageSize || offset >= len(cached.items) {
			return nil, reject("CURSOR_EXPIRED", "库存分页已失效，请从第一页重新查询")
		}
		snapshot = cached
	}
	end := offset + params.PageSize
	if end > len(snapshot.items) {
		end = len(snapshot.items)
	}
	var next any
	if end < len(snapshot.items) {
		if token == "" {
			token = nodeCursorToken()
			service.cacheMu.Lock()
			if service.inventorySnapshots == nil {
				service.inventorySnapshots = make(map[string]nodeInventorySnapshot)
			}
			for key, v := range service.inventorySnapshots {
				if time.Now().After(v.expiresAt) {
					delete(service.inventorySnapshots, key)
				}
			}
			// Bound abandoned first-page snapshots as well as expiring them.
			if len(service.inventorySnapshots) >= 32 {
				oldest := ""
				var deadline time.Time
				for key, v := range service.inventorySnapshots {
					if oldest == "" || v.expiresAt.Before(deadline) {
						oldest = key
						deadline = v.expiresAt
					}
				}
				delete(service.inventorySnapshots, oldest)
			}
			service.inventorySnapshots[token] = snapshot
			service.cacheMu.Unlock()
		}
		next = token + "." + strconv.Itoa(end)
	}
	return map[string]any{"source": "registry", "generated_at": nodemqtt.FormatTime(snapshot.generatedAt), "items": snapshot.items[offset:end], "cursor": next, "complete": next == nil}, nil
}

func (service *nodeActionService) collectInventory(ctx context.Context) (nodeInventorySnapshot, *nodemqtt.ActionError) {
	result := nodeInventorySnapshot{items: []any{}}
	if service.mapper.Devices == nil {
		return result, failed("POOL_OFFLINE", "设备发现服务不可用")
	}
	configs, err := service.database.ListDevices(ctx)
	if err != nil {
		return result, failed("INTERNAL_ERROR", "读取设备列表失败")
	}
	byIMEI := make(map[string]store.Device)
	for _, cfg := range configs {
		imei := strings.TrimSpace(cfg.ModemIMEI)
		if imei == "" {
			continue
		}
		previous, exists := byIMEI[imei]
		// Preserve the active group's target when legacy records alias one modem.
		active := service.multisim != nil && service.multisim.Owns(cfg.ID)
		previousActive := exists && service.multisim != nil && service.multisim.Owns(previous.ID)
		if !exists || (active && !previousActive) || (active == previousActive && ((cfg.ID == imei && previous.ID != imei) || (cfg.ID != imei && previous.ID != imei && cfg.ID < previous.ID))) {
			byIMEI[imei] = cfg
		}
	}
	seen := make(map[string]string)
	entries := service.mapper.Devices.List()
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return result, failed("POOL_OFFLINE", "设备枚举超时")
		}
		// Virtual PC/SC readers belonging to EC20s are not additional modems.
		if !entry.Discovered || entry.Candidate.HardwareKind == pcsc.HardwareKind || !strings.EqualFold(entry.Candidate.VendorID, "2c7c") {
			continue
		}
		if entry.Snapshot == nil || !validNodeIMEI(entry.Snapshot.IMEI) {
			return result, reject("IDENTITY_PENDING", "发现 EC20，但尚未核实完整 IMEI；请稍后重新查询")
		}
		imei := strings.TrimSpace(entry.Snapshot.IMEI)
		if physicalID, exists := seen[imei]; exists {
			if physicalID != entry.ID {
				return result, reject("IDENTITY_PENDING", "多个物理模块返回相同 IMEI，无法唯一定位")
			}
			continue
		}
		seen[imei] = entry.ID
		cfg, configured := byIMEI[imei]
		if !configured {
			// A record without a persisted IMEI can still have an exact discovery ID.
			for _, candidate := range configs {
				if candidate.ID == entry.ID && strings.TrimSpace(candidate.ModemIMEI) == "" {
					cfg = candidate
					configured = true
					break
				}
			}
		}
		if !configured {
			cfg = store.Device{ID: entry.ID}
		}
		cfg.ModemIMEI = imei
		record := service.slotRecord(ctx, cfg, false)
		// Desired group membership is neither a card inventory nor evidence that
		// an empty group means no card. Keep every IMEI even without profile data.
		record["profiles_complete"] = false
		record["profiles_observed_at"] = nil
		record["current_iccid"] = nil
		for _, raw := range record["profiles"].([]any) {
			raw.(map[string]any)["state"] = "unknown"
		}
		if !configured || record["state"] == "error" || record["state"] == "empty" {
			record["state"] = "pending"
		}
		result.items = append(result.items, record)
	}
	sort.Slice(result.items, func(i, j int) bool {
		return result.items[i].(map[string]any)["slot"].(string) < result.items[j].(map[string]any)["slot"].(string)
	})
	result.generatedAt = time.Now().UTC()
	return result, nil
}

func validNodeIMEI(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 15 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
