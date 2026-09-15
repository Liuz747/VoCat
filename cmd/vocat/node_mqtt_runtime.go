package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"vocat/internal/nodemqtt"
	"vocat/internal/store"
)

const (
	nodeMQTTSettingsKey = "node_mqtt.settings.v1"
	nodeMQTTPasswordKey = "node_mqtt.password.v1"
)

type nodeMQTTSettingsRuntime struct {
	mu        sync.RWMutex
	baseCtx   context.Context
	database  *store.Store
	executor  *nodeActionService
	logger    *slog.Logger
	settings  nodemqtt.Settings
	runtime   *nodemqtt.Runtime
	cancelSMS context.CancelFunc
}

func newNodeMQTTSettingsRuntime(ctx context.Context, database *store.Store, executor *nodeActionService, logger *slog.Logger) (*nodeMQTTSettingsRuntime, error) {
	manager := &nodeMQTTSettingsRuntime{baseCtx: ctx, database: database, executor: executor, logger: logger}
	settings, err := manager.load(ctx)
	if err != nil {
		return nil, err
	}
	manager.settings = settings
	executor.setIdentity(settings.Node, settings.MaxPayloadBytes)
	if settings.Enabled {
		runtime, startErr := nodemqtt.Start(ctx, settings, database, executor, executor, logger)
		if startErr != nil {
			return nil, startErr
		}
		manager.runtime = runtime
		manager.startSMSBridgeLocked()
	}
	return manager, nil
}

func (manager *nodeMQTTSettingsRuntime) load(ctx context.Context) (nodemqtt.Settings, error) {
	settings := nodemqtt.DefaultSettings()
	value, err := manager.database.AppSetting(ctx, nodeMQTTSettingsKey)
	if errors.Is(err, store.ErrNotFound) {
		return settings, nil
	}
	if err != nil {
		return settings, err
	}
	if err := json.Unmarshal(value.Value, &settings); err != nil {
		return settings, err
	}
	if secret, secretErr := manager.database.AppSetting(ctx, nodeMQTTPasswordKey); secretErr == nil {
		_ = json.Unmarshal(secret.Value, &settings.Password)
	} else if !errors.Is(secretErr, store.ErrNotFound) {
		return settings, secretErr
	}
	settings.Normalize()
	return settings, nil
}

func (manager *nodeMQTTSettingsRuntime) Settings() nodemqtt.Settings {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.settings.Public()
}

func (manager *nodeMQTTSettingsRuntime) Status(ctx context.Context) nodemqtt.Status {
	manager.mu.RLock()
	runtime, enabled := manager.runtime, manager.settings.Enabled
	manager.mu.RUnlock()
	if runtime == nil {
		return nodemqtt.Status{Enabled: enabled}
	}
	return runtime.Status(ctx)
}

func (manager *nodeMQTTSettingsRuntime) Apply(ctx context.Context, settings nodemqtt.Settings) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	settings.Normalize()
	if settings.Password == nodemqtt.SecretMask || (settings.Password == "" && manager.settings.Password != "") {
		settings.Password = manager.settings.Password
	}
	if settings.Enabled {
		if err := settings.Validate(); err != nil {
			return err
		}
	}
	var nextRuntime *nodemqtt.Runtime
	if settings.Enabled {
		runtime, err := nodemqtt.Start(manager.baseCtx, settings, manager.database, manager.executor, manager.executor, manager.logger)
		if err != nil {
			return err
		}
		nextRuntime = runtime
	}
	if err := manager.persist(ctx, settings); err != nil {
		if nextRuntime != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = nextRuntime.Close(closeCtx)
			cancel()
		}
		return err
	}
	oldRuntime := manager.runtime
	if manager.cancelSMS != nil {
		manager.cancelSMS()
		manager.cancelSMS = nil
	}
	manager.runtime = nextRuntime
	manager.settings = settings
	manager.executor.setIdentity(settings.Node, settings.MaxPayloadBytes)
	if oldRuntime != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = oldRuntime.Close(closeCtx)
		cancel()
	}
	if settings.Enabled {
		manager.startSMSBridgeLocked()
	}
	return nil
}

func (manager *nodeMQTTSettingsRuntime) Test(ctx context.Context, settings nodemqtt.Settings) error {
	manager.mu.RLock()
	if settings.Password == nodemqtt.SecretMask || (settings.Password == "" && manager.settings.Password != "") {
		settings.Password = manager.settings.Password
	}
	manager.mu.RUnlock()
	settings.Enabled = true
	settings.Normalize()
	manager.logger.Info("node MQTT connection test starting", "node", settings.Node, "broker", settings.Host, "port", settings.Port, "scheme", settings.Scheme, "client_id", settings.ClientID)
	if err := nodemqtt.TestConnection(ctx, settings); err != nil {
		manager.logger.Warn("node MQTT connection test failed", "node", settings.Node, "broker", settings.Host, "port", settings.Port, "error", err)
		return err
	}
	manager.logger.Info("node MQTT connection test succeeded", "node", settings.Node, "broker", settings.Host, "port", settings.Port)
	return nil
}

func (manager *nodeMQTTSettingsRuntime) persist(ctx context.Context, settings nodemqtt.Settings) error {
	password := settings.Password
	settings.Password = ""
	document, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	if err := manager.database.UpsertAppSetting(ctx, store.AppSetting{Key: nodeMQTTSettingsKey, Value: document}); err != nil {
		return err
	}
	secret, _ := json.Marshal(password)
	return manager.database.UpsertAppSetting(ctx, store.AppSetting{Key: nodeMQTTPasswordKey, Value: secret, Sensitive: true})
}

func (manager *nodeMQTTSettingsRuntime) Close(ctx context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.cancelSMS != nil {
		manager.cancelSMS()
		manager.cancelSMS = nil
	}
	if manager.runtime == nil {
		return nil
	}
	err := manager.runtime.Close(ctx)
	manager.runtime = nil
	return err
}

func (manager *nodeMQTTSettingsRuntime) startSMSBridgeLocked() {
	ctx, cancel := context.WithCancel(manager.baseCtx)
	manager.cancelSMS = cancel
	go manager.smsBridge(ctx)
}

func (manager *nodeMQTTSettingsRuntime) smsBridge(ctx context.Context) {
	manager.mu.RLock()
	node := manager.settings.Node
	manager.mu.RUnlock()
	key := "node_mqtt.sms_cursor." + node
	cursor, err := manager.database.NodeMQTTMetaInt64(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		cursor, _ = manager.database.MaxInboundSMSID(ctx)
		_ = manager.database.UpsertNodeMQTTMetaInt64(ctx, key, cursor)
	} else if err != nil {
		manager.logger.Warn("read node MQTT SMS cursor", "error", err)
		return
	}
	deliveryKey := "node_mqtt.delivery_cursor." + node
	deliveryCursor, deliveryErr := manager.database.NodeMQTTMetaInt64(ctx, deliveryKey)
	if errors.Is(deliveryErr, store.ErrNotFound) {
		deliveryCursor = 0
	} else if deliveryErr != nil {
		manager.logger.Warn("read node MQTT delivery cursor", "error", deliveryErr)
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		messages, listErr := manager.database.ListInboundSMSAfterID(ctx, cursor, 100)
		if listErr == nil {
			for _, message := range messages {
				if manager.emitReceivedSMS(ctx, message) && message.ID > cursor {
					cursor = message.ID
					_ = manager.database.UpsertNodeMQTTMetaInt64(ctx, key, cursor)
				} else if message.ID > cursor {
					break
				}
			}
		}
		deliveryCursor = manager.emitDeliveryEvents(ctx, deliveryKey, deliveryCursor)
	}
}

func (manager *nodeMQTTSettingsRuntime) emitReceivedSMS(ctx context.Context, message store.SMSMessage) bool {
	if strings.TrimSpace(message.ICCID) == "" {
		manager.logger.Warn("skip node MQTT SMS event without ICCID", "sms_id", message.ID)
		return true
	}
	config, err := manager.database.Device(ctx, message.DeviceID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			manager.logger.Warn("skip node MQTT SMS event for removed device", "sms_id", message.ID, "device_id", message.DeviceID)
			return true
		}
		return false
	}
	slot, err := manager.executor.deviceSlot(config)
	if err != nil {
		return false
	}
	phone := strings.TrimSpace(message.LocalPhone)
	if phone == "" {
		phone, _ = manager.database.PhoneNumberForICCID(ctx, message.ICCID)
	}
	var phonePointer *string
	if nodemqtt.ValidE164(phone) {
		phonePointer = &phone
	}
	target := &nodemqtt.Target{Device: config.ID, Slot: slot, Phone: phonePointer, ICCID: strings.TrimSpace(message.ICCID), BindingVersion: 1}
	manager.mu.RLock()
	runtime, node := manager.runtime, manager.settings.Node
	manager.mu.RUnlock()
	if runtime != nil {
		id := stableNodeEventID("sms", node, message.ID)
		if _, err := runtime.EmitEventWithID(id, "sms.received", message.Timestamp, target, map[string]any{"from": nullableString(message.Peer), "to": nullableString(phone), "text": message.Body}); err != nil {
			manager.logger.Warn("persist node MQTT SMS event", "sms_id", message.ID, "error", err)
			return false
		}
		return true
	}
	return false
}

func (manager *nodeMQTTSettingsRuntime) emitDeliveryEvents(ctx context.Context, key string, cursor int64) int64 {
	messages, err := manager.database.ListNodeMQTTDeliveryCandidates(ctx, cursor, 100)
	if err != nil {
		return cursor
	}
	manager.mu.RLock()
	runtime, node := manager.runtime, manager.settings.Node
	manager.mu.RUnlock()
	if runtime == nil {
		return cursor
	}
	for _, message := range messages {
		command := strings.TrimPrefix(message.MessageID, "node-mqtt:")
		if command == "" || message.ICCID == "" {
			cursor = message.ID
			_ = manager.database.UpsertNodeMQTTMetaInt64(ctx, key, cursor)
			continue
		}
		config, configErr := manager.database.Device(ctx, message.DeviceID)
		if configErr != nil {
			if errors.Is(configErr, store.ErrNotFound) {
				cursor = message.ID
				_ = manager.database.UpsertNodeMQTTMetaInt64(ctx, key, cursor)
				continue
			}
			break
		}
		slot, slotErr := manager.executor.deviceSlot(config)
		if slotErr != nil {
			break
		}
		phone := message.LocalPhone
		var phonePointer *string
		if nodemqtt.ValidE164(phone) {
			phonePointer = &phone
		}
		target := &nodemqtt.Target{Device: config.ID, Slot: slot, Phone: phonePointer, ICCID: message.ICCID, BindingVersion: 1}
		state := "undeliverable"
		if message.DeliveryState == "delivered" {
			state = "delivered"
		}
		if _, err := runtime.EmitEventWithID(stableNodeEventID("delivery", node, message.ID), "sms.delivery", message.UpdatedAt, target, map[string]any{"command": command, "delivery_state": state}); err != nil {
			break
		}
		cursor = message.ID
		_ = manager.database.UpsertNodeMQTTMetaInt64(ctx, key, cursor)
	}
	return cursor
}

func stableNodeEventID(kind, node string, id int64) string {
	sum := sha256.Sum256([]byte(kind + ":" + node + ":" + strconv.FormatInt(id, 10)))
	return "evt-" + kind + "-" + hex.EncodeToString(sum[:12])
}
func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}
