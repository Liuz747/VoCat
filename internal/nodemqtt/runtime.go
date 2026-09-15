package nodemqtt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"vocat/internal/buildinfo"
	"vocat/internal/store"
)

type ActionError struct {
	State     string
	Code      string
	Message   string
	Certainty string
	Result    any
}

func (err *ActionError) Error() string { return err.Code + ": " + err.Message }

type Executor interface {
	Execute(context.Context, Command, func(string, *int)) (any, *ActionError)
}

type HealthSnapshot struct {
	Health            string
	DevicesOnline     int
	DevicesOffline    int
	RegisteredTunnels int
}

type HealthProvider interface {
	NodeMQTTHealth(context.Context) HealthSnapshot
}

type Runtime struct {
	settings       Settings
	store          *store.Store
	executor       Executor
	health         HealthProvider
	logger         *slog.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	manager        *autopaho.ConnectionManager
	commands       chan Command
	wakeOutbox     chan struct{}
	commandsAtRest *commandCipher
	executionLocks sync.Map
	connected      atomic.Bool
	subscribed     atomic.Bool
	lastError      atomic.Value
	startedAt      time.Time
}

func Start(parent context.Context, settings Settings, database *store.Store, executor Executor, health HealthProvider, logger *slog.Logger) (*Runtime, error) {
	settings.Normalize()
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	if database == nil || executor == nil {
		return nil, errors.New("node MQTT store and executor are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(parent)
	commandsAtRest, err := newCommandCipher(database.DatabasePath())
	if err != nil {
		cancel()
		return nil, err
	}
	runtime := &Runtime{settings: settings, store: database, executor: executor, health: health, logger: logger, ctx: ctx, cancel: cancel, commands: make(chan Command, 128), wakeOutbox: make(chan struct{}, 1), commandsAtRest: commandsAtRest, startedAt: time.Now().UTC()}
	runtime.lastError.Store("")
	manager, err := runtime.connect()
	if err != nil {
		cancel()
		return nil, err
	}
	runtime.manager = manager
	for range settings.CommandWorkers {
		go runtime.commandWorker()
	}
	go runtime.outboxWorker()
	go runtime.heartbeatWorker()
	go runtime.cleanupWorker()
	go runtime.recoverTasks()
	return runtime, nil
}

func (runtime *Runtime) connect() (*autopaho.ConnectionManager, error) {
	serverURL, err := url.Parse(runtime.settings.BrokerURL())
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if runtime.settings.Scheme == "tls" {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: runtime.settings.Host}
		if runtime.settings.CACertificate != "" {
			roots, _ := x509.SystemCertPool()
			if roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM([]byte(runtime.settings.CACertificate)) {
				return nil, errors.New("CA certificate contains no valid PEM certificate")
			}
			tlsConfig.RootCAs = roots
		}
	}
	presence := runtime.topic("presence")
	will, _ := json.Marshal(map[string]any{"online": false, "time": nil, "reason": "connection_lost"})
	config := autopaho.ClientConfig{
		ServerUrls: []*url.URL{serverURL}, TlsCfg: tlsConfig,
		KeepAlive: uint16(runtime.settings.KeepAliveSeconds), CleanStartOnInitialConnection: false,
		SessionExpiryInterval: uint32(runtime.settings.SessionExpirySeconds), ConnectTimeout: 10 * time.Second,
		ReconnectBackoff: func(attempt int) time.Duration { return reconnectBackoff(attempt) },
		ConnectUsername:  runtime.settings.Username, ConnectPassword: []byte(runtime.settings.Password),
		WillMessage:    &paho.WillMessage{Topic: presence, Payload: will, QoS: 1, Retain: true},
		OnConnectionUp: func(manager *autopaho.ConnectionManager, _ *paho.Connack) { go runtime.connectionUp(manager) },
		OnConnectionDown: func() bool {
			runtime.connected.Store(false)
			runtime.subscribed.Store(false)
			runtime.logger.Warn("node MQTT connection down", "node", runtime.settings.Node, "broker", runtime.settings.Host, "port", runtime.settings.Port)
			return true
		},
		OnConnectError: func(err error) {
			runtime.setError(err)
			runtime.logger.Warn("node MQTT connection failed", "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID:          runtime.settings.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){runtime.onPublish},
			OnClientError:     func(err error) { runtime.setError(err); runtime.logger.Warn("node MQTT client error", "error", err) },
			OnServerDisconnect: func(disconnect *paho.Disconnect) {
				runtime.connected.Store(false)
				runtime.subscribed.Store(false)
				runtime.setError(fmt.Errorf("server disconnect reason %d", disconnect.ReasonCode))
				runtime.logger.Warn("node MQTT server disconnected", "node", runtime.settings.Node, "reason_code", disconnect.ReasonCode)
			},
		},
	}
	runtime.logger.Info("node MQTT connection starting", "node", runtime.settings.Node, "broker", runtime.settings.Host, "port", runtime.settings.Port, "scheme", runtime.settings.Scheme, "client_id", runtime.settings.ClientID, "business_ack", runtime.settings.BusinessACKEnabled)
	return autopaho.NewConnection(runtime.ctx, config)
}

func (runtime *Runtime) connectionUp(manager *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(runtime.ctx, 15*time.Second)
	defer cancel()
	subscriptions := []paho.SubscribeOptions{{Topic: runtime.topic("command"), QoS: 1}}
	if runtime.settings.BusinessACKEnabled {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: runtime.topic("ack"), QoS: 1})
	}
	_, err := manager.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil {
		runtime.setError(err)
		runtime.logger.Error("node MQTT subscribe failed", "topics", subscriptionTopics(subscriptions), "error", err)
		return
	}
	runtime.connected.Store(true)
	runtime.subscribed.Store(true)
	runtime.setError(nil)
	online, _ := json.Marshal(map[string]any{"online": true, "time": FormatTime(time.Now()), "reason": "connected"})
	_, err = manager.Publish(ctx, &paho.Publish{Topic: runtime.topic("presence"), QoS: 1, Retain: true, Payload: online})
	if err != nil {
		runtime.setError(err)
		runtime.logger.Warn("node MQTT TX failed", "topic", runtime.topic("presence"), "qos", 1, "retain", true, "payload", logPayload(online), "error", err)
	} else {
		runtime.logger.Debug("node MQTT TX", "topic", runtime.topic("presence"), "qos", 1, "retain", true, "payload", logPayload(online))
	}
	runtime.emitHello()
	if err := runtime.store.WakeNodeMQTTOutbox(ctx, runtime.settings.Node); err != nil {
		runtime.setError(err)
	}
	runtime.wake()
	runtime.logger.Info("node MQTT connected and subscribed", "node", runtime.settings.Node, "broker", runtime.settings.Host, "port", runtime.settings.Port, "topics", subscriptionTopics(subscriptions), "business_ack", runtime.settings.BusinessACKEnabled)
}

func (runtime *Runtime) onPublish(received paho.PublishReceived) (bool, error) {
	packet := received.Packet
	runtime.logger.Debug("node MQTT RX", "topic", packet.Topic, "qos", packet.QoS, "retain", packet.Retain, "bytes", len(packet.Payload), "payload", logPayload(packet.Payload))
	if len(packet.Payload) > runtime.settings.MaxPayloadBytes {
		runtime.logger.Warn("node MQTT payload rejected", "topic", packet.Topic, "bytes", len(packet.Payload))
		return true, nil
	}
	switch packet.Topic {
	case runtime.topic("command"):
		runtime.receiveCommand(packet.Payload)
	case runtime.topic("ack"):
		runtime.receiveAck(packet.Payload)
	}
	return true, nil
}

func (runtime *Runtime) receiveCommand(payload []byte) {
	command, err := DecodeCommand(payload)
	if err != nil {
		runtime.rejectInvalidCommand(command, payload, err)
		runtime.logger.Warn("invalid node MQTT command", "error", err)
		return
	}
	runtime.logger.Info("node MQTT command received", "task_id", command.ID, "action", command.Action)
	canonical, _ := json.Marshal(command)
	hash := sha256.Sum256(canonical)
	storedCommand := json.RawMessage(canonical)
	if command.Action == "esim.profile.download" {
		storedCommand, err = runtime.commandsAtRest.encrypt(canonical)
		if err != nil {
			runtime.setError(err)
			return
		}
	}
	expires, _ := ParseTime(command.ExpiresAt)
	now := time.Now()
	if !now.Before(expires) {
		taskError := &TaskError{Code: "COMMAND_EXPIRED", Message: "任务尚未执行就已过期", Certainty: "not_started"}
		errorJSON, _ := json.Marshal(taskError)
		payload := marshalTask(command.ID, 1, now, "expired", nil, errorJSON)
		task, created, recordErr := runtime.store.RecordTerminalNodeMQTTTask(runtime.ctx, store.NodeMQTTTask{
			ID: command.ID, Node: runtime.settings.Node, Action: command.Action,
			RequestHash: hex.EncodeToString(hash[:]), RequestJSON: canonical, ExpiresAt: expires,
		}, "expired", errorJSON, payload)
		if recordErr != nil {
			runtime.setError(recordErr)
			return
		}
		if !created && (task.Node != runtime.settings.Node || task.RequestHash != hex.EncodeToString(hash[:])) {
			runtime.emitAlarm("IDEMPOTENCY_CONFLICT", "任务编号被重复用于不同内容", map[string]any{"command": command.ID})
		}
		_ = runtime.store.RequeueLatestNodeMQTTTask(runtime.ctx, command.ID)
		runtime.wake()
		return
	}
	acceptedPayload := marshalTask(command.ID, 1, now, "accepted", nil, nil)
	task, created, err := runtime.store.AcceptNodeMQTTTask(runtime.ctx, store.NodeMQTTTask{ID: command.ID, Node: runtime.settings.Node, Action: command.Action, RequestHash: hex.EncodeToString(hash[:]), RequestJSON: storedCommand, ExpiresAt: expires}, acceptedPayload)
	if err != nil {
		runtime.setError(err)
		return
	}
	if !created {
		if task.Node != runtime.settings.Node || task.RequestHash != hex.EncodeToString(hash[:]) {
			runtime.emitAlarm("IDEMPOTENCY_CONFLICT", "任务编号被重复用于不同内容", map[string]any{"command": command.ID})
			_ = runtime.store.RequeueLatestNodeMQTTTask(runtime.ctx, command.ID)
			runtime.wake()
			return
		}
		_ = runtime.store.RequeueLatestNodeMQTTTask(runtime.ctx, command.ID)
		runtime.wake()
		runtime.logger.Info("node MQTT duplicate command replayed", "task_id", command.ID, "state", task.State, "seq", task.LatestSeq)
		return
	}
	runtime.logger.Info("node MQTT command accepted", "task_id", command.ID, "action", command.Action)
	runtime.wake()
	select {
	case runtime.commands <- command:
	default:
		go func() {
			select {
			case runtime.commands <- command:
			case <-runtime.ctx.Done():
			}
		}()
	}
}

func (runtime *Runtime) rejectInvalidCommand(command Command, payload []byte, validationErr error) {
	command.ID = strings.TrimSpace(command.ID)
	if !idPattern.MatchString(command.ID) || !json.Valid(payload) {
		return
	}
	var generic any
	if err := json.Unmarshal(payload, &generic); err != nil {
		return
	}
	canonical, err := json.Marshal(generic)
	if err != nil {
		return
	}
	hash := sha256.Sum256(canonical)
	action := strings.TrimSpace(command.Action)
	if action == "" {
		action = "_invalid"
	}
	taskError := &TaskError{Code: "INVALID_ARGUMENT", Message: "命令格式无效: " + validationErr.Error(), Certainty: "not_started"}
	errorJSON, _ := json.Marshal(taskError)
	taskPayload := marshalTask(command.ID, 1, time.Now(), "rejected", nil, errorJSON)
	task, created, err := runtime.store.RejectNodeMQTTTask(runtime.ctx, store.NodeMQTTTask{
		ID: command.ID, Node: runtime.settings.Node, Action: action,
		RequestHash: hex.EncodeToString(hash[:]), RequestJSON: json.RawMessage(`{"redacted":true}`),
	}, errorJSON, taskPayload)
	if err != nil {
		runtime.setError(err)
		return
	}
	if !created && (task.Node != runtime.settings.Node || task.RequestHash != hex.EncodeToString(hash[:])) {
		runtime.emitAlarm("IDEMPOTENCY_CONFLICT", "任务编号被重复用于不同内容", map[string]any{"command": command.ID})
	}
	_ = runtime.store.RequeueLatestNodeMQTTTask(runtime.ctx, command.ID)
	runtime.wake()
}

func (runtime *Runtime) receiveAck(payload []byte) {
	if !runtime.settings.BusinessACKEnabled {
		runtime.logger.Debug("node MQTT ACK ignored because business ACK is disabled", "bytes", len(payload))
		return
	}
	ack, err := DecodeAck(payload)
	if err != nil {
		runtime.logger.Warn("invalid node MQTT ACK", "error", err)
		return
	}
	for _, item := range ack.Items {
		seq := 0
		if item.Seq != nil {
			seq = *item.Seq
		}
		if err := runtime.store.AckNodeMQTTOutbox(runtime.ctx, runtime.settings.Node, item.Kind, item.ID, seq); err != nil {
			runtime.setError(err)
		} else {
			runtime.logger.Debug("node MQTT business ACK received", "kind", item.Kind, "id", item.ID, "seq", seq)
		}
	}
}

func (runtime *Runtime) commandWorker() {
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case command := <-runtime.commands:
			runtime.execute(command)
		}
	}
}

func (runtime *Runtime) execute(command Command) {
	task, err := runtime.store.NodeMQTTTask(runtime.ctx, command.ID)
	if err != nil || task.State != "accepted" {
		return
	}
	expires, _ := ParseTime(command.ExpiresAt)
	waitCtx, cancelWait := context.WithDeadline(runtime.ctx, expires)
	release, lockErr := runtime.lockExecution(waitCtx, executionKey(command))
	cancelWait()
	if lockErr != nil {
		if errors.Is(lockErr, context.DeadlineExceeded) {
			runtime.advance(command.ID, "expired", nil, &TaskError{Code: "COMMAND_EXPIRED", Message: "任务在排队期间过期", Certainty: "not_started"})
		}
		return
	}
	defer release()
	if !time.Now().Before(expires) {
		runtime.advance(command.ID, "expired", nil, &TaskError{Code: "COMMAND_EXPIRED", Message: "任务尚未执行就已过期", Certainty: "not_started"})
		return
	}
	if !runtime.advance(command.ID, "running", nil, nil) {
		return
	}
	progress := func(stage string, percent *int) {
		result := map[string]any{"progress": map[string]any{"stage": stage, "percent": percent}}
		runtime.advance(command.ID, "running", result, nil)
	}
	actionCtx, cancel := context.WithTimeout(runtime.ctx, actionTimeout(command.Action))
	defer cancel()
	result, actionErr := runtime.executor.Execute(actionCtx, command, progress)
	if actionErr != nil {
		state := actionErr.State
		if state == "" {
			state = "failed"
		}
		runtime.advance(command.ID, state, actionErr.Result, &TaskError{Code: actionErr.Code, Message: actionErr.Message, Certainty: firstNonEmpty(actionErr.Certainty, certaintyForState(state))})
		runtime.logger.Info("node MQTT command finished", "task_id", command.ID, "action", command.Action, "state", state, "error_code", actionErr.Code)
		return
	}
	runtime.advance(command.ID, "succeeded", result, nil)
	runtime.logger.Info("node MQTT command finished", "task_id", command.ID, "action", command.Action, "state", "succeeded")
}

type executionLock struct{ token chan struct{} }

func (runtime *Runtime) lockExecution(ctx context.Context, key string) (func(), error) {
	created := &executionLock{token: make(chan struct{}, 1)}
	created.token <- struct{}{}
	value, _ := runtime.executionLocks.LoadOrStore(key, created)
	lock := value.(*executionLock)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock.token:
		return func() { lock.token <- struct{}{} }, nil
	}
}

func executionKey(command Command) string {
	if command.Target != nil && strings.TrimSpace(command.Target.Device) != "" {
		return "device:" + strings.TrimSpace(command.Target.Device)
	}
	return "node:" + command.Action
}

func (runtime *Runtime) advance(id, state string, result any, taskError *TaskError) bool {
	current, err := runtime.store.NodeMQTTTask(runtime.ctx, id)
	if err != nil || isTerminal(current.State) {
		return false
	}
	resultJSON := NullJSON()
	if result != nil {
		resultJSON, _ = json.Marshal(result)
	}
	errorJSON := NullJSON()
	if taskError != nil {
		errorJSON, _ = json.Marshal(taskError)
	}
	payload := marshalTask(id, current.LatestSeq+1, time.Now(), state, resultJSON, errorJSON)
	if _, err := runtime.store.AdvanceNodeMQTTTask(runtime.ctx, id, state, resultJSON, errorJSON, payload); err != nil {
		runtime.setError(err)
		runtime.logger.Error("persist node MQTT task state", "task_id", id, "state", state, "error", err)
		return false
	}
	runtime.wake()
	return true
}

func (runtime *Runtime) recoverTasks() {
	tasks, err := runtime.store.ListRecoverableNodeMQTTTasks(runtime.ctx, runtime.settings.Node)
	if err != nil {
		runtime.setError(err)
		return
	}
	for _, task := range tasks {
		if task.State == "running" {
			runtime.advance(task.ID, "uncertain", nil, &TaskError{Code: "RESULT_UNCERTAIN", Message: "节点在执行期间重启，结果无法确认，请勿自动重试", Certainty: "unknown"})
			continue
		}
		requestJSON := task.RequestJSON
		if task.Action == "esim.profile.download" {
			requestJSON, err = runtime.commandsAtRest.decrypt(task.RequestJSON)
			if err != nil {
				runtime.advance(task.ID, "failed", nil, &TaskError{Code: "INTERNAL_ERROR", Message: "加密的持久化任务无法恢复", Certainty: "not_started"})
				continue
			}
		}
		command, decodeErr := DecodeCommand(requestJSON)
		if decodeErr != nil {
			runtime.advance(task.ID, "failed", nil, &TaskError{Code: "INTERNAL_ERROR", Message: "持久化任务无法恢复", Certainty: "not_started"})
			continue
		}
		select {
		case runtime.commands <- command:
		case <-runtime.ctx.Done():
			return
		}
	}
}

func (runtime *Runtime) outboxWorker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case <-ticker.C:
		case <-runtime.wakeOutbox:
		}
		if !runtime.connected.Load() || runtime.manager == nil {
			continue
		}
		items, err := runtime.store.PendingNodeMQTTOutbox(runtime.ctx, runtime.settings.Node, time.Now(), 100)
		if err != nil {
			runtime.setError(err)
			continue
		}
		for _, item := range items {
			topic := runtime.topic(item.Kind)
			ctx, cancel := context.WithTimeout(runtime.ctx, 10*time.Second)
			_, publishErr := runtime.manager.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: item.Payload})
			cancel()
			next := time.Now().Add(outboxBackoff(item.Attempts))
			_ = runtime.store.MarkNodeMQTTOutboxAttempt(runtime.ctx, item.Kind, item.BusinessID, item.Seq, next)
			if publishErr != nil {
				runtime.setError(publishErr)
				runtime.logger.Warn("node MQTT TX failed", "topic", topic, "qos", 1, "retain", false, "kind", item.Kind, "id", item.BusinessID, "seq", item.Seq, "attempt", item.Attempts+1, "payload", logPayload(item.Payload), "error", publishErr)
				break
			}
			if !runtime.settings.BusinessACKEnabled {
				if ackErr := runtime.store.AckNodeMQTTOutbox(runtime.ctx, runtime.settings.Node, item.Kind, item.BusinessID, item.Seq); ackErr != nil {
					runtime.setError(ackErr)
					runtime.logger.Error("node MQTT transport-confirmed message cleanup failed", "kind", item.Kind, "id", item.BusinessID, "seq", item.Seq, "error", ackErr)
					break
				}
			}
			runtime.logger.Debug("node MQTT TX", "topic", topic, "qos", 1, "retain", false, "kind", item.Kind, "id", item.BusinessID, "seq", item.Seq, "attempt", item.Attempts+1, "business_ack_required", runtime.settings.BusinessACKEnabled, "payload", logPayload(item.Payload))
		}
	}
}

func (runtime *Runtime) heartbeatWorker() {
	ticker := time.NewTicker(time.Duration(runtime.settings.HeartbeatSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case <-ticker.C:
			if !runtime.connected.Load() || runtime.manager == nil {
				continue
			}
			runtime.publishHeartbeat()
		}
	}
}

func (runtime *Runtime) cleanupWorker() {
	if err := runtime.store.CleanupNodeMQTT(runtime.ctx, runtime.settings.Node, time.Now()); err != nil {
		runtime.setError(err)
	}
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case now := <-ticker.C:
			if err := runtime.store.CleanupNodeMQTT(runtime.ctx, runtime.settings.Node, now); err != nil {
				runtime.setError(err)
			}
		}
	}
}

func (runtime *Runtime) publishHeartbeat() {
	snapshot := HealthSnapshot{Health: "ok"}
	if runtime.health != nil {
		snapshot = runtime.health.NodeMQTTHealth(runtime.ctx)
	}
	if snapshot.Health == "" {
		snapshot.Health = "ok"
	}
	pending, _ := runtime.store.CountPendingNodeMQTTOutbox(runtime.ctx, runtime.settings.Node)
	payload, _ := json.Marshal(map[string]any{"time": FormatTime(time.Now()), "uptime_seconds": int(time.Since(runtime.startedAt).Seconds()), "health": snapshot.Health, "devices_online": snapshot.DevicesOnline, "devices_offline": snapshot.DevicesOffline, "registered_tunnels": snapshot.RegisteredTunnels, "outbox_pending": pending})
	ctx, cancel := context.WithTimeout(runtime.ctx, 5*time.Second)
	defer cancel()
	if _, err := runtime.manager.Publish(ctx, &paho.Publish{Topic: runtime.topic("heartbeat"), QoS: 0, Payload: payload}); err != nil {
		runtime.setError(err)
		runtime.logger.Warn("node MQTT TX failed", "topic", runtime.topic("heartbeat"), "qos", 0, "retain", false, "payload", logPayload(payload), "error", err)
		return
	}
	runtime.logger.Debug("node MQTT TX", "topic", runtime.topic("heartbeat"), "qos", 0, "retain", false, "payload", logPayload(payload))
}

func (runtime *Runtime) EmitEvent(event string, target *Target, data any) (string, error) {
	id := "evt-" + randomID()
	return runtime.EmitEventWithID(id, event, time.Now(), target, data)
}

func (runtime *Runtime) EmitEventWithID(id, event string, observedAt time.Time, target *Target, data any) (string, error) {
	id, event = strings.TrimSpace(id), strings.TrimSpace(event)
	if !idPattern.MatchString(id) || event == "" {
		return "", errors.New("invalid node MQTT event identity")
	}
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	var eventTarget *EventTarget
	if target != nil {
		eventTarget = &EventTarget{Device: target.Device, Slot: target.Slot, Phone: target.Phone, BindingVersion: target.BindingVersion, ICCID: target.ICCID}
	}
	message := EventMessage{ID: id, Event: event, Time: FormatTime(observedAt), Target: eventTarget, Data: dataJSON}
	payload, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	if err := runtime.store.EnqueueNodeMQTTEvent(runtime.ctx, runtime.settings.Node, id, payload); err != nil {
		return "", err
	}
	runtime.logger.Debug("node MQTT event queued", "event", event, "id", id, "payload", logPayload(payload))
	runtime.wake()
	return id, nil
}

func (runtime *Runtime) emitHello() {
	pending, _ := runtime.store.CountPendingNodeMQTTOutbox(runtime.ctx, runtime.settings.Node)
	_, err := runtime.EmitEvent("node.hello", nil, map[string]any{"node_type": "mdd_cloud", "software_version": buildinfo.Version, "protocol_version": ProtocolVersion, "capabilities": []string{"phones.list", "phones.check", "sms.send", "esim.profile.download", "esim.profile.delete"}, "limits": map[string]any{"payload_bytes": runtime.settings.MaxPayloadBytes, "sms_text_chars": 4096, "inventory_page_size": 100, "card_operations_parallel": 1}, "outbox_pending": pending})
	if err != nil {
		runtime.setError(err)
	}
}

func (runtime *Runtime) emitAlarm(code, message string, details any) {
	_, _ = runtime.EmitEvent("alarm.raised", nil, map[string]any{"code": code, "severity": "error", "message": message, "details": details})
}

func (runtime *Runtime) Status(ctx context.Context) Status {
	pending, _ := runtime.store.CountPendingNodeMQTTOutbox(ctx, runtime.settings.Node)
	last, _ := runtime.lastError.Load().(string)
	return Status{Enabled: true, Connected: runtime.connected.Load(), Subscribed: runtime.subscribed.Load(), LastError: last, OutboxPending: pending}
}

func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if runtime.connected.Load() && runtime.manager != nil {
		payload, _ := json.Marshal(map[string]any{"online": false, "time": FormatTime(time.Now()), "reason": "graceful_shutdown"})
		if _, err := runtime.manager.Publish(ctx, &paho.Publish{Topic: runtime.topic("presence"), QoS: 1, Retain: true, Payload: payload}); err != nil {
			runtime.logger.Warn("node MQTT TX failed", "topic", runtime.topic("presence"), "qos", 1, "retain", true, "payload", logPayload(payload), "error", err)
		} else {
			runtime.logger.Debug("node MQTT TX", "topic", runtime.topic("presence"), "qos", 1, "retain", true, "payload", logPayload(payload))
		}
	}
	runtime.cancel()
	if runtime.manager != nil {
		select {
		case <-runtime.manager.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestConnection(ctx context.Context, settings Settings) error {
	settings.Normalize()
	if settings.Password == SecretMask {
		return errors.New("saved password is required for connection test")
	}
	if err := settings.Validate(); err != nil {
		return err
	}
	testCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	serverURL, err := url.Parse(settings.BrokerURL())
	if err != nil {
		return err
	}
	testClientID := settings.ClientID
	if len(testClientID) > 80 {
		testClientID = testClientID[:80]
	}
	testClientID += "-test-" + randomID()
	config := autopaho.ClientConfig{ServerUrls: []*url.URL{serverURL}, KeepAlive: uint16(settings.KeepAliveSeconds), CleanStartOnInitialConnection: true, ConnectTimeout: 10 * time.Second, ConnectUsername: settings.Username, ConnectPassword: []byte(settings.Password), ClientConfig: paho.ClientConfig{ClientID: testClientID}}
	if settings.Scheme == "tls" {
		config.TlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: settings.Host}
		if settings.CACertificate != "" {
			roots, _ := x509.SystemCertPool()
			if roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM([]byte(settings.CACertificate)) {
				return errors.New("invalid CA certificate")
			}
			config.TlsCfg.RootCAs = roots
		}
	}
	manager, err := autopaho.NewConnection(testCtx, config)
	if err != nil {
		return err
	}
	if err := manager.AwaitConnection(testCtx); err != nil {
		return err
	}
	cancel()
	return nil
}

func (runtime *Runtime) topic(suffix string) string {
	return "cardpool/v1/nodes/" + runtime.settings.Node + "/" + suffix
}

func subscriptionTopics(subscriptions []paho.SubscribeOptions) []string {
	topics := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		topics = append(topics, subscription.Topic)
	}
	return topics
}

// logPayload preserves protocol structure for diagnostics without persisting
// credentials, profile download codes, APDUs, or message bodies.
func logPayload(payload []byte) any {
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return map[string]any{"valid_json": false, "bytes": len(payload)}
	}
	return redactNodeLogValue(value)
}

func redactNodeLogValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
			switch normalized {
			case "password", "cacertificate", "activationcode", "confirmationcode", "apdu", "apdureq", "text", "body", "content":
				result[key] = "[REDACTED]"
			default:
				result[key] = redactNodeLogValue(item)
			}
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = redactNodeLogValue(item)
		}
		return result
	default:
		return value
	}
}

func (runtime *Runtime) wake() {
	select {
	case runtime.wakeOutbox <- struct{}{}:
	default:
	}
}
func (runtime *Runtime) setError(err error) {
	if err == nil {
		runtime.lastError.Store("")
	} else {
		runtime.lastError.Store(err.Error())
	}
}

func marshalTask(id string, seq int, at time.Time, state string, result, taskError json.RawMessage) json.RawMessage {
	if len(result) == 0 {
		result = NullJSON()
	}
	if len(taskError) == 0 {
		taskError = NullJSON()
	}
	payload, _ := json.Marshal(TaskMessage{ID: id, Seq: seq, Time: FormatTime(at), State: state, Result: result, Error: taskError})
	return payload
}

func outboxBackoff(attempt int) time.Duration {
	values := []time.Duration{10, 20, 40, 80, 160, 300}
	if attempt >= len(values) {
		attempt = len(values) - 1
	}
	base := values[attempt] * time.Second
	return base + retryJitter(base/5)
}
func reconnectBackoff(attempt int) time.Duration {
	values := []time.Duration{1, 2, 5, 10, 30}
	if attempt >= len(values) {
		attempt = len(values) - 1
	}
	return values[attempt] * time.Second
}
func certaintyForState(state string) string {
	if state == "uncertain" {
		return "unknown"
	}
	if state == "rejected" || state == "expired" {
		return "not_started"
	}
	return "known_failed"
}
func actionTimeout(action string) time.Duration {
	switch action {
	case "esim.profile.download":
		return 10 * time.Minute
	case "esim.profile.delete":
		return 2 * time.Minute
	case "sms.send":
		return time.Minute
	default:
		return time.Minute
	}
}
func isTerminal(state string) bool {
	return state == "succeeded" || state == "rejected" || state == "failed" || state == "expired" || state == "uncertain"
}
func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}
func retryJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0
	}
	number := uint64(0)
	for _, part := range value {
		number = number<<8 | uint64(part)
	}
	return time.Duration(number % uint64(max+1))
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
