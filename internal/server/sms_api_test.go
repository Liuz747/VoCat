package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/developer"
	"vocat/internal/device"
	"vocat/internal/loghub"
	"vocat/internal/store"
	"vocat/internal/vowifi"
	"vocat/internal/vowifi/multisim"
)

type smsDeletionController struct {
	fakeDeviceController
	mu             sync.Mutex
	storedMessages []device.SMSMessage
	deleted        []string
}

func (controller *smsDeletionController) ListSMS(context.Context, string) ([]device.SMSMessage, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return append([]device.SMSMessage(nil), controller.storedMessages...), nil
}

func (controller *smsDeletionController) DeleteSMSFromStorage(
	_ context.Context,
	_ string,
	storageName string,
	index int,
) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	key := storageName + ":" + strconv.Itoa(index)
	controller.deleted = append(controller.deleted, key)
	remaining := controller.storedMessages[:0]
	for _, message := range controller.storedMessages {
		if message.Storage == storageName && message.Index == index {
			continue
		}
		remaining = append(remaining, message)
	}
	controller.storedMessages = remaining
	return nil
}

func TestSMSThreadAllDevicesUsesIMSIFilter(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	for index, imsi := range []string{"imsi-a", "imsi-b"} {
		if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
			MessageID: "message-" + imsi,
			DeviceID:  "ec20",
			IMSI:      imsi,
			Peer:      "VOXI",
			Direction: "inbound",
			Body:      imsi,
			Timestamp: time.Unix(1_700_000_000+int64(index), 0),
		}); err != nil {
			t.Fatal(err)
		}
	}

	server := &Server{store: database}
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/sms/thread?device_id=all&imsi=imsi-a&peer=VOXI",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleSMSThread(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data) != 1 || envelope.Data[0]["imsi"] != "imsi-a" {
		t.Fatalf("thread data = %#v", envelope.Data)
	}
}

func TestNative410DoesNotUseModemSMSStorage(t *testing.T) {
	if supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypeWiFi410}) {
		t.Fatal("native OpenStick 410 unexpectedly enabled modem SMS storage polling")
	}
	if !supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypePCIeEC20EC25}) {
		t.Fatal("EC20 modem SMS storage polling was disabled")
	}
}

func TestSyncModemSMSDoesNotRelabelStoredMessageAfterProfileSwitch(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	storedMessage := device.SMSMessage{
		Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
		ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
	}
	server := &Server{store: database, logger: regionTestLogger()}
	server.devices = fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: imei, ICCID: "iccid-a", IMSI: "imsi-a",
			Phone: device.PhoneNumber{Number: "+441111"},
		}},
		smsMessages: []device.SMSMessage{storedMessage},
	}
	server.syncModemSMS(ctx, deviceID)

	server.devices = fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: imei, ICCID: "iccid-b", IMSI: "imsi-b",
			Phone: device.PhoneNumber{Number: "+442222"},
		}},
		smsMessages: []device.SMSMessage{storedMessage},
	}
	server.syncModemSMS(ctx, deviceID)

	messages, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ICCID != "iccid-a" ||
		messages[0].IMSI != "imsi-a" || messages[0].LocalPhone != "+441111" {
		t.Fatalf("message identity after profile B rescan = %#v", messages)
	}
}

func TestSMSThreadConfiguredDeviceUsesStableIMEI(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const imei = "867394042309830"
	if err := database.UpsertDevice(ctx, store.Device{
		ID: "ec20_2", Name: "EC20 renamed", ModemIMEI: imei,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
		MessageID: "before-rename", DeviceID: "ec20_1", ModemIMEI: imei,
		IMSI: "imsi-a", Peer: "VOXI", Direction: "inbound", Body: "history",
	}); err != nil {
		t.Fatal(err)
	}

	server := &Server{store: database}
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/sms/thread?device_id=ec20_2&imsi=imsi-a&peer=VOXI",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleSMSThread(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data) != 1 || envelope.Data[0]["modem_imei"] != imei {
		t.Fatalf("thread data = %#v", envelope.Data)
	}
}

func TestNormalizeSMSDeviceFilter(t *testing.T) {
	if got := normalizeSMSDeviceFilter(" ALL "); got != "" {
		t.Fatalf("all filter = %q", got)
	}
	if got := normalizeSMSDeviceFilter("EC20"); got != "EC20" {
		t.Fatalf("device filter = %q", got)
	}
}

func TestSupportsModemSMSStorageRejectsUSBReader(t *testing.T) {
	if supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypeUSBSIMReader}) {
		t.Fatal("USB SIM reader must not be polled with modem SMS AT commands")
	}
	if !supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypePCIeEC20EC25}) {
		t.Fatal("cellular modem should retain modem SMS storage synchronization")
	}
}

func TestSyncModemSMSLogsMultipartMessageOnlyOnceAcrossStoragesAndPolls(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	messages := make([]device.SMSMessage, 0, 6)
	for _, storageName := range []string{"SM", "ME"} {
		for sequence, body := range []string{"part one ", "part two ", "part three"} {
			messages = append(messages, device.SMSMessage{
				Index:                  sequence + 1,
				Storage:                storageName,
				StorageStatus:          device.SMSStatusReceivedUnread,
				Direction:              device.SMSDirectionReceived,
				From:                   "+447700900123",
				Text:                   body,
				Encoding:               device.SMSEncodingGSM7PDU,
				ServiceCenterTimestamp: &receivedAt,
				Concat: &device.SMSConcatInfo{
					Reference: 23,
					Total:     3,
					Sequence:  sequence + 1,
				},
				RawPDU: storageName + body,
			})
		}
	}
	hub := loghub.New(slog.NewTextHandler(io.Discard, nil), 100)
	server := &Server{
		store:  database,
		logger: slog.New(hub),
		devices: fakeDeviceController{
			entry: device.Device{
				ID: deviceID, Discovered: true,
				Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
			},
			smsMessages: messages,
		},
	}

	server.syncModemSMS(ctx, deviceID)
	server.syncModemSMS(ctx, deviceID)

	receivedLogs := hub.History(100, slog.LevelInfo, "cellular SMS received")
	if len(receivedLogs) != 1 {
		t.Fatalf("sms.received logs = %d, want 1: %#v", len(receivedLogs), receivedLogs)
	}
	parts := receivedLogs[0].Fields["parts"]
	if receivedLogs[0].Fields["event"] != "sms.received" || (parts != int64(3) && parts != 3) {
		t.Fatalf("received log fields = %#v", receivedLogs[0].Fields)
	}
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || stored[0].Body != "part one part two part three" {
		t.Fatalf("stored messages = %#v", stored)
	}
}

func TestSyncModemSMSSeparatesReusedConcatReferencesWithoutCursorChurn(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
		peer     = "+447700900123"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	part := func(index, sequence int, body string) device.SMSMessage {
		return device.SMSMessage{
			Index: index, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: peer, Text: body,
			Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &receivedAt,
			Concat: &device.SMSConcatInfo{Reference: 7, Total: 2, Sequence: sequence},
			RawPDU: body,
		}
	}
	messages := []device.SMSMessage{
		// Storage slot 21 and 43 contain unrelated messages that the modem did
		// not return in this filtered view. Multipart segments still belong to
		// the first segment's actual slot, rather than an inferred consecutive
		// slot number.
		part(20, 1, "old-a "), part(25, 2, "old-b"),
		part(42, 1, "new-a "), part(47, 2, "new-b"),
	}
	server := &Server{
		store: database, logger: regionTestLogger(),
		devices: fakeDeviceController{
			entry: device.Device{ID: deviceID, Discovered: true,
				Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"}},
			smsMessages: messages,
		},
	}

	server.syncModemSMS(ctx, deviceID)
	first, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(first) != 2 {
		t.Fatalf("first sync messages = %#v, %v", first, err)
	}
	latest, err := database.LatestSMSMessageID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server.syncModemSMS(ctx, deviceID)
	second, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(second) != 2 {
		t.Fatalf("second sync messages = %#v, %v", second, err)
	}
	fresh, err := database.ListInboundSMSAfterID(ctx, latest, 10)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("notification rows after repeated scan = %#v, %v", fresh, err)
	}
}

func TestDeleteSMSRemovesModemCopyBeforeDatabaseRow(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	modemMessages := make([]device.SMSMessage, 0, 6)
	for _, storageName := range []string{"SM", "ME"} {
		for sequence, body := range []string{"cloud offer ", "claim link ", "reply R"} {
			modemMessages = append(modemMessages, device.SMSMessage{
				Index: sequence + 1, Storage: storageName,
				StorageStatus: device.SMSStatusReceivedUnread,
				Direction:     device.SMSDirectionReceived,
				From:          "+447700900123", Text: body,
				Encoding:               device.SMSEncodingGSM7PDU,
				ServiceCenterTimestamp: &receivedAt,
				Concat: &device.SMSConcatInfo{
					Reference: 23, Total: 3, Sequence: sequence + 1,
				},
				RawPDU: storageName + body,
			})
		}
	}
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
		}},
		storedMessages: modemMessages,
	}
	hub := loghub.New(slog.NewTextHandler(io.Discard, nil), 100)
	server := &Server{store: database, logger: slog.New(hub), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 1 {
		t.Fatalf("initial stored messages = %#v, %v", stored, err)
	}
	deletedID := stored[0].ID

	request := httptest.NewRequest(http.MethodDelete, "/api/sms/messages/"+strconv.FormatInt(deletedID, 10), nil)
	response := httptest.NewRecorder()
	server.handleSMSMessage(response, request, strconv.FormatInt(deletedID, 10))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	controller.mu.Lock()
	deleted := append([]string(nil), controller.deleted...)
	remainingOnModem := len(controller.storedMessages)
	controller.mu.Unlock()
	sort.Strings(deleted)
	wantDeleted := []string{"ME:1", "ME:2", "ME:3", "SM:1", "SM:2", "SM:3"}
	if strings.Join(deleted, ",") != strings.Join(wantDeleted, ",") || remainingOnModem != 0 {
		t.Fatalf("modem deletion = %v, remaining = %d", deleted, remainingOnModem)
	}

	server.syncModemSMS(ctx, deviceID)
	stored, err = database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 0 {
		t.Fatalf("messages after delete and resync = %#v, %v", stored, err)
	}
	fresh, err := database.ListInboundSMSAfterID(ctx, deletedID, 10)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("notification rows after delete and resync = %#v, %v", fresh, err)
	}
	if logs := hub.History(100, slog.LevelInfo, "cellular SMS received"); len(logs) != 1 {
		t.Fatalf("sms.received logs after delete and resync = %d, want 1", len(logs))
	}
}

func TestSMSSendOutcome(t *testing.T) {
	tests := []struct {
		name      string
		all       bool
		accepted  int
		total     int
		delivered bool
		want      string
	}{
		{name: "delivered", all: true, accepted: 1, total: 1, delivered: true, want: "delivered"},
		{name: "accepted but unconfirmed", all: true, accepted: 2, total: 2, want: "accepted_unconfirmed"},
		{name: "partial", accepted: 1, total: 2, want: "partial"},
		{name: "failed", total: 1, want: "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := smsSendOutcome(test.all, test.accepted, test.total, test.delivered); got != test.want {
				t.Fatalf("smsSendOutcome() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestHandleSMSSendAcceptsDestinationsRegardlessOfCountry(t *testing.T) {
	for _, phone := range []string{"+8613800138000", "8613800138000", "008613800138000", "+86 138 0013 8000", "+86-138-0013-8000", "+12025550177", "+447700900123"} {
		t.Run(phone, func(t *testing.T) {
			server := blockedRegionServer(t, "460001234567890")
			controller := server.devices.(fakeDeviceController)
			controller.smsSendResult = device.SMSSendResult{To: phone, SubmittedAt: time.Now().UTC(), PartsTotal: 1, PartsAttempted: 1, PartsAccepted: 1, AllPartsAccepted: true, SubmissionStatus: "accepted"}
			server.devices = controller

			if err := server.store.UpsertDevice(context.Background(), store.Device{ID: "dev1", Name: "EC20"}); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]string{"device_id": "dev1", "phone": phone, "message": "HELLO"})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/sms/send", strings.NewReader(string(payload)))
			response := httptest.NewRecorder()
			server.handleSMSSend(response, request)
			if response.Code != http.StatusAccepted {
				t.Fatalf("SMS request: status %d, body %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHandleSMSSendEnforcesGlobalHourlyLimit(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := developer.SetSMSHourlyLimit(ctx, database, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20_1", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	if reservation, err := database.ReserveSMSSend(ctx, "another-device", 1, time.Now().UTC()); err != nil || !reservation.Allowed {
		t.Fatalf("seed global SMS reservation = %+v, %v", reservation, err)
	}
	server := &Server{
		store:               database,
		logger:              regionTestLogger(),
		maxRequestBodyBytes: 4096,
		devices: fakeDeviceController{entry: device.Device{
			ID:         "ec20_1",
			Discovered: true,
			Snapshot:   &device.Snapshot{DeviceID: "ec20_1"},
		}},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/sms/send",
		strings.NewReader(`{"device_id":"ec20_1","phone":"+447700900123","message":"hello"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleSMSSend(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After header is missing")
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "sms_rate_limited" {
		t.Fatalf("error code = %q, want sms_rate_limited", envelope.Error.Code)
	}
}

func multiSIMSendTestServer(t *testing.T, controller *fakeMultiSIMController) (*Server, *store.Store) {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20", Name: "EC20", VoWiFiEnabled: true, ModemIMEI: "861234567890123"}); err != nil {
		t.Fatal(err)
	}
	server := &Server{
		store:               database,
		logger:              regionTestLogger(),
		maxRequestBodyBytes: 4096,
		devices:             fakeDeviceController{},
		multisim:            controller,
	}
	return server, database
}

func postSMSSend(t *testing.T, server *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/sms/send", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleSMSSend(response, request)
	return response
}

func TestHandleSMSSendRoutesThroughSelectedMultiSIMLine(t *testing.T) {
	controller := &fakeMultiSIMController{
		owned:      true,
		sendResult: vowifi.SMSSubmitResult{Encoding: "gsm7_pdu", PartsTotal: 1, PartsAttempted: 1, PartsAccepted: 1, AllPartsAccepted: true, SubmissionStatus: "accepted_by_ims"},
		sendLine:   multisim.LineIdentity{DeviceID: "ec20", SessionID: "multisim-abc", ICCID: "8944100000000000002", IMSI: "310240000000002", PhoneNumber: "+18605550002"},
	}
	server, database := multiSIMSendTestServer(t, controller)
	response := postSMSSend(t, server, `{"device_id":"ec20","iccid":"8944100000000000002","phone":"+447700900123","message":"hello"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", response.Code, response.Body.String())
	}
	if controller.sentDevice != "ec20" || controller.sentLine != "8944100000000000002" || len(controller.sent) != 1 || controller.sent[0].Text != "hello" {
		t.Fatalf("multisim send = device %q line %q requests %+v", controller.sentDevice, controller.sentLine, controller.sent)
	}
	messages, err := database.ListSMSMessages(context.Background(), store.SMSFilter{DeviceID: "ec20", Peer: "+447700900123", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("stored %d messages, want 1", len(messages))
	}
	saved := messages[0]
	if saved.Direction != "outbound" || saved.Source != "ims" || saved.ICCID != "8944100000000000002" || saved.IMSI != "310240000000002" || saved.LocalPhone != "+18605550002" || saved.ModemIMEI != "861234567890123" || saved.Status != "accepted_by_ims" {
		t.Fatalf("stored message = %+v", saved)
	}
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data["transport"] != "ims" || envelope.Data["submission_accepted"] != true {
		t.Fatalf("response data = %+v", envelope.Data)
	}
}

func TestHandleSMSSendMultiSIMAcceptsSessionSelectorAndReportsLineErrors(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		sendErr  error
		wantCode int
		wantErr  string
		wantLine string
	}{
		{name: "session selector", body: `{"device_id":"ec20","session_id":"multisim-abc","phone":"+447700900123","message":"hello"}`, wantCode: http.StatusAccepted, wantLine: "multisim-abc"},
		{name: "line required", body: `{"device_id":"ec20","phone":"+447700900123","message":"hello"}`, sendErr: multisim.ErrLineRequired, wantCode: http.StatusConflict, wantErr: "multisim_line_required"},
		{name: "unknown line", body: `{"device_id":"ec20","iccid":"8944100000000000009","phone":"+447700900123","message":"hello"}`, sendErr: multisim.ErrNotRegistered, wantCode: http.StatusConflict, wantErr: "multisim_line_unavailable", wantLine: "8944100000000000009"},
		{name: "line not ready", wantLine: "8944100000000000002", body: `{"device_id":"ec20","iccid":"8944100000000000002","phone":"+447700900123","message":"hello"}`, sendErr: vowifi.ErrSMSNotReady, wantCode: http.StatusConflict, wantErr: "multisim_line_not_ready"},
		{name: "group busy", wantLine: "8944100000000000002", body: `{"device_id":"ec20","iccid":"8944100000000000002","phone":"+447700900123","message":"hello"}`, sendErr: multisim.ErrOperationInProgress, wantCode: http.StatusConflict, wantErr: "multisim_active"},
		{name: "ims rejected", wantLine: "8944100000000000002", body: `{"device_id":"ec20","iccid":"8944100000000000002","phone":"+447700900123","message":"hello"}`, sendErr: errors.New("ims: 403"), wantCode: http.StatusBadGateway, wantErr: "ims_sms_submission_failed"},
		{name: "invalid recipient never reaches the line", body: `{"device_id":"ec20","iccid":"8944100000000000002","phone":"abc","message":"hello"}`, wantCode: http.StatusBadGateway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			controller := &fakeMultiSIMController{
				owned:      true,
				sendErr:    tc.sendErr,
				sendResult: vowifi.SMSSubmitResult{PartsTotal: 1, PartsAttempted: 1, PartsAccepted: 1, AllPartsAccepted: true, SubmissionStatus: "accepted_by_ims"},
				sendLine:   multisim.LineIdentity{DeviceID: "ec20", SessionID: "multisim-abc", ICCID: "8944100000000000002"},
			}
			server, _ := multiSIMSendTestServer(t, controller)
			response := postSMSSend(t, server, tc.body)
			if response.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tc.wantCode, response.Body.String())
			}
			if tc.wantErr != "" {
				var envelope errorEnvelope
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope.Error.Code != tc.wantErr {
					t.Fatalf("error code = %q, want %q; body=%s", envelope.Error.Code, tc.wantErr, response.Body.String())
				}
			}
			if controller.sentLine != tc.wantLine {
				t.Fatalf("selector passed to the group = %q, want %q", controller.sentLine, tc.wantLine)
			}
			if tc.name == "invalid recipient never reaches the line" && len(controller.sent) != 0 {
				t.Fatal("an invalid recipient reached the multisim line")
			}
		})
	}
}

func TestHandleSMSSendMultiSIMCountsAgainstGlobalHourlyLimit(t *testing.T) {
	controller := &fakeMultiSIMController{owned: true, sendResult: vowifi.SMSSubmitResult{PartsTotal: 1, PartsAttempted: 1, PartsAccepted: 1, AllPartsAccepted: true, SubmissionStatus: "accepted_by_ims"}}
	server, database := multiSIMSendTestServer(t, controller)
	ctx := context.Background()
	if err := developer.SetSMSHourlyLimit(ctx, database, 1); err != nil {
		t.Fatal(err)
	}
	if reservation, err := database.ReserveSMSSend(ctx, "another-device", 1, time.Now().UTC()); err != nil || !reservation.Allowed {
		t.Fatalf("seed global SMS reservation = %+v, %v", reservation, err)
	}
	response := postSMSSend(t, server, `{"device_id":"ec20","iccid":"8944100000000000002","phone":"+447700900123","message":"hello"}`)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if len(controller.sent) != 0 {
		t.Fatal("a rate-limited request reached the multisim line")
	}
}

func saveTestInboundSMS(t *testing.T, database *store.Store, direction string, body string, extra string) store.SMSMessage {
	t.Helper()
	value := store.SMSMessage{
		MessageID: "test-" + direction + "-" + body + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		DeviceID:  "ec20-1",
		ModemIMEI: "000000000000001",
		ICCID:     "8900000000000000001",
		IMSI:      "001010000000001",
		Peer:      "+15550000001",
		Direction: direction,
		Body:      body,
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
		Source:    "ims",
	}
	if extra != "" {
		value.Extra = json.RawMessage(extra)
	}
	saved, err := database.SaveSMSMessage(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func openSMSTestStore(t *testing.T) *store.Store {
	t.Helper()
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

type smsInboundEnvelope struct {
	Data struct {
		Messages    []map[string]any `json:"messages"`
		NextAfterID int64            `json:"next_after_id"`
		Count       int              `json:"count"`
	} `json:"data"`
}

func getSMSInbound(t *testing.T, server *Server, query string) (int, smsInboundEnvelope, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/api/sms/inbound"+query, nil)
	response := httptest.NewRecorder()
	server.handleSMSInbound(response, request)
	var envelope smsInboundEnvelope
	if response.Code == http.StatusOK {
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode inbound response %s: %v", response.Body.String(), err)
		}
	}
	return response.Code, envelope, response.Body.String()
}

func TestHandleSMSInboundEmptyStoreKeepsCursor(t *testing.T) {
	server := &Server{store: openSMSTestStore(t), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	code, envelope, body := getSMSInbound(t, server, "?after_id=7")
	if code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if envelope.Data.Count != 0 || len(envelope.Data.Messages) != 0 || envelope.Data.NextAfterID != 7 {
		t.Fatalf("empty store response = %s", body)
	}
}

func TestHandleSMSInboundCursorSkipsUnreadyRowsAndOmitsOutbound(t *testing.T) {
	database := openSMSTestStore(t)
	server := &Server{store: database, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	first := saveTestInboundSMS(t, database, "inbound", "first", `{"k":"v"}`)
	empty := saveTestInboundSMS(t, database, "inbound", "", "")
	saveTestInboundSMS(t, database, "outbound", "reply", "")
	third := saveTestInboundSMS(t, database, "received", "third", "")
	if !(first.ID < empty.ID && empty.ID < third.ID) {
		t.Fatalf("unexpected id order: %d %d %d", first.ID, empty.ID, third.ID)
	}

	code, envelope, body := getSMSInbound(t, server, "")
	if code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if envelope.Data.Count != 2 || len(envelope.Data.Messages) != 2 {
		t.Fatalf("expected two ready messages, got %s", body)
	}
	if envelope.Data.NextAfterID != third.ID {
		t.Fatalf("next_after_id = %d, want %d (body %s)", envelope.Data.NextAfterID, third.ID, body)
	}
	got := envelope.Data.Messages[0]
	if int64(got["id"].(float64)) != first.ID || got["body"] != "first" || got["direction"] != "inbound" {
		t.Fatalf("first message = %#v", got)
	}
	if got["read"] != false {
		t.Fatalf("read flag = %#v, want false", got["read"])
	}
	createdAt, ok := got["created_at"].(string)
	if !ok || createdAt == "" {
		t.Fatalf("created_at = %#v", got["created_at"])
	}
	if parsed, err := time.Parse(time.RFC3339, createdAt); err != nil || parsed.Location() != time.UTC {
		t.Fatalf("created_at %q is not RFC3339 UTC: %v", createdAt, err)
	}
	if extra, ok := got["extra"].(map[string]any); !ok || extra["k"] != "v" {
		t.Fatalf("extra = %#v", got["extra"])
	}
	second := envelope.Data.Messages[1]
	if int64(second["id"].(float64)) != third.ID || second["body"] != "third" {
		t.Fatalf("second message = %#v", second)
	}
	// The store normalises an absent Extra to {} before persisting, so a row
	// saved without data surfaces as an empty object rather than null.
	if extra, ok := second["extra"].(map[string]any); !ok || len(extra) != 0 {
		t.Fatalf("extra for row without data = %#v, want {}", second["extra"])
	}
	for _, message := range envelope.Data.Messages {
		if message["direction"] == "outbound" || message["direction"] == "sent" {
			t.Fatalf("outbound row leaked: %#v", message)
		}
	}

	code, envelope, body = getSMSInbound(t, server, "?after_id="+strconv.FormatInt(first.ID, 10))
	if code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if envelope.Data.Count != 1 || int64(envelope.Data.Messages[0]["id"].(float64)) != third.ID || envelope.Data.NextAfterID != third.ID {
		t.Fatalf("after_id=%d response = %s", first.ID, body)
	}

	code, envelope, body = getSMSInbound(t, server, "?after_id="+strconv.FormatInt(third.ID, 10))
	if code != http.StatusOK || envelope.Data.Count != 0 || envelope.Data.NextAfterID != third.ID {
		t.Fatalf("tail response = %d %s", code, body)
	}

	// The cursor endpoint must not mark anything read.
	for _, id := range []int64{first.ID, empty.ID, third.ID} {
		stored, err := database.SMSMessage(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Read {
			t.Fatalf("message %d was marked read by a GET", id)
		}
	}
}

func TestHandleSMSInboundLimitAndValidation(t *testing.T) {
	database := openSMSTestStore(t)
	server := &Server{store: database, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ids := make([]int64, 0, 3)
	for _, body := range []string{"a", "b", "c"} {
		ids = append(ids, saveTestInboundSMS(t, database, "inbound", body, "").ID)
	}

	code, envelope, body := getSMSInbound(t, server, "?limit=2")
	if code != http.StatusOK || envelope.Data.Count != 2 || envelope.Data.NextAfterID != ids[1] {
		t.Fatalf("limit=2 response = %d %s", code, body)
	}
	// Values above 1000 and below 1 clamp instead of failing.
	code, envelope, body = getSMSInbound(t, server, "?limit=5000")
	if code != http.StatusOK || envelope.Data.Count != 3 {
		t.Fatalf("limit=5000 response = %d %s", code, body)
	}
	code, envelope, body = getSMSInbound(t, server, "?limit=0")
	if code != http.StatusOK || envelope.Data.Count != 1 || envelope.Data.NextAfterID != ids[0] {
		t.Fatalf("limit=0 response = %d %s", code, body)
	}
	code, _, body = getSMSInbound(t, server, "?limit=abc")
	if code != http.StatusBadRequest || !strings.Contains(body, "invalid_limit") {
		t.Fatalf("limit=abc response = %d %s", code, body)
	}
	code, _, body = getSMSInbound(t, server, "?after_id=-1")
	if code != http.StatusBadRequest || !strings.Contains(body, "invalid_after_id") {
		t.Fatalf("after_id=-1 response = %d %s", code, body)
	}
	code, _, body = getSMSInbound(t, server, "?after_id=x")
	if code != http.StatusBadRequest || !strings.Contains(body, "invalid_after_id") {
		t.Fatalf("after_id=x response = %d %s", code, body)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/sms/inbound", nil)
	response := httptest.NewRecorder()
	server.handleSMSInbound(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestHandleSMSMessageGetReturnsRowAndDeleteStillWorks(t *testing.T) {
	database := openSMSTestStore(t)
	server := &Server{store: database, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	saved := saveTestInboundSMS(t, database, "outbound", "hello", `{"delivery":"x"}`)
	idText := strconv.FormatInt(saved.ID, 10)

	request := httptest.NewRequest(http.MethodGet, "/api/sms/messages/"+idText, nil)
	response := httptest.NewRecorder()
	server.handleSMSMessage(response, request, idText)
	if response.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body = %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if int64(envelope.Data["id"].(float64)) != saved.ID || envelope.Data["body"] != "hello" || envelope.Data["direction"] != "outbound" {
		t.Fatalf("GET data = %#v", envelope.Data)
	}
	if _, ok := envelope.Data["delivery_state"]; !ok {
		t.Fatalf("delivery_state missing: %#v", envelope.Data)
	}
	if _, ok := envelope.Data["created_at"].(string); !ok {
		t.Fatalf("created_at = %#v", envelope.Data["created_at"])
	}
	if extra, ok := envelope.Data["extra"].(map[string]any); !ok || extra["delivery"] != "x" {
		t.Fatalf("extra = %#v", envelope.Data["extra"])
	}

	request = httptest.NewRequest(http.MethodGet, "/api/sms/messages/999999", nil)
	response = httptest.NewRecorder()
	server.handleSMSMessage(response, request, "999999")
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET missing status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPut, "/api/sms/messages/"+idText, nil)
	response = httptest.NewRecorder()
	server.handleSMSMessage(response, request, idText)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodDelete, "/api/sms/messages/"+idText, nil)
	response = httptest.NewRecorder()
	server.handleSMSMessage(response, request, idText)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"deleted":true`) {
		t.Fatalf("DELETE status = %d, body = %s", response.Code, response.Body.String())
	}
	if _, err := database.SMSMessage(context.Background(), saved.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("message after delete err = %v, want not found", err)
	}
}
