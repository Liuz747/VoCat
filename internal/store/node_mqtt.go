package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type NodeMQTTTask struct {
	ID          string
	Node        string
	Action      string
	RequestHash string
	RequestJSON json.RawMessage
	State       string
	LatestSeq   int
	Result      json.RawMessage
	Error       json.RawMessage
	ExpiresAt   time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type NodeMQTTOutboxItem struct {
	Kind             string
	BusinessID       string
	Seq              int
	Node             string
	Payload          json.RawMessage
	Attempts         int
	NextAttemptAt    time.Time
	CreatedAt        time.Time
	AckedAt          time.Time
	ReplayGeneration int
}

// AcceptNodeMQTTTask atomically records a new command and its first task
// response. A duplicate ID returns the original row without changing it.
func (s *Store) AcceptNodeMQTTTask(ctx context.Context, task NodeMQTTTask, payload json.RawMessage) (NodeMQTTTask, bool, error) {
	task.ID = strings.TrimSpace(task.ID)
	task.Node = strings.TrimSpace(task.Node)
	task.Action = strings.TrimSpace(task.Action)
	if task.ID == "" || task.Node == "" || task.Action == "" || task.RequestHash == "" || !json.Valid(task.RequestJSON) || !json.Valid(payload) {
		return NodeMQTTTask{}, false, errors.New("invalid node MQTT task")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("begin node MQTT task: %w", err)
	}
	defer tx.Rollback()
	existing, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, task.ID))
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return NodeMQTTTask{}, false, err
	}
	expiresAt := int64(0)
	if !task.ExpiresAt.IsZero() {
		expiresAt = task.ExpiresAt.Unix()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_mqtt_tasks(
		id,node,action,request_hash,request_json,state,latest_seq,result_json,error_json,
		expires_at,started_at,finished_at,created_at,updated_at
	) VALUES(?,?,?,?,?,'accepted',1,'null','null',?,0,0,?,?)`,
		task.ID, task.Node, task.Action, task.RequestHash, string(task.RequestJSON), expiresAt, now.Unix(), now.Unix())
	if err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("insert node MQTT task: %w", err)
	}
	if err := insertNodeMQTTOutbox(ctx, tx, NodeMQTTOutboxItem{Kind: "task", BusinessID: task.ID, Seq: 1, Node: task.Node, Payload: payload, NextAttemptAt: now, CreatedAt: now}); err != nil {
		return NodeMQTTTask{}, false, err
	}
	created, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, task.ID))
	if err != nil {
		return NodeMQTTTask{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("commit node MQTT task: %w", err)
	}
	return created, true, nil
}

// RejectNodeMQTTTask durably records a malformed but identifiable command.
// It deliberately emits rejected as seq=1 without an intermediate accepted
// state because the command was never eligible for execution.
func (s *Store) RejectNodeMQTTTask(ctx context.Context, task NodeMQTTTask, taskError, payload json.RawMessage) (NodeMQTTTask, bool, error) {
	return s.RecordTerminalNodeMQTTTask(ctx, task, "rejected", taskError, payload)
}

// RecordTerminalNodeMQTTTask records a command that must not enter the
// execution queue, such as an invalid or already-expired command.
func (s *Store) RecordTerminalNodeMQTTTask(ctx context.Context, task NodeMQTTTask, state string, taskError, payload json.RawMessage) (NodeMQTTTask, bool, error) {
	task.ID = strings.TrimSpace(task.ID)
	task.Node = strings.TrimSpace(task.Node)
	task.Action = strings.TrimSpace(task.Action)
	state = strings.TrimSpace(state)
	if task.ID == "" || task.Node == "" || task.Action == "" || task.RequestHash == "" || !nodeMQTTTerminalState(state) || !json.Valid(task.RequestJSON) || !json.Valid(taskError) || !json.Valid(payload) {
		return NodeMQTTTask{}, false, errors.New("invalid terminal node MQTT task")
	}
	now := time.Now().UTC()
	if task.Action == "esim.profile.download" {
		task.RequestJSON = json.RawMessage(`{"redacted":true}`)
	}
	expiresAt := int64(0)
	if !task.ExpiresAt.IsZero() {
		expiresAt = task.ExpiresAt.Unix()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("begin terminal node MQTT task: %w", err)
	}
	defer tx.Rollback()
	existing, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, task.ID))
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return NodeMQTTTask{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_mqtt_tasks(
		id,node,action,request_hash,request_json,state,latest_seq,result_json,error_json,
		expires_at,started_at,finished_at,created_at,updated_at
	) VALUES(?,?,?,?,?,?,1,'null',?,?,0,?,?,?)`,
		task.ID, task.Node, task.Action, task.RequestHash, string(task.RequestJSON), state, string(taskError), expiresAt, now.Unix(), now.Unix(), now.Unix())
	if err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("insert terminal node MQTT task: %w", err)
	}
	if err := insertNodeMQTTOutbox(ctx, tx, NodeMQTTOutboxItem{Kind: "task", BusinessID: task.ID, Seq: 1, Node: task.Node, Payload: payload, NextAttemptAt: now, CreatedAt: now}); err != nil {
		return NodeMQTTTask{}, false, err
	}
	created, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, task.ID))
	if err != nil {
		return NodeMQTTTask{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return NodeMQTTTask{}, false, fmt.Errorf("commit terminal node MQTT task: %w", err)
	}
	return created, true, nil
}

func (s *Store) NodeMQTTTask(ctx context.Context, id string) (NodeMQTTTask, error) {
	return scanNodeMQTTTask(s.db.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, strings.TrimSpace(id)))
}

func (s *Store) ListRecoverableNodeMQTTTasks(ctx context.Context, node string) ([]NodeMQTTTask, error) {
	rows, err := s.db.QueryContext(ctx, nodeMQTTTaskSelect+` WHERE node = ? AND state IN ('accepted','running') ORDER BY created_at,id`, strings.TrimSpace(node))
	if err != nil {
		return nil, fmt.Errorf("list recoverable node MQTT tasks: %w", err)
	}
	defer rows.Close()
	var result []NodeMQTTTask
	for rows.Next() {
		item, scanErr := scanNodeMQTTTask(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// AdvanceNodeMQTTTask updates state and inserts the immutable response in one
// transaction. The caller supplies payload containing the next sequence.
func (s *Store) AdvanceNodeMQTTTask(ctx context.Context, id, state string, result, taskError, payload json.RawMessage) (NodeMQTTTask, error) {
	if !json.Valid(result) || !json.Valid(taskError) || !json.Valid(payload) {
		return NodeMQTTTask{}, errors.New("invalid node MQTT task JSON")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeMQTTTask{}, err
	}
	defer tx.Rollback()
	current, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, strings.TrimSpace(id)))
	if err != nil {
		return NodeMQTTTask{}, err
	}
	nextSeq := current.LatestSeq + 1
	startedAt, finishedAt := current.StartedAt, current.FinishedAt
	requestJSON := current.RequestJSON
	if state == "running" && startedAt.IsZero() {
		startedAt = now
	}
	if nodeMQTTTerminalState(state) {
		finishedAt = now
		if current.Action == "esim.profile.download" {
			requestJSON = json.RawMessage(`{"redacted":true}`)
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE node_mqtt_tasks SET state=?,latest_seq=?,result_json=?,error_json=?,request_json=?,started_at=?,finished_at=?,updated_at=? WHERE id=?`,
		state, nextSeq, string(result), string(taskError), string(requestJSON), nodeMQTTUnixOrZero(startedAt), nodeMQTTUnixOrZero(finishedAt), now.Unix(), current.ID)
	if err != nil {
		return NodeMQTTTask{}, fmt.Errorf("update node MQTT task: %w", err)
	}
	if err := insertNodeMQTTOutbox(ctx, tx, NodeMQTTOutboxItem{Kind: "task", BusinessID: current.ID, Seq: nextSeq, Node: current.Node, Payload: payload, NextAttemptAt: now, CreatedAt: now}); err != nil {
		return NodeMQTTTask{}, err
	}
	updated, err := scanNodeMQTTTask(tx.QueryRowContext(ctx, nodeMQTTTaskSelect+` WHERE id = ?`, current.ID))
	if err != nil {
		return NodeMQTTTask{}, err
	}
	if err := tx.Commit(); err != nil {
		return NodeMQTTTask{}, err
	}
	return updated, nil
}

func (s *Store) RequeueLatestNodeMQTTTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE node_mqtt_outbox SET replay_requested=replay_requested+1,next_attempt_at=?,attempts=0
		WHERE paused_at=0 AND kind='task' AND business_id=? AND seq=(SELECT latest_seq FROM node_mqtt_tasks WHERE id=?)`, time.Now().UTC().Unix(), id, id)
	return err
}

func (s *Store) EnqueueNodeMQTTEvent(ctx context.Context, node, id string, payload json.RawMessage) error {
	now := time.Now().UTC()
	return insertNodeMQTTOutbox(ctx, s.db, NodeMQTTOutboxItem{Kind: "event", BusinessID: id, Node: node, Payload: payload, NextAttemptAt: now, CreatedAt: now})
}

func (s *Store) PendingNodeMQTTOutbox(ctx context.Context, node string, now time.Time, limit int) ([]NodeMQTTOutboxItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT kind,business_id,seq,node,payload_json,attempts,next_attempt_at,created_at,acked_at,replay_requested
		FROM node_mqtt_outbox WHERE node=? AND paused_at=0 AND (acked_at=0 OR replay_requested>0) AND next_attempt_at<=? ORDER BY created_at,kind,business_id,seq LIMIT ?`, strings.TrimSpace(node), now.Unix(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []NodeMQTTOutboxItem
	for rows.Next() {
		item, scanErr := scanNodeMQTTOutbox(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) MarkNodeMQTTOutboxAttempt(ctx context.Context, kind, id string, seq int, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE node_mqtt_outbox SET attempts=attempts+1,next_attempt_at=? WHERE kind=? AND business_id=? AND seq=? AND paused_at=0 AND (acked_at=0 OR replay_requested>0)`, next.Unix(), kind, id, seq)
	return err
}

// WakeNodeMQTTOutbox makes every unacknowledged message immediately eligible
// after a broker reconnect. Business ACKs, rather than MQTT PUBACKs, remain
// the authority for removing an item from the retry set.
func (s *Store) WakeNodeMQTTOutbox(ctx context.Context, node string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE node_mqtt_outbox SET next_attempt_at=? WHERE node=? AND paused_at=0 AND (acked_at=0 OR replay_requested>0)`, time.Now().UTC().Unix(), strings.TrimSpace(node))
	return err
}

func (s *Store) AckNodeMQTTOutbox(ctx context.Context, node, kind, id string, seq int) error {
	result, err := s.db.ExecContext(ctx, `UPDATE node_mqtt_outbox SET acked_at=? WHERE node=? AND kind=? AND business_id=? AND seq=? AND acked_at=0`, time.Now().UTC().Unix(), node, kind, id, seq)
	if err != nil {
		return err
	}
	_, _ = result.RowsAffected()
	return nil
}

func (s *Store) CountPendingNodeMQTTOutbox(ctx context.Context, node string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_mqtt_outbox WHERE node=? AND acked_at=0 AND paused_at=0`, strings.TrimSpace(node)).Scan(&count)
	return count, err
}

// A previously confirmed result may be replayed, but that does not undo the
// receiver's earlier durable ACK. PUBACK completes only this replay request.
func (s *Store) CompleteNodeMQTTReplay(ctx context.Context, node, kind, id string, seq, generation int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE node_mqtt_outbox SET replay_requested=0 WHERE node=? AND kind=? AND business_id=? AND seq=? AND replay_requested=?`, node, kind, id, seq, generation)
	return err
}

// Recheck immediately before each publish: an earlier batch read must not
// override a later administrative pause or an ACK received while publishing.
func (s *Store) NodeMQTTOutboxDeliverable(ctx context.Context, node, kind, id string, seq int) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_mqtt_outbox WHERE node=? AND kind=? AND business_id=? AND seq=? AND paused_at=0 AND (acked_at=0 OR replay_requested>0)`, node, kind, id, seq).Scan(&count)
	return count == 1, err
}

func (s *Store) CountPausedNodeMQTTOutbox(ctx context.Context, node string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_mqtt_outbox WHERE node=? AND acked_at=0 AND paused_at>0`, node).Scan(&count)
	return count, err
}

func (s *Store) CleanupNodeMQTT(ctx context.Context, node string, now time.Time) error {
	node = strings.TrimSpace(node)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM node_mqtt_tasks
		WHERE node=? AND state IN ('succeeded','rejected','failed','expired') AND finished_at>0 AND finished_at<?
		AND NOT EXISTS (SELECT 1 FROM node_mqtt_outbox o WHERE o.kind='task' AND o.business_id=node_mqtt_tasks.id AND o.acked_at=0)`, node, now.Add(-30*24*time.Hour).Unix()); err != nil {
		return err
	}
	// A duplicate command replays its latest outbox payload. Keep that exact
	// reply for as long as the dedupe record exists (including uncertain tasks),
	// even after the usual seven-day retention for acknowledged messages.
	if _, err := tx.ExecContext(ctx, `DELETE FROM node_mqtt_outbox
		WHERE node=? AND acked_at>0 AND acked_at<?
		AND NOT EXISTS (SELECT 1 FROM node_mqtt_tasks t
			WHERE node_mqtt_outbox.kind='task' AND t.node=node_mqtt_outbox.node
			AND t.id=node_mqtt_outbox.business_id AND t.latest_seq=node_mqtt_outbox.seq)`, node, now.Add(-7*24*time.Hour).Unix()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) NodeMQTTMetaInt64(ctx context.Context, key string) (int64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value_json FROM node_mqtt_meta WHERE key=?`, strings.TrimSpace(key)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	var value int64
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return 0, err
	}
	return value, nil
}

func (s *Store) UpsertNodeMQTTMetaInt64(ctx context.Context, key string, value int64) error {
	raw, _ := json.Marshal(value)
	_, err := s.db.ExecContext(ctx, `INSERT INTO node_mqtt_meta(key,value_json,updated_at) VALUES(?,?,?)
		ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`, strings.TrimSpace(key), string(raw), time.Now().UTC().Unix())
	return err
}

type nodeMQTTExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertNodeMQTTOutbox(ctx context.Context, executor nodeMQTTExecer, item NodeMQTTOutboxItem) error {
	_, err := executor.ExecContext(ctx, `INSERT INTO node_mqtt_outbox(kind,business_id,seq,node,payload_json,attempts,next_attempt_at,created_at,acked_at)
		VALUES(?,?,?,?,?,0,?,?,0) ON CONFLICT(kind,business_id,seq) DO NOTHING`, item.Kind, item.BusinessID, item.Seq, item.Node, string(item.Payload), item.NextAttemptAt.Unix(), item.CreatedAt.Unix())
	if err != nil {
		return fmt.Errorf("insert node MQTT outbox: %w", err)
	}
	return nil
}

const nodeMQTTTaskSelect = `SELECT id,node,action,request_hash,request_json,state,latest_seq,result_json,error_json,expires_at,started_at,finished_at,created_at,updated_at FROM node_mqtt_tasks`

func scanNodeMQTTTask(row rowScanner) (NodeMQTTTask, error) {
	var value NodeMQTTTask
	var request, result, taskError string
	var expires, started, finished, created, updated int64
	err := row.Scan(&value.ID, &value.Node, &value.Action, &value.RequestHash, &request, &value.State, &value.LatestSeq, &result, &taskError, &expires, &started, &finished, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeMQTTTask{}, ErrNotFound
	}
	if err != nil {
		return NodeMQTTTask{}, err
	}
	value.RequestJSON, value.Result, value.Error = json.RawMessage(request), json.RawMessage(result), json.RawMessage(taskError)
	value.ExpiresAt, value.StartedAt, value.FinishedAt = timeOrZero(expires), timeOrZero(started), timeOrZero(finished)
	value.CreatedAt, value.UpdatedAt = time.Unix(created, 0).UTC(), time.Unix(updated, 0).UTC()
	return value, nil
}

func scanNodeMQTTOutbox(row rowScanner) (NodeMQTTOutboxItem, error) {
	var value NodeMQTTOutboxItem
	var payload string
	var next, created, acked int64
	if err := row.Scan(&value.Kind, &value.BusinessID, &value.Seq, &value.Node, &payload, &value.Attempts, &next, &created, &acked, &value.ReplayGeneration); err != nil {
		return value, err
	}
	value.Payload = json.RawMessage(payload)
	value.NextAttemptAt, value.CreatedAt, value.AckedAt = timeOrZero(next), timeOrZero(created), timeOrZero(acked)
	return value, nil
}

func nodeMQTTTerminalState(state string) bool {
	switch state {
	case "succeeded", "rejected", "failed", "expired", "uncertain":
		return true
	default:
		return false
	}
}

func nodeMQTTUnixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}

func timeOrZero(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(value, 0).UTC()
}
