package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type NodeMQTTOutboxKey struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Seq  int    `json:"seq"`
}

type NodeMQTTOutboxSummary struct {
	NodeMQTTOutboxKey
	Attempts      int    `json:"attempts"`
	CreatedAt     int64  `json:"created_at"`
	NextAttemptAt int64  `json:"next_attempt_at"`
	PausedAt      int64  `json:"paused_at"`
	PauseReason   string `json:"pause_reason,omitempty"`
	Event         string `json:"event,omitempty"`
	State         string `json:"state,omitempty"`
}

// ListNodeMQTTOutbox exposes delivery metadata, never message bodies or codes.
func (s *Store) ListNodeMQTTOutbox(ctx context.Context, node string, after int64) ([]NodeMQTTOutboxSummary, int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rowid,kind,business_id,seq,attempts,created_at,next_attempt_at,paused_at,pause_reason,payload_json FROM node_mqtt_outbox WHERE node=? AND acked_at=0 AND rowid>? ORDER BY rowid LIMIT 201`, node, after)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []NodeMQTTOutboxSummary{}
	var lastRow int64
	for rows.Next() {
		var item NodeMQTTOutboxSummary
		var payload string
		var rowID int64
		if err := rows.Scan(&rowID, &item.Kind, &item.ID, &item.Seq, &item.Attempts, &item.CreatedAt, &item.NextAttemptAt, &item.PausedAt, &item.PauseReason, &payload); err != nil {
			return nil, 0, err
		}
		if len(items) == 200 {
			return items, lastRow, nil
		}
		lastRow = rowID
		var metadata struct {
			Event string `json:"event"`
			State string `json:"state"`
		}
		if json.Unmarshal([]byte(payload), &metadata) == nil {
			item.Event = metadata.Event
			item.State = metadata.State
		}
		items = append(items, item)
	}
	return items, 0, rows.Err()
}

// SetNodeMQTTOutboxPaused retains immutable payloads and their real ACK state.
// A batch is atomic and restricted to exact identities in the selected node.
func (s *Store) SetNodeMQTTOutboxPaused(ctx context.Context, node string, items []NodeMQTTOutboxKey, paused bool, reason string) error {
	reason = strings.TrimSpace(reason)
	if strings.TrimSpace(node) == "" || len(items) < 1 || len(items) > 100 || len(reason) > 500 || (paused && reason == "") {
		return errors.New("invalid outbox pause request")
	}
	for _, item := range items {
		if strings.TrimSpace(item.ID) == "" || len(item.ID) > 128 || (item.Kind != "task" && item.Kind != "event") || (item.Kind == "task" && item.Seq < 1) || (item.Kind == "event" && item.Seq != 0) {
			return errors.New("invalid outbox identity")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range items {
		var exists int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_mqtt_outbox WHERE node=? AND kind=? AND business_id=? AND seq=?`, node, item.Kind, item.ID, item.Seq).Scan(&exists); err != nil {
			return err
		}
		if exists != 1 {
			return ErrNotFound
		}
	}
	now := time.Now().UTC().Unix()
	for _, item := range items {
		if paused {
			_, err = tx.ExecContext(ctx, `UPDATE node_mqtt_outbox SET paused_at=?,pause_reason=? WHERE node=? AND kind=? AND business_id=? AND seq=?`, now, reason, node, item.Kind, item.ID, item.Seq)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE node_mqtt_outbox SET paused_at=0,pause_reason='',next_attempt_at=? WHERE node=? AND kind=? AND business_id=? AND seq=?`, now, node, item.Kind, item.ID, item.Seq)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
