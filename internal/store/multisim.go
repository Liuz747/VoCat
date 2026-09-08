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

var ErrMultiSIMConflict = errors.New("stop this device's enabled or pending automatic tasks before enabling multi-SIM")

type MultiSIMProfile struct {
	ICCID string `json:"iccid"`
	AID   string `json:"aid"`
	Name  string `json:"name,omitempty"`
}
type MultiSIMConfig struct {
	DeviceID  string            `json:"device_id"`
	Enabled   bool              `json:"enabled"`
	Profiles  []MultiSIMProfile `json:"profiles"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

func (s *Store) MultiSIMConfig(ctx context.Context, id string) (MultiSIMConfig, error) {
	return scanMultiSIMConfig(s.db.QueryRowContext(ctx, `SELECT device_id,enabled,profiles_json,created_at,updated_at FROM device_multisim WHERE device_id=?`, id))
}
func (s *Store) ListMultiSIMConfigs(ctx context.Context) ([]MultiSIMConfig, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT device_id,enabled,profiles_json,created_at,updated_at FROM device_multisim ORDER BY device_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []MultiSIMConfig{}
	for rows.Next() {
		v, err := scanMultiSIMConfig(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}
func scanMultiSIMConfig(row rowScanner) (MultiSIMConfig, error) {
	var v MultiSIMConfig
	var encoded string
	var created, updated int64
	if err := row.Scan(&v.DeviceID, &v.Enabled, &encoded, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return v, ErrNotFound
		}
		return v, err
	}
	if err := json.Unmarshal([]byte(encoded), &v.Profiles); err != nil {
		return v, errors.New("invalid saved multi-SIM profiles")
	}
	v.CreatedAt = time.Unix(created, 0).UTC()
	v.UpdatedAt = time.Unix(updated, 0).UTC()
	if v.Profiles == nil {
		v.Profiles = []MultiSIMProfile{}
	}
	return v, nil
}
func (s *Store) SaveMultiSIMConfig(ctx context.Context, v MultiSIMConfig) (MultiSIMConfig, error) {
	v.DeviceID = strings.TrimSpace(v.DeviceID)
	if v.DeviceID == "" || (v.Enabled && len(v.Profiles) < 1) {
		return v, errors.New("multi-SIM requires a device and at least one profile when enabled")
	}
	seen := map[string]bool{}
	for i := range v.Profiles {
		p := &v.Profiles[i]
		p.ICCID = strings.TrimSpace(p.ICCID)
		p.AID = strings.ToUpper(strings.TrimSpace(p.AID))
		p.Name = strings.TrimSpace(p.Name)
		if p.ICCID == "" || seen[p.ICCID] {
			return v, errors.New("multi-SIM profiles require unique ICCIDs")
		}
		seen[p.ICCID] = true
	}
	encoded, err := json.Marshal(v.Profiles)
	if err != nil {
		return v, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	if v.Enabled {
		var conflicts int
		err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM automatic_tasks WHERE device_id=? AND enabled=1)+(SELECT COUNT(*) FROM automatic_task_runs WHERE device_id=? AND status IN ('queued','running'))`, v.DeviceID, v.DeviceID).Scan(&conflicts)
		if err != nil {
			return v, err
		}
		if conflicts > 0 {
			return v, ErrMultiSIMConflict
		}
	}
	now := time.Now().UTC().Unix()
	_, err = tx.ExecContext(ctx, `INSERT INTO device_multisim(device_id,enabled,profiles_json,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(device_id) DO UPDATE SET enabled=excluded.enabled,profiles_json=excluded.profiles_json,updated_at=excluded.updated_at`, v.DeviceID, v.Enabled, string(encoded), now, now)
	if err != nil {
		return v, fmt.Errorf("save multi-SIM config: %w", err)
	}
	saved, err := scanMultiSIMConfig(tx.QueryRowContext(ctx, `SELECT device_id,enabled,profiles_json,created_at,updated_at FROM device_multisim WHERE device_id=?`, v.DeviceID))
	if err != nil {
		return v, err
	}
	if err = tx.Commit(); err != nil {
		return v, err
	}
	return saved, nil
}

func (s *Store) DeleteMultiSIMConfig(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM device_multisim WHERE device_id=?`, id)
	return err
}
