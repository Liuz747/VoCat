package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *Store) UpsertPhoneAssociation(ctx context.Context, value PhoneAssociation) error {
	value.ICCID = strings.TrimSpace(value.ICCID)
	value.DeviceID = strings.TrimSpace(value.DeviceID)
	value.Number = strings.TrimSpace(value.Number)
	value.Source = strings.TrimSpace(value.Source)
	if value.ICCID == "" || value.Number == "" || value.Source == "" {
		return errors.New("phone association ICCID, number, and source are required")
	}
	now := time.Now().UTC()
	createdAt := value.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := value.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO phone_associations (
			iccid, device_id, number, source, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(iccid) DO UPDATE SET
			device_id = excluded.device_id,
			number = excluded.number,
			source = excluded.source,
			updated_at = excluded.updated_at
	`,
		value.ICCID,
		value.DeviceID,
		value.Number,
		value.Source,
		createdAt.Unix(),
		updatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert phone association for ICCID %q: %w", value.ICCID, err)
	}
	return nil
}

func (s *Store) PhoneAssociation(
	ctx context.Context,
	iccid string,
) (PhoneAssociation, error) {
	var value PhoneAssociation
	var createdAt, updatedAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT iccid, device_id, number, source, created_at, updated_at
		FROM phone_associations
		WHERE iccid = ?
	`, strings.TrimSpace(iccid)).Scan(
		&value.ICCID,
		&value.DeviceID,
		&value.Number,
		&value.Source,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return PhoneAssociation{}, ErrNotFound
	}
	if err != nil {
		return PhoneAssociation{}, err
	}
	value.CreatedAt = time.Unix(createdAt, 0).UTC()
	value.UpdatedAt = time.Unix(updatedAt, 0).UTC()
	return value, nil
}

// PhoneNumberForICCID returns the best durable number associated with one SIM.
// A user override wins over an IMS-published association, matching the device
// overview while keeping the lookup scoped to the historical ICCID.
func (s *Store) PhoneNumberForICCID(ctx context.Context, iccid string) (string, error) {
	iccid = strings.TrimSpace(iccid)
	if iccid == "" {
		return "", ErrNotFound
	}
	var number string
	err := s.db.QueryRowContext(ctx, `
		SELECT number
		FROM (
			SELECT custom_phone_number AS number, 0 AS priority
			FROM card_policies
			WHERE iccid = ? AND TRIM(custom_phone_number) <> ''
			UNION ALL
			SELECT number, 1 AS priority
			FROM phone_associations
			WHERE iccid = ? AND TRIM(number) <> ''
		)
		ORDER BY priority
		LIMIT 1
	`, iccid, iccid).Scan(&number)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read phone number for ICCID %q: %w", iccid, err)
	}
	return strings.TrimSpace(number), nil
}
