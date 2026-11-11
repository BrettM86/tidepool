package store

import (
	"context"
	"database/sql"
	"fmt"

	"tidepool/internal/errors"
)

func (r *postgresOutboundDeliveries) HasConsentHoldTx(ctx context.Context, tx *sql.Tx, atURI string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	if atURI == "" {
		return false, errors.NewValidationError("at_uri", "must not be empty")
	}
	var held bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM outbound_deliveries
			WHERE state = 'held' AND held_reason = $2 AND held_subject = $1)`,
		atURI, HeldReasonConsent).Scan(&held); err != nil {
		return false, fmt.Errorf("read consent holds on %q: %w", atURI, err)
	}
	return held, nil
}
