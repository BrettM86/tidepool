package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

// HostGoneStreakGap is the longest quiet gap that keeps a host-gone streak
// running. It is twice the backfill retry delay cap (24h), so a failing walk
// retried at that cap records a new failure before the streak lapses, and
// shorter than ingest.DeadHostAfter (72h), so a host must fail on several
// retries spread over the threshold before backfill skips it.
const HostGoneStreakGap = 48 * time.Hour

// Instance is one remote instance's nodeinfo discovery state (migration
// 042). SoftwareName is "" when no discovery has succeeded yet. FetchedAt is
// zero when no discovery has been attempted: a row created by RecordHostGone
// alone (migration 051) has none.
type Instance struct {
	Host         string
	SoftwareName string
	FetchedAt    time.Time
}

// InstanceDiscovery stores per-host nodeinfo discovery results.
type InstanceDiscovery interface {
	// Get returns host's row, or a NotFound error when there is none.
	Get(ctx context.Context, host string) (*Instance, error)
	// RecordSoftware stores a successful discovery: name and fetchedAt.
	RecordSoftware(ctx context.Context, host, name string, fetchedAt time.Time) error
	// RecordFetchFailure stores a failed discovery: fetchedAt advances and
	// any stored name is kept (a new row stores no name).
	RecordFetchFailure(ctx context.Context, host string, fetchedAt time.Time) error
}

// Instances stores per-host nodeinfo discovery results and host-gone streaks.
type Instances interface {
	InstanceDiscovery
	HostGoneStreaks
}

// HostGoneStreaks tracks per-host host-gone streaks.
type HostGoneStreaks interface {
	// RecordHostGone records a host-gone failure at at and returns when the
	// host's current streak began. A host with no streak, or whose last
	// host-gone failure is more than HostGoneStreakGap before at, starts a new
	// streak at at. A host with no row gets one, with no discovery state.
	RecordHostGone(ctx context.Context, host string, at time.Time) (time.Time, error)
	// ClearHostGone ends any host-gone streak for host.
	ClearHostGone(ctx context.Context, host string) error
}

type postgresInstances struct {
	db *sql.DB
}

// NewInstances creates the postgres-backed instances repository.
func NewInstances(db *sql.DB) Instances {
	return &postgresInstances{db: db}
}

func (r *postgresInstances) Get(ctx context.Context, host string) (*Instance, error) {
	if host == "" {
		return nil, errors.NewValidationError("host", "must not be empty")
	}
	var (
		instance  Instance
		name      sql.NullString
		fetchedAt sql.NullTime
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT host, software_name, fetched_at FROM instances WHERE host = $1`, host).
		Scan(&instance.Host, &name, &fetchedAt)
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("instance", host)
		}
		return nil, fmt.Errorf("get instance %q: %w", host, err)
	}
	instance.SoftwareName = name.String
	instance.FetchedAt = fetchedAt.Time
	return &instance, nil
}

func (r *postgresInstances) RecordSoftware(ctx context.Context, host, name string, fetchedAt time.Time) error {
	switch {
	case host == "":
		return errors.NewValidationError("host", "must not be empty")
	case name == "":
		return errors.NewValidationError("software_name", "must not be empty")
	case fetchedAt.IsZero():
		return errors.NewValidationError("fetched_at", "must be set")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO instances (host, software_name, fetched_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (host) DO UPDATE
		SET software_name = EXCLUDED.software_name, fetched_at = EXCLUDED.fetched_at`,
		host, name, fetchedAt)
	if err != nil {
		return fmt.Errorf("record instance software for %q: %w", host, err)
	}
	return nil
}

func (r *postgresInstances) RecordFetchFailure(ctx context.Context, host string, fetchedAt time.Time) error {
	switch {
	case host == "":
		return errors.NewValidationError("host", "must not be empty")
	case fetchedAt.IsZero():
		return errors.NewValidationError("fetched_at", "must be set")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO instances (host, software_name, fetched_at)
		VALUES ($1, NULL, $2)
		ON CONFLICT (host) DO UPDATE
		SET fetched_at = EXCLUDED.fetched_at`,
		host, fetchedAt)
	if err != nil {
		return fmt.Errorf("record instance fetch failure for %q: %w", host, err)
	}
	return nil
}

func (r *postgresInstances) RecordHostGone(ctx context.Context, host string, at time.Time) (time.Time, error) {
	switch {
	case host == "":
		return time.Time{}, errors.NewValidationError("host", "must not be empty")
	case at.IsZero():
		return time.Time{}, errors.NewValidationError("host_gone_last_at", "must be set")
	}
	// One statement, so concurrent failures for the same host cannot both
	// read "no streak" and race to start one.
	var goneSince time.Time
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO instances (host, host_gone_since, host_gone_last_at)
		VALUES ($1, $2, $2)
		ON CONFLICT (host) DO UPDATE
		SET host_gone_since = CASE
				WHEN instances.host_gone_since IS NULL
					OR instances.host_gone_last_at IS NULL
					OR instances.host_gone_last_at < EXCLUDED.host_gone_last_at - make_interval(secs => $3)
				THEN EXCLUDED.host_gone_since
				ELSE instances.host_gone_since
			END,
			-- GREATEST ignores NULL and never moves the latest failure
			-- backwards when an older failure is recorded late.
			host_gone_last_at = GREATEST(instances.host_gone_last_at, EXCLUDED.host_gone_last_at)
		RETURNING host_gone_since`,
		host, at, HostGoneStreakGap.Seconds()).Scan(&goneSince)
	if err != nil {
		return time.Time{}, fmt.Errorf("record host gone for %q: %w", host, err)
	}
	return goneSince, nil
}

func (r *postgresInstances) ClearHostGone(ctx context.Context, host string) error {
	if host == "" {
		return errors.NewValidationError("host", "must not be empty")
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE instances SET host_gone_since = NULL, host_gone_last_at = NULL
		WHERE host = $1 AND host_gone_since IS NOT NULL`, host)
	if err != nil {
		return fmt.Errorf("clear host gone for %q: %w", host, err)
	}
	return nil
}
