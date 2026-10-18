package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// HandleMigrationMode identifies the direction of a handle migration run.
type HandleMigrationMode string

const (
	HandleMigrationModeForward  HandleMigrationMode = "forward"
	HandleMigrationModeRollback HandleMigrationMode = "rollback"
)

type HandleMigrationRun struct {
	Mode      HandleMigrationMode
	ID        int64
	DIDs      []string
	StartedAt time.Time
}

// HandleMigrationProgress counts persisted per-actor migration state.
type HandleMigrationProgress struct {
	Remaining         int
	Pending           int
	Migrated          int
	Errors            map[string]int
	RollbackRemaining int
}

type HandleMigrationRuns interface {
	Start(ctx context.Context, mode HandleMigrationMode, dids []string) (*HandleMigrationRun, error)
	Active(ctx context.Context) (*HandleMigrationRun, error)
	Finish(ctx context.Context, id int64) error
	Progress(ctx context.Context, bridgeHostname, handleZoneRoot string) (HandleMigrationProgress, error)
}

type postgresHandleMigrationRuns struct{ db *sql.DB }

func NewHandleMigrationRuns(db *sql.DB) HandleMigrationRuns {
	return &postgresHandleMigrationRuns{db: db}
}

func (r *postgresHandleMigrationRuns) Start(ctx context.Context, mode HandleMigrationMode, dids []string) (*HandleMigrationRun, error) {
	if mode != HandleMigrationModeForward && mode != HandleMigrationModeRollback {
		return nil, errors.NewValidationError("mode", fmt.Sprintf("unknown handle migration mode %q", mode))
	}
	run := &HandleMigrationRun{Mode: mode, DIDs: dids}
	err := r.db.QueryRowContext(ctx, `INSERT INTO handle_migration_runs (mode, dids)
		VALUES ($1, $2) RETURNING id, started_at`, mode, pq.Array(dids)).Scan(&run.ID, &run.StartedAt)
	if constraint, ok := uniqueViolation(err); ok && constraint == "handle_migration_runs_one_active_idx" {
		return nil, errors.NewConflictError("handle_migration_run", "active", string(mode))
	}
	if err != nil {
		return nil, fmt.Errorf("start handle migration run: %w", err)
	}
	return run, nil
}

func (r *postgresHandleMigrationRuns) Active(ctx context.Context) (*HandleMigrationRun, error) {
	var run HandleMigrationRun
	var mode string
	err := r.db.QueryRowContext(ctx, `SELECT id, mode, dids, started_at FROM handle_migration_runs
		WHERE finished_at IS NULL`).Scan(&run.ID, &mode, pq.Array(&run.DIDs), &run.StartedAt)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read active handle migration run: %w", err)
	}
	if HandleMigrationMode(mode) != HandleMigrationModeForward && HandleMigrationMode(mode) != HandleMigrationModeRollback {
		return nil, fmt.Errorf("read active handle migration run: unsupported mode %q", mode)
	}
	run.Mode = HandleMigrationMode(mode)
	return &run, nil
}

func (r *postgresHandleMigrationRuns) Finish(ctx context.Context, id int64) error {
	result, err := r.db.ExecContext(ctx, `UPDATE handle_migration_runs SET finished_at = now()
		WHERE id = $1 AND finished_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("finish handle migration run: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("finish handle migration run: rows affected: %w", err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("active_handle_migration_run", fmt.Sprint(id))
	}
	return nil
}

func (r *postgresHandleMigrationRuns) Progress(ctx context.Context, bridgeHostname, handleZoneRoot string) (HandleMigrationProgress, error) {
	bridge, zone := handleFormatSuffixes(bridgeHostname, handleZoneRoot)
	var progress HandleMigrationProgress
	var rejected, notControlled, invalid, unavailable int
	err := r.db.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE consent_state <> 'deleted' AND `+oldFormatHandlePredicate("handle")+`),
		count(*) FILTER (WHERE pending_handle IS NOT NULL),
		count(*) FILTER (WHERE handle_migrated_at IS NOT NULL),
		count(*) FILTER (WHERE handle_migration_error = 'plc_rejected'),
		count(*) FILTER (WHERE handle_migration_error = 'plc_not_controlled'),
		count(*) FILTER (WHERE handle_migration_error = 'handle_invalid'),
		count(*) FILTER (WHERE handle_migration_error = 'username_unavailable'),
		count(*) FILTER (WHERE `+rollbackCandidatePredicate()+`)
		FROM bridged_actors`, bridge, zone).Scan(&progress.Remaining, &progress.Pending,
		&progress.Migrated, &rejected, &notControlled, &invalid, &unavailable, &progress.RollbackRemaining)
	if err != nil {
		return HandleMigrationProgress{}, fmt.Errorf("count handle migration progress: %w", err)
	}
	progress.Errors = map[string]int{
		string(HandleMigrationPLCRejected):         rejected,
		string(HandleMigrationPLCNotControlled):    notControlled,
		string(HandleMigrationHandleInvalid):       invalid,
		string(HandleMigrationUsernameUnavailable): unavailable,
	}
	return progress, nil
}
