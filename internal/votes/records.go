package votes

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// RED compile stubs for the bridged-vote-records tests. No logic yet.

type RecordMode int

const (
	RecordsOff RecordMode = iota
	RecordsUp
	RecordsBoth
)

type RecordWriterOptions struct {
	Mode     RecordMode
	DryRun   bool
	Interval time.Duration
	Batch    int
}

type RecordCommitter interface {
	ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error)
}

type SubjectRefresher interface {
	RefreshSubject(ctx context.Context, subjectAPID string) error
}

type RecordWriter struct{}

func NewRecordWriter(db *sql.DB, committer RecordCommitter, objects store.APObjects, stats SubjectRefresher, opts RecordWriterOptions, logger *slog.Logger) (*RecordWriter, error) {
	return nil, nil
}

func (a *Aggregator) SetRecordWriter(writer *RecordWriter) {}
