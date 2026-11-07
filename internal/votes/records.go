package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Bridged vote records (PLAN.md decision 21). An eligible bridged voter's vote
// is recorded in two phases, in this order:
//
//  1. The aggregator's vote transaction, under the subject's aggregate lock,
//     marks the row pending (record_at_uri set) and recomputes — the served
//     aggregate drops the vote at once.
//  2. RecordWriter.SyncVoter, with no aggregate lock held, repins the
//     subject's bridgedStats and commits the record in the voter's repo.
//
// A failed phase 2 leaves the row pending: served is one low until the sweep
// repairs it, never one high. Lock order wherever both are taken: the repo
// commit advisory lock, then vote_events rows. Phase 1 never takes the commit
// lock, so the two cannot deadlock.

// RecordMode is the BRIDGED_VOTE_RECORDS switch: which vote directions become
// records. Off stops NEW records only; outstanding creates and deletes drain.
type RecordMode int

const (
	RecordsOff RecordMode = iota
	RecordsUp
	RecordsBoth
)

// ParseRecordMode parses off, up or both.
func ParseRecordMode(value string) (RecordMode, error) {
	switch value {
	case "off":
		return RecordsOff, nil
	case "up":
		return RecordsUp, nil
	case "both":
		return RecordsBoth, nil
	}
	return RecordsOff, errors.NewValidationError("BRIDGED_VOTE_RECORDS",
		fmt.Sprintf("must be off, up or both, got %q", value))
}

func (m RecordMode) String() string {
	switch m {
	case RecordsOff:
		return "off"
	case RecordsUp:
		return "up"
	case RecordsBoth:
		return "both"
	}
	return fmt.Sprintf("RecordMode(%d)", int(m))
}

// allows reports whether a vote in direction becomes a record.
func (m RecordMode) allows(direction string) bool {
	switch m {
	case RecordsBoth:
		return true
	case RecordsUp:
		return direction == directionUp
	}
	return false
}

// RecordWriterOptions are the switches. DryRun logs what would be recorded and
// writes and marks nothing (ScrubVoter excepted: it always deletes). Interval
// paces Run; Batch bounds the voters one Sweep handles.
type RecordWriterOptions struct {
	Mode     RecordMode
	DryRun   bool
	Interval time.Duration
	Batch    int
}

// RecordCommitter is the slice of *repo.Manager the writer commits through.
type RecordCommitter interface {
	ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error)
}

// SubjectRefresher repins a subject's bridgedStats if its aggregate is due
// (*Refresher).
type SubjectRefresher interface {
	RefreshSubject(ctx context.Context, subjectAPID string) error
}

const (
	voteRecordCollection = "social.coves.feed.vote"

	recordPending = "pending"
	recordWritten = "written"
	recordRemoved = "removed"

	// maxRecordOpsPerCommit caps one record commit (and one voter's
	// conversion per sweep); larger backlogs drain over several.
	maxRecordOpsPerCommit = 100

	// maxScrubRounds bounds ScrubVoter's delete-records/delete-rows loop
	// against votes that keep becoming records while it runs.
	maxScrubRounds = 3

	// DefaultRecordInterval and DefaultRecordBatch apply when the options
	// carry a non-positive value.
	DefaultRecordInterval = time.Minute
	DefaultRecordBatch    = 20
)

// errStaleRecordWork is returned from the commit side effect when a row's
// record state no longer matches what the ops were built from (or the voter
// stopped being eligible). The whole commit rolls back; SyncVoter re-reads and
// retries once.
var errStaleRecordWork = stderrors.New("votes: vote record work changed before its commit")

// eligibleVoterSQL is the eligibility predicate for vote_events alias e: a
// bridged person, consent ok, whose repo exists. The vote path never mints.
const eligibleVoterSQL = `EXISTS (
	SELECT 1 FROM bridged_actors ba JOIN repo_state rs ON rs.did = ba.did
	WHERE ba.ap_actor_id = e.voter_ap_id AND ba.actor_type = 'person' AND ba.consent_state = 'ok')`

// RecordWriter commits bridged voters' feed.vote records (phase 2) and runs
// the catch-up/repair sweep.
type RecordWriter struct {
	db        *sql.DB
	committer RecordCommitter
	objects   store.APObjects
	stats     SubjectRefresher
	mode      RecordMode
	dryRun    bool
	interval  time.Duration
	batch     int
	logger    *slog.Logger
}

// NewRecordWriter validates dependencies and builds a RecordWriter.
func NewRecordWriter(db *sql.DB, committer RecordCommitter, objects store.APObjects, stats SubjectRefresher, opts RecordWriterOptions, logger *slog.Logger) (*RecordWriter, error) {
	if db == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if committer == nil {
		return nil, errors.NewValidationError("committer", "must not be nil")
	}
	if objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if stats == nil {
		return nil, errors.NewValidationError("stats", "must not be nil")
	}
	switch opts.Mode {
	case RecordsOff, RecordsUp, RecordsBoth:
	default:
		return nil, errors.NewValidationError("mode", fmt.Sprintf("unknown record mode %d", int(opts.Mode)))
	}
	if opts.Interval <= 0 {
		opts.Interval = DefaultRecordInterval
	}
	if opts.Batch <= 0 {
		opts.Batch = DefaultRecordBatch
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RecordWriter{db: db, committer: committer, objects: objects, stats: stats,
		mode: opts.Mode, dryRun: opts.DryRun, interval: opts.Interval, batch: opts.Batch, logger: logger}, nil
}

// rowQuerier is what *sql.DB and *sql.Tx share for single-row reads.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// eligibleDID returns the voter's DID when the voter is eligible for records.
func eligibleDID(ctx context.Context, q rowQuerier, voter string) (string, bool, error) {
	var did string
	err := q.QueryRowContext(ctx, `
		SELECT ba.did FROM bridged_actors ba
		WHERE ba.ap_actor_id = $1 AND ba.actor_type = 'person' AND ba.consent_state = 'ok'
		  AND EXISTS (SELECT 1 FROM repo_state rs WHERE rs.did = ba.did)`, voter).Scan(&did)
	if stderrors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("votes: read record eligibility of %q: %w", voter, err)
	}
	return did, true, nil
}

// recordATURI names the feed.vote record of one vote row. The rkey is
// DeterministicTID(created_at, activity id): Lemmy votes carry no `published`,
// so the timestamp is when the bridge first recorded the vote.
func recordATURI(did, activityID string, createdAt time.Time) (string, error) {
	rkey, err := repo.DeterministicTID(createdAt, activityID)
	if err != nil {
		return "", fmt.Errorf("votes: derive vote record rkey for %q: %w", activityID, err)
	}
	return "at://" + did + "/" + voteRecordCollection + "/" + rkey.String(), nil
}

// markIfEligible is phase 1's record decision for a just-inserted vote row,
// inside the vote transaction: when the voter is eligible and the mode allows
// the direction, the row becomes pending. A nil writer marks nothing.
func (w *RecordWriter) markIfEligible(ctx context.Context, tx *sql.Tx, eventID int64, voter, direction, activityID string, createdAt time.Time) (bool, error) {
	if w == nil || !w.mode.allows(direction) {
		return false, nil
	}
	did, eligible, err := eligibleDID(ctx, tx, voter)
	if err != nil || !eligible {
		return false, err
	}
	if w.dryRun {
		w.logger.Info("vote records dry run: vote would become a record", "voter", voter, "activity", activityID)
		return false, nil
	}
	atURI, err := recordATURI(did, activityID, createdAt)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE vote_events SET record_at_uri = $2, record_state = 'pending' WHERE id = $1`,
		eventID, atURI); err != nil {
		return false, fmt.Errorf("mark vote %q pending: %w", activityID, err)
	}
	return true, nil
}

// workRow is one vote_events row with record work.
type workRow struct {
	id        int64
	subject   string
	direction string
	undone    bool
	state     string
	atURI     string
	createdAt time.Time
}

// loadRecordRows reads a voter's rows whose record_state is pending or
// written, narrowed by extra (a predicate on those columns), in id order.
func (w *RecordWriter) loadRecordRows(ctx context.Context, voter, extra string) ([]workRow, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, subject_ap_id, direction, undone, record_state, record_at_uri, created_at
		FROM vote_events
		WHERE voter_ap_id = $1 AND record_state IN ('pending', 'written') `+extra+`
		ORDER BY id`, voter)
	if err != nil {
		return nil, fmt.Errorf("votes: load record work of %q: %w", voter, err)
	}
	defer rows.Close()
	var out []workRow
	for rows.Next() {
		var r workRow
		if err := rows.Scan(&r.id, &r.subject, &r.direction, &r.undone, &r.state, &r.atURI, &r.createdAt); err != nil {
			return nil, fmt.Errorf("votes: scan record work of %q: %w", voter, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("votes: iterate record work of %q: %w", voter, err)
	}
	return out, nil
}

// SyncVoter commits a voter's outstanding record work: creates for live
// pending rows, deletes for undone written rows, and 'removed' for rows that
// end without a commit. A stale-work rollback is retried once; whatever is
// still left is the sweep's. In dry-run it only logs.
func (w *RecordWriter) SyncVoter(ctx context.Context, voterAPID string) error {
	if w.dryRun {
		w.logger.Info("vote records dry run: voter's record work not committed", "voter", voterAPID)
		return nil
	}
	err := w.syncOnce(ctx, voterAPID)
	if stderrors.Is(err, errStaleRecordWork) {
		err = w.syncOnce(ctx, voterAPID)
	}
	return err
}

func (w *RecordWriter) syncOnce(ctx context.Context, voter string) error {
	work, err := w.loadRecordRows(ctx, voter, `AND (record_state = 'pending' OR undone)`)
	if err != nil || len(work) == 0 {
		return err
	}
	// A pending row that was undone was never written: it ends with no commit.
	var neverWritten, opRows []workRow
	for _, r := range work {
		if r.undone && r.state == recordPending {
			neverWritten = append(neverWritten, r)
		} else {
			opRows = append(opRows, r)
		}
	}
	if err := w.removePending(ctx, neverWritten); err != nil {
		return err
	}
	if len(opRows) == 0 {
		return nil
	}
	// Creates only for a voter still eligible; an ineligible voter's pending
	// rows wait for the sweep's scrub. Re-checked inside the commit.
	_, eligible, err := eligibleDID(ctx, w.db, voter)
	if err != nil {
		return err
	}
	var errs []error
	for start := 0; start < len(opRows); start += maxRecordOpsPerCommit {
		chunk := opRows[start:min(start+maxRecordOpsPerCommit, len(opRows))]
		if err := w.commitChunk(ctx, voter, eligible, chunk); err != nil {
			if stderrors.Is(err, errStaleRecordWork) {
				return err
			}
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

// stateGuard is one row's state transition, checked inside the commit.
type stateGuard struct {
	id       int64
	from, to string
	// undone, when valid, must also match (sync deletes are of undone rows,
	// creates of live ones; the scrub deletes either).
	undone sql.NullBool
}

// pendingCommit is one DID's share of a chunk.
type pendingCommit struct {
	ops     []repo.RecordOp
	guards  []stateGuard
	creates bool
}

// commitChunk refreshes the chunk's create subjects first (bridgedStats must
// show the dropped count before Coves sees the record, or Coves counts the
// vote twice for a while), then builds the
// ops from the current subject mappings and commits them.
func (w *RecordWriter) commitChunk(ctx context.Context, voter string, eligible bool, chunk []workRow) error {
	var errs []error
	refreshFailed := map[string]bool{}
	if eligible {
		var subjects []string
		seen := map[string]bool{}
		for _, r := range chunk {
			if !r.undone && !seen[r.subject] {
				seen[r.subject] = true
				subjects = append(subjects, r.subject)
			}
		}
		sort.Strings(subjects)
		for _, subject := range subjects {
			if err := w.stats.RefreshSubject(ctx, subject); err != nil {
				refreshFailed[subject] = true
				errs = append(errs, fmt.Errorf("votes: repin stats of %s before its vote records: %w", subject, err))
			}
		}
	}

	commits := map[string]*pendingCommit{}
	var dids []string
	add := func(did string, op repo.RecordOp, guard stateGuard, create bool) {
		c := commits[did]
		if c == nil {
			c = &pendingCommit{}
			commits[did] = c
			dids = append(dids, did)
		}
		c.ops = append(c.ops, op)
		c.guards = append(c.guards, guard)
		c.creates = c.creates || create
	}
	var abandoned []workRow
	for _, r := range chunk {
		uri, err := syntax.ParseATURI(r.atURI)
		if err != nil {
			errs = append(errs, fmt.Errorf("votes: vote %d record uri %q: %w", r.id, r.atURI, err))
			continue
		}
		did, rkey := uri.Authority().String(), uri.RecordKey().String()
		if r.undone {
			add(did, repo.RecordOp{Action: repo.OpActionDelete, Collection: voteRecordCollection, RKey: rkey},
				stateGuard{id: r.id, from: recordWritten, to: recordRemoved, undone: sql.NullBool{Bool: true, Valid: true}}, false)
			continue
		}
		if !eligible || refreshFailed[r.subject] {
			continue
		}
		mapping, err := w.objects.GetByAPID(ctx, r.subject)
		if errors.IsNotFound(err) || (err == nil && mapping.IsDeleted()) {
			// The subject is gone: no record to write, and none is owed.
			abandoned = append(abandoned, r)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("votes: resolve vote record subject %s: %w", r.subject, err))
			continue
		}
		add(did, repo.RecordOp{Action: repo.OpActionCreate, Collection: voteRecordCollection, RKey: rkey,
			Record: map[string]any{
				"$type":     voteRecordCollection,
				"subject":   map[string]any{"uri": mapping.ATURI, "cid": mapping.CID},
				"direction": r.direction,
				"createdAt": r.createdAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			}},
			stateGuard{id: r.id, from: recordPending, to: recordWritten, undone: sql.NullBool{Bool: false, Valid: true}}, true)
	}
	if err := w.removePending(ctx, abandoned); err != nil {
		return err
	}
	for _, did := range dids {
		if err := w.commit(ctx, voter, did, commits[did]); err != nil {
			if stderrors.Is(err, errStaleRecordWork) {
				return err
			}
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

// commit applies one DID's ops in one repo commit. The side effect moves every
// row's state under the commit lock; a guard miss rolls back the whole commit
// (records, firehose event and the other rows' transitions). Never called with
// zero ops.
func (w *RecordWriter) commit(ctx context.Context, voter, did string, c *pendingCommit) error {
	if len(c.ops) == 0 {
		return nil
	}
	_, err := w.committer.ApplyOpsTx(ctx, did, c.ops, func(ctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		if c.creates {
			var eligible bool
			if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS (SELECT 1 FROM bridged_actors
					WHERE ap_actor_id = $1 AND did = $2 AND actor_type = 'person' AND consent_state = 'ok')`,
				voter, did).Scan(&eligible); err != nil {
				return fmt.Errorf("votes: re-check record eligibility of %q: %w", voter, err)
			}
			if !eligible {
				return errStaleRecordWork
			}
		}
		return applyGuards(ctx, tx, c.guards)
	})
	if err != nil {
		if stderrors.Is(err, errStaleRecordWork) {
			return err
		}
		return fmt.Errorf("votes: commit %d vote record ops for %q: %w", len(c.ops), voter, err)
	}
	w.logger.Debug("vote records committed", "voter", voter, "ops", len(c.ops))
	return nil
}

// applyGuards runs each guarded state transition; any miss is stale work.
func applyGuards(ctx context.Context, tx *sql.Tx, guards []stateGuard) error {
	for _, g := range guards {
		result, err := tx.ExecContext(ctx, `
			UPDATE vote_events SET record_state = $3
			WHERE id = $1 AND record_state = $2 AND ($4::boolean IS NULL OR undone = $4)`,
			g.id, g.from, g.to, g.undone)
		if err != nil {
			return fmt.Errorf("votes: move vote %d record %s → %s: %w", g.id, g.from, g.to, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("votes: move vote %d record: rows affected: %w", g.id, err)
		}
		if n == 0 {
			return errStaleRecordWork
		}
	}
	return nil
}

// removePending ends never-written rows at 'removed' in a plain transaction:
// no record exists, so there is nothing to commit. A row that was written
// meanwhile misses its guard and is stale work.
func (w *RecordWriter) removePending(ctx context.Context, rows []workRow) error {
	if len(rows) == 0 {
		return nil
	}
	guards := make([]stateGuard, len(rows))
	for i, r := range rows {
		guards[i] = stateGuard{id: r.id, from: recordPending, to: recordRemoved}
	}
	return inTx(ctx, w.db, func(tx *sql.Tx) error { return applyGuards(ctx, tx, guards) })
}

// ScrubVoter deletes every record the voter has, then the voter's rows (and
// recomputes), repeating while rows still owed a record remain — a vote can
// become a record between the two steps. It runs whatever the mode or dry-run
// say: this is the privacy path.
func (w *RecordWriter) ScrubVoter(ctx context.Context, voterAPID string) error {
	if voterAPID == "" {
		return errors.NewValidationError("voter_ap_id", "must not be empty")
	}
	for range maxScrubRounds {
		if err := w.deleteVoterRecords(ctx, voterAPID); err != nil && !stderrors.Is(err, errStaleRecordWork) {
			return err
		}
		owed, err := scrubVoterRows(ctx, w.db, w.logger, voterAPID)
		if err != nil {
			return err
		}
		if owed == 0 {
			return nil
		}
	}
	return fmt.Errorf("votes: scrub %q: rows still owed a record delete after %d rounds", voterAPID, maxScrubRounds)
}

// deleteVoterRecords deletes every written record of the voter (live or
// undone) and ends pending rows at 'removed'.
func (w *RecordWriter) deleteVoterRecords(ctx context.Context, voter string) error {
	work, err := w.loadRecordRows(ctx, voter, "")
	if err != nil {
		return err
	}
	var pending []workRow
	commits := map[string]*pendingCommit{}
	var dids []string
	for _, r := range work {
		if r.state == recordPending {
			pending = append(pending, r)
			continue
		}
		uri, err := syntax.ParseATURI(r.atURI)
		if err != nil {
			return fmt.Errorf("votes: vote %d record uri %q: %w", r.id, r.atURI, err)
		}
		did := uri.Authority().String()
		c := commits[did]
		if c == nil || len(c.ops) == maxRecordOpsPerCommit {
			if c != nil {
				if err := w.commit(ctx, voter, did, c); err != nil {
					return err
				}
			} else {
				dids = append(dids, did)
			}
			c = &pendingCommit{}
			commits[did] = c
		}
		c.ops = append(c.ops, repo.RecordOp{Action: repo.OpActionDelete, Collection: voteRecordCollection, RKey: uri.RecordKey().String()})
		c.guards = append(c.guards, stateGuard{id: r.id, from: recordWritten, to: recordRemoved})
	}
	if err := w.removePending(ctx, pending); err != nil {
		return err
	}
	for _, did := range dids {
		if err := w.commit(ctx, voter, did, commits[did]); err != nil {
			return err
		}
	}
	return nil
}

// Run sweeps once immediately and then every interval until ctx is done.
func (w *RecordWriter) Run(ctx context.Context) {
	w.runSweep(ctx)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runSweep(ctx)
		}
	}
}

func (w *RecordWriter) runSweep(ctx context.Context) {
	if err := w.Sweep(ctx); err != nil && ctx.Err() == nil {
		w.logger.Warn("vote record sweep left work for the next sweep", "error", err)
	}
}

// Sweep handles up to Batch voters, in three passes sharing that budget:
// repair (eligible voters with outstanding work), scrub (voters holding
// record rows who are no longer eligible), and — unless the mode is off —
// conversion (eligible voters with live unrecorded votes in an allowed
// direction, at any age). In dry-run it logs what it would do and touches
// nothing.
func (w *RecordWriter) Sweep(ctx context.Context) error {
	upOnly := w.mode == RecordsUp
	repairQuery := `
		SELECT DISTINCT e.voter_ap_id FROM vote_events e
		WHERE e.record_state IN ('pending', 'written') AND (e.record_state = 'pending' OR e.undone)
		  AND ` + eligibleVoterSQL + `
		ORDER BY 1 LIMIT $1`
	scrubQuery := `
		SELECT DISTINCT e.voter_ap_id FROM vote_events e
		WHERE e.record_state IN ('pending', 'written') AND NOT ` + eligibleVoterSQL + `
		ORDER BY 1 LIMIT $1`
	convertQuery := `
		SELECT DISTINCT e.voter_ap_id FROM vote_events e
		WHERE NOT e.undone AND e.record_at_uri IS NULL AND (NOT $2 OR e.direction = 'up')
		  AND ` + eligibleVoterSQL + `
		ORDER BY 1 LIMIT $1`

	if w.dryRun {
		repair, err := w.sweepVoters(ctx, repairQuery, w.batch)
		if err != nil {
			return err
		}
		scrub, err := w.sweepVoters(ctx, scrubQuery, w.batch)
		if err != nil {
			return err
		}
		var convert []string
		if w.mode != RecordsOff {
			if convert, err = w.sweepVoters(ctx, convertQuery, w.batch, upOnly); err != nil {
				return err
			}
		}
		if len(repair)+len(scrub)+len(convert) > 0 {
			w.logger.Info("vote records dry run: sweep would act",
				"repair_voters", len(repair), "scrub_voters", len(scrub), "convert_voters", len(convert))
		}
		return nil
	}

	budget := w.batch
	var errs []error
	repair, err := w.sweepVoters(ctx, repairQuery, budget)
	if err != nil {
		return err
	}
	budget -= len(repair)
	for _, voter := range repair {
		if err := w.SyncVoter(ctx, voter); err != nil {
			errs = append(errs, fmt.Errorf("repair %q: %w", voter, err))
		}
	}
	scrub, err := w.sweepVoters(ctx, scrubQuery, budget)
	if err != nil {
		return stderrors.Join(append(errs, err)...)
	}
	budget -= len(scrub)
	for _, voter := range scrub {
		if err := w.ScrubVoter(ctx, voter); err != nil {
			errs = append(errs, fmt.Errorf("scrub ineligible %q: %w", voter, err))
		}
	}
	if w.mode != RecordsOff {
		convert, err := w.sweepVoters(ctx, convertQuery, budget, upOnly)
		if err != nil {
			return stderrors.Join(append(errs, err)...)
		}
		for _, voter := range convert {
			if err := w.convertVoter(ctx, voter, upOnly); err != nil {
				errs = append(errs, fmt.Errorf("convert %q: %w", voter, err))
				continue
			}
			if err := w.SyncVoter(ctx, voter); err != nil {
				errs = append(errs, fmt.Errorf("record converted %q: %w", voter, err))
			}
		}
	}
	return stderrors.Join(errs...)
}

// sweepVoters runs one of Sweep's voter queries ($1 = limit).
func (w *RecordWriter) sweepVoters(ctx context.Context, query string, limit int, args ...any) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := w.db.QueryContext(ctx, query, append([]any{limit}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("votes: select record sweep voters: %w", err)
	}
	defer rows.Close()
	var voters []string
	for rows.Next() {
		var voter string
		if err := rows.Scan(&voter); err != nil {
			return nil, fmt.Errorf("votes: scan record sweep voter: %w", err)
		}
		voters = append(voters, voter)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("votes: iterate record sweep voters: %w", err)
	}
	return voters, nil
}

// convertVoter marks up to maxRecordOpsPerCommit of an eligible voter's live
// unrecorded votes pending, under their aggregates' locks taken in subject
// order (the scrub's discipline), and recomputes those aggregates.
func (w *RecordWriter) convertVoter(ctx context.Context, voter string, upOnly bool) error {
	did, eligible, err := eligibleDID(ctx, w.db, voter)
	if err != nil || !eligible {
		return err
	}
	type candidate struct {
		id         int64
		subject    string
		activityID string
		createdAt  time.Time
	}
	var candidates []candidate
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, subject_ap_id, activity_id, created_at FROM vote_events
		WHERE voter_ap_id = $1 AND NOT undone AND record_at_uri IS NULL AND (NOT $2 OR direction = 'up')
		ORDER BY subject_ap_id, id
		LIMIT $3`, voter, upOnly, maxRecordOpsPerCommit)
	if err != nil {
		return fmt.Errorf("votes: select convertible votes of %q: %w", voter, err)
	}
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.subject, &c.activityID, &c.createdAt); err != nil {
			_ = rows.Close()
			return fmt.Errorf("votes: scan convertible vote of %q: %w", voter, err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("votes: iterate convertible votes of %q: %w", voter, err)
	}
	_ = rows.Close()
	if len(candidates) == 0 {
		return nil
	}

	var subjects []string
	for i, c := range candidates {
		if i == 0 || c.subject != candidates[i-1].subject {
			subjects = append(subjects, c.subject) // already in subject order
		}
	}
	marked := 0
	if err := inTx(ctx, w.db, func(tx *sql.Tx) error {
		for _, subject := range subjects {
			var one int
			err := tx.QueryRowContext(ctx, `
				SELECT 1 FROM vote_aggregates WHERE subject_ap_id = $1 FOR UPDATE`, subject).Scan(&one)
			if err != nil && !stderrors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lock vote aggregate for %q: %w", subject, err)
			}
		}
		marked = 0
		for _, c := range candidates {
			atURI, err := recordATURI(did, c.activityID, c.createdAt)
			if err != nil {
				return err
			}
			// Guarded: a vote superseded since the read above stays as it is.
			result, err := tx.ExecContext(ctx, `
				UPDATE vote_events SET record_at_uri = $2, record_state = 'pending'
				WHERE id = $1 AND NOT undone AND record_at_uri IS NULL`, c.id, atURI)
			if err != nil {
				return fmt.Errorf("mark vote %d pending: %w", c.id, err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("mark vote %d pending: rows affected: %w", c.id, err)
			}
			marked += int(n)
		}
		for _, subject := range subjects {
			if err := recomputeAggregate(ctx, tx, subject); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	w.logger.Info("bridged votes marked for records", "voter", voter, "votes", marked)
	return nil
}
