package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"expvar"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Bridged vote records (PLAN.md decision 21). An eligible bridged voter's vote
// is recorded in two phases, in this order:
//
//  1. Once the vote is verified at the voter's origin (inline, or later by
//     the sweep), a transaction under the subject's aggregate lock marks the
//     row pending (record_at_uri set) and recomputes — the served aggregate
//     drops the vote at once. An unverified vote never leaves the aggregate.
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
	// RecordsOff makes no new records.
	RecordsOff RecordMode = iota
	// RecordsUp records Likes only; a Dislike stays aggregate.
	RecordsUp
	// RecordsBoth records Likes and Dislikes.
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

// String returns the switch value ParseRecordMode accepts.
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
// paces Run, and a sweep spends at most 80% of it verifying origins; Batch
// bounds the voters each of a Sweep's passes handles.
type RecordWriterOptions struct {
	Mode     RecordMode
	DryRun   bool
	Interval time.Duration
	Batch    int
	// Verifier checks each vote at the voter's origin before it may become a
	// record (required unless Mode is off); Window bounds which votes are
	// still checkable; VerifyBatch bounds a sweep's verification candidates;
	// Sleep waits between verification rounds.
	Verifier    OriginVerifier
	Window      time.Duration
	VerifyBatch int
	Sleep       func(ctx context.Context, d time.Duration) error
	// Now is the sweep's clock for its verification budget (default
	// time.Now); tests pass the verifier's clock.
	Now func() time.Time
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

	// DefaultVerifyWindow and DefaultVerifyBatch apply when the options carry
	// a non-positive value. Lemmy prunes sent activities at 7 days.
	DefaultVerifyWindow = 144 * time.Hour
	DefaultVerifyBatch  = 300

	// A sweep spends at most verifyBudgetPercent of its interval verifying,
	// in rounds verifyRoundPause apart, taking at most verifyPerHost
	// candidates from one voter host.
	verifyBudgetPercent = 80
	verifyRoundPause    = 100 * time.Millisecond
	verifyPerHost       = 60
	// verifyRetryAfter keeps an 'unavailable' vote out of the queue for a
	// while after its check (the verifier also backs the host off).
	verifyRetryAfter = originFirstBackoff
	// maxExpirePerSweep bounds the unavailable → expired UPDATE.
	maxExpirePerSweep = 1000
	// verifyWarnMargin: warn when the oldest pending vote is this close to
	// leaving the window unchecked (at most a quarter of the window).
	verifyWarnMargin = 24 * time.Hour
	// inlineVerifyTimeout bounds the live path's origin fetch; a vote whose
	// check runs out stays unchecked for the sweep.
	inlineVerifyTimeout = 2 * time.Second
	// sweepVerifyTimeout bounds one sweep fetch (or the budget left, if
	// less), so a tarpit host costs at most this per check; the check is
	// left unchecked for a later sweep.
	sweepVerifyTimeout = 3 * time.Second
)

// Origin verification metrics. The gauges are recomputed each sweep; the
// verdict map counts every verdict stored, plus votes deferred for lack of a
// fetch slot.
var (
	voteVerifyBacklog       = expvar.NewInt("tidepool_vote_verify_backlog")
	voteVerifyOldestPending = expvar.NewInt("tidepool_vote_verify_oldest_pending_seconds")
	voteVerifyVerdicts      = expvar.NewMap("tidepool_vote_verify_verdicts")
)

// errStaleRecordWork is returned from the commit side effect when a row's
// record state no longer matches what the ops were built from (or the voter
// stopped being eligible). The whole commit rolls back; SyncVoter re-reads and
// retries once.
var errStaleRecordWork = stderrors.New("votes: vote record work changed before its commit")

// onlyStaleRecordWork reports whether err is stale work and nothing else
// (every branch of a joined error is errStaleRecordWork).
func onlyStaleRecordWork(err error) bool {
	var joined interface{ Unwrap() []error }
	if stderrors.As(err, &joined) {
		for _, branch := range joined.Unwrap() {
			if !onlyStaleRecordWork(branch) {
				return false
			}
		}
		return true
	}
	return stderrors.Is(err, errStaleRecordWork)
}

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
	verifier  OriginVerifier
	window    time.Duration
	verifyMax int
	sleep     func(ctx context.Context, d time.Duration) error
	now       func() time.Time
	// inlineSlot admits one live-path origin fetch at a time, so a slow
	// origin cannot hold more than one ingest worker.
	inlineSlot chan struct{}
	logger     *slog.Logger
	// Each sweep pass resumes after the last voter it handled, wrapping, so
	// voters whose work keeps failing cannot starve the rest. Sweep runs on
	// one goroutine (Run).
	repairCursor, scrubCursor, convertCursor string
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
	// There is no unverified record path.
	if opts.Verifier == nil && opts.Mode != RecordsOff {
		return nil, errors.NewValidationError("verifier", "must not be nil unless the record mode is off")
	}
	if opts.Window <= 0 {
		opts.Window = DefaultVerifyWindow
	}
	if opts.VerifyBatch <= 0 {
		opts.VerifyBatch = DefaultVerifyBatch
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RecordWriter{db: db, committer: committer, objects: objects, stats: stats,
		mode: opts.Mode, dryRun: opts.DryRun, interval: opts.Interval, batch: opts.Batch,
		verifier: opts.Verifier, window: opts.Window, verifyMax: opts.VerifyBatch, sleep: opts.Sleep,
		now: opts.Now, inlineSlot: make(chan struct{}, 1), logger: logger}, nil
}

// sleepContext waits d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

// verifyLive checks a new vote at its origin before the vote transaction, when
// it could become a record: the mode allows the direction, not dry-run, the
// voter is eligible and the activity id is new. It never fetches under a lock
// and never waits for a slot: at most one inline fetch runs at a time, for at
// most inlineVerifyTimeout. "" means unchecked (the sweep picks it up).
func (w *RecordWriter) verifyLive(ctx context.Context, activityID, voter, subject, direction string) (OriginVerdict, error) {
	if !w.mode.allows(direction) {
		return "", nil
	}
	var seen bool
	if err := w.db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM vote_events WHERE activity_id = $1)`, activityID).Scan(&seen); err != nil {
		return "", fmt.Errorf("votes: dedupe vote %q before its origin check: %w", activityID, err)
	}
	if seen {
		return "", nil
	}
	_, eligible, err := eligibleDID(ctx, w.db, voter)
	if err != nil || !eligible {
		return "", err
	}
	if w.dryRun {
		w.logger.Debug("vote records dry run: vote would be checked at its origin", "voter", voter, "activity", activityID)
		return "", nil
	}
	select {
	case w.inlineSlot <- struct{}{}:
	default:
		voteVerifyVerdicts.Add("deferred", 1)
		return "", nil
	}
	defer func() { <-w.inlineSlot }()
	verifyCtx, cancel := context.WithTimeout(ctx, inlineVerifyTimeout)
	defer cancel()
	verdict, deferred := w.verifier.Verify(verifyCtx, voteClaim(activityID, voter, subject, direction))
	if deferred {
		voteVerifyVerdicts.Add("deferred", 1)
		return "", nil
	}
	return verdict, nil
}

// voteClaim is the claim a stored vote makes.
func voteClaim(activityID, voter, subject, direction string) VoteClaim {
	voteType := ap.TypeLike
	if direction == directionDown {
		voteType = ap.TypeDislike
	}
	return VoteClaim{ActivityID: activityID, Actor: voter, Object: subject, Type: voteType}
}

// markIfEligible is phase 1's record decision for a just-inserted vote row,
// inside the vote transaction. It stores verifyLive's verdict ("" stores
// nothing; the caller counts the verdict once the transaction commits). The
// row becomes pending only when the vote was verified, the voter is still
// eligible and the mode allows the direction.
func (w *RecordWriter) markIfEligible(ctx context.Context, tx *sql.Tx, eventID int64, voter, direction, activityID string, createdAt time.Time, verdict OriginVerdict) (bool, error) {
	if verdict != "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE vote_events SET origin_check = $2, origin_checked_at = now() WHERE id = $1`,
			eventID, string(verdict)); err != nil {
			return false, fmt.Errorf("store origin check of vote %q: %w", activityID, err)
		}
	}
	if verdict != OriginVerified || w.dryRun || !w.mode.allows(direction) {
		return false, nil
	}
	did, eligible, err := eligibleDID(ctx, tx, voter)
	if err != nil || !eligible {
		return false, err
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
	for _, chunk := range chunkBySubject(opRows) {
		if err := w.commitChunk(ctx, voter, eligible, chunk); err != nil {
			if stderrors.Is(err, errStaleRecordWork) {
				return stderrors.Join(append(errs, err)...)
			}
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

// chunkBySubject splits work rows into chunks of at most
// maxRecordOpsPerCommit, never splitting one subject's rows (a flip's delete
// and create commit together) unless that subject alone exceeds the cap.
// Subjects keep the order of their first row.
func chunkBySubject(rows []workRow) [][]workRow {
	var subjects []string
	bySubject := map[string][]workRow{}
	for _, r := range rows {
		if _, ok := bySubject[r.subject]; !ok {
			subjects = append(subjects, r.subject)
		}
		bySubject[r.subject] = append(bySubject[r.subject], r)
	}
	var chunks [][]workRow
	var current []workRow
	for _, subject := range subjects {
		group := bySubject[subject]
		if len(current) > 0 && len(current)+len(group) > maxRecordOpsPerCommit {
			chunks = append(chunks, current)
			current = nil
		}
		for len(group) > maxRecordOpsPerCommit {
			chunks = append(chunks, group[:maxRecordOpsPerCommit])
			group = group[maxRecordOpsPerCommit:]
		}
		current = append(current, group...)
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
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
// vote twice for a while), then builds the ops from the current subject
// mappings and commits them. A live vote whose subject mapping is missing or
// soft-deleted goes back to the aggregate instead.
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
	// A subject whose create is held back this round (refresh failure, a
	// failed mapping lookup) holds back its deletes too: a flip's delete never
	// commits without its create.
	held := map[string]bool{}
	for subject := range refreshFailed {
		held[subject] = true
	}
	type parsedRow struct {
		row       workRow
		did, rkey string
		mapping   *store.APObjectMapping
	}
	var parsed []parsedRow
	var unmapped []workRow
	for _, r := range chunk {
		uri, err := syntax.ParseATURI(r.atURI)
		if err != nil {
			errs = append(errs, fmt.Errorf("votes: vote %d record uri %q: %w", r.id, r.atURI, err))
			if !r.undone {
				held[r.subject] = true
			}
			continue
		}
		p := parsedRow{row: r, did: uri.Authority().String(), rkey: uri.RecordKey().String()}
		if !r.undone {
			if !eligible || held[r.subject] {
				continue
			}
			mapping, err := w.objects.GetByAPID(ctx, r.subject)
			if errors.IsNotFound(err) || (err == nil && mapping.IsDeleted()) {
				// No subject to point at; a soft delete can be restored, so
				// the vote is counted in the aggregate until then.
				unmapped = append(unmapped, r)
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("votes: resolve vote record subject %s: %w", r.subject, err))
				held[r.subject] = true
				continue
			}
			p.mapping = mapping
		}
		parsed = append(parsed, p)
	}
	for _, p := range parsed {
		r := p.row
		if held[r.subject] {
			continue
		}
		if r.undone {
			add(p.did, repo.RecordOp{Action: repo.OpActionDelete, Collection: voteRecordCollection, RKey: p.rkey},
				stateGuard{id: r.id, from: recordWritten, to: recordRemoved, undone: sql.NullBool{Bool: true, Valid: true}}, false)
			continue
		}
		add(p.did, repo.RecordOp{Action: repo.OpActionCreate, Collection: voteRecordCollection, RKey: p.rkey,
			Record: map[string]any{
				"$type":     voteRecordCollection,
				"subject":   map[string]any{"uri": p.mapping.ATURI, "cid": p.mapping.CID},
				"direction": r.direction,
				"createdAt": r.createdAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			}},
			stateGuard{id: r.id, from: recordPending, to: recordWritten, undone: sql.NullBool{Bool: false, Valid: true}}, true)
	}
	if err := w.returnToAggregate(ctx, voter, unmapped); err != nil {
		errs = append(errs, err)
	}
	for _, did := range dids {
		if err := w.commit(ctx, voter, did, commits[did]); err != nil {
			if stderrors.Is(err, errStaleRecordWork) {
				return stderrors.Join(append(errs, err)...)
			}
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

// commit applies one DID's ops in one repo commit. The side effect moves every
// row's state under the commit lock; a guard miss rolls back the whole commit
// (records, firehose event and the other rows' transitions). With zero ops it
// does nothing: ApplyOpsTx is never called empty.
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

// returnToAggregate clears the record columns of live pending votes and
// recomputes, each subject in its own transaction under its aggregate lock
// (the vote path's lock; no commit lock is held). A row no longer pending and
// live is left alone. Conversion skips subjects without a live mapping, so the
// vote stays aggregate until its subject is restored.
func (w *RecordWriter) returnToAggregate(ctx context.Context, voter string, rows []workRow) error {
	var errs []error
	for _, r := range rows {
		returned := false
		err := inTx(ctx, w.db, func(tx *sql.Tx) error {
			var one int
			err := tx.QueryRowContext(ctx, `
				SELECT 1 FROM vote_aggregates WHERE subject_ap_id = $1 FOR UPDATE`, r.subject).Scan(&one)
			if err != nil && !stderrors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lock vote aggregate for %q: %w", r.subject, err)
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE vote_events SET record_at_uri = NULL, record_state = NULL
				WHERE id = $1 AND record_state = 'pending' AND NOT undone`, r.id)
			if err != nil {
				return fmt.Errorf("return vote %d to the aggregate: %w", r.id, err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("return vote %d to the aggregate: rows affected: %w", r.id, err)
			}
			if n == 0 {
				return nil
			}
			returned = true
			return recomputeAggregate(ctx, tx, r.subject)
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("votes: %w", err))
			continue
		}
		if returned {
			w.logger.Info("vote returned to the aggregate: subject mapping missing or soft-deleted",
				"voter", voter, "subject", r.subject, "vote_id", r.id)
		}
	}
	return stderrors.Join(errs...)
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

// Sweep runs repair (eligible voters with outstanding work) and scrub (voters
// holding record rows who are no longer eligible, and opted-out or deleted
// voters with any rows left), each over up to Batch voters, then — unless the
// mode is off — origin verification (verifyOrigins) and conversion of up to
// Batch voters' verified live unrecorded votes in an allowed direction on a
// live subject hosted here (a verified vote at any age: verified is final).
// In dry-run it logs what it would do and touches nothing; it previews only
// already-verified conversions.
func (w *RecordWriter) Sweep(ctx context.Context) error {
	upOnly := w.mode == RecordsUp
	// $1 limit, $2/$3 the voter window (sweepVoters), $4 upOnly.
	repairQuery := `
		SELECT DISTINCT e.voter_ap_id FROM vote_events e
		WHERE e.record_state IN ('pending', 'written') AND (e.record_state = 'pending' OR e.undone)
		  AND ` + eligibleVoterSQL + voterWindowSQL
	// Scrub: voters holding record rows who are no longer eligible, and
	// voters who opted out or were deleted while any row of theirs is left (a
	// scrub that failed after its record deletes leaves live 'removed' rows
	// nothing else revisits). Never a voter merely for having no repo.
	scrubQuery := `
		SELECT DISTINCT e.voter_ap_id FROM (
			SELECT e.voter_ap_id FROM vote_events e
			WHERE e.record_state IN ('pending', 'written') AND NOT ` + eligibleVoterSQL + `
			UNION
			SELECT ba.ap_actor_id FROM bridged_actors ba
			WHERE ba.consent_state IN ('nobridge', 'deleted')
			  AND EXISTS (SELECT 1 FROM vote_events v WHERE v.voter_ap_id = ba.ap_actor_id)
		) e
		WHERE TRUE` + voterWindowSQL
	convertQuery := `
		SELECT DISTINCT e.voter_ap_id FROM vote_events e
		WHERE NOT e.undone AND e.record_at_uri IS NULL AND e.origin_check = 'verified'
		  AND (NOT $4 OR e.direction = 'up')
		  AND ` + hostedSubjectSQL + `
		  AND ` + eligibleVoterSQL + voterWindowSQL

	if w.dryRun {
		// Counted from a throwaway cursor: a dry run moves no pass forward.
		var cursor string
		repair, err := w.sweepVoters(ctx, repairQuery, &cursor)
		if err != nil {
			return err
		}
		cursor = ""
		scrub, err := w.sweepVoters(ctx, scrubQuery, &cursor)
		if err != nil {
			return err
		}
		var convert []string
		if w.mode != RecordsOff {
			cursor = ""
			if convert, err = w.sweepVoters(ctx, convertQuery, &cursor, upOnly); err != nil {
				return err
			}
		}
		if len(repair)+len(scrub)+len(convert) > 0 {
			w.logger.Info("vote records dry run: sweep would act",
				"repair_voters", len(repair), "scrub_voters", len(scrub), "convert_voters", len(convert))
		}
		return nil
	}

	var errs []error
	repair, err := w.sweepVoters(ctx, repairQuery, &w.repairCursor)
	if err != nil {
		return err
	}
	for _, voter := range repair {
		if err := w.SyncVoter(ctx, voter); err != nil {
			errs = append(errs, fmt.Errorf("repair %q: %w", voter, err))
		}
	}
	scrub, err := w.sweepVoters(ctx, scrubQuery, &w.scrubCursor)
	if err != nil {
		return stderrors.Join(append(errs, err)...)
	}
	for _, voter := range scrub {
		if err := w.ScrubVoter(ctx, voter); err != nil {
			errs = append(errs, fmt.Errorf("scrub ineligible %q: %w", voter, err))
		}
	}
	if w.mode != RecordsOff {
		if err := w.verifyOrigins(ctx, upOnly); err != nil {
			if ctx.Err() != nil {
				return stderrors.Join(append(errs, err)...)
			}
			errs = append(errs, err)
		}
		convert, err := w.sweepVoters(ctx, convertQuery, &w.convertCursor, upOnly)
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

// voterWindowSQL ends a sweep voter query: voters after $2, and at or before
// $3 when $3 is not NULL, in order, at most $1.
const voterWindowSQL = `
		  AND e.voter_ap_id > $2 AND ($3::text IS NULL OR e.voter_ap_id <= $3)
		ORDER BY 1 LIMIT $1`

// hostedSubjectSQL holds for a vote_events row (alias e) whose subject
// mapping exists, is not soft-deleted, and whose record is in a repo hosted
// here. Only such a subject's bridgedStats can be repinned,
// so only there may the sweep convert an already-counted vote: on a native
// Coves subject Coves keeps serving the bridged count from its last poll and
// the record would double-count. Native subjects get records only from
// inline-verified votes, which were never counted.
const hostedSubjectSQL = `EXISTS (
	SELECT 1 FROM ap_objects o JOIN repo_state rs ON rs.did = o.did
	WHERE o.ap_id = e.subject_ap_id AND o.deleted_at IS NULL)`

// sweepVoters returns up to Batch voters of one sweep query, starting after
// *cursor and wrapping to the start, and moves *cursor to the last one.
func (w *RecordWriter) sweepVoters(ctx context.Context, query string, cursor *string, args ...any) ([]string, error) {
	voters, err := w.queryVoters(ctx, query, w.batch, *cursor, sql.NullString{}, args)
	if err != nil {
		return nil, err
	}
	if len(voters) < w.batch && *cursor != "" {
		wrapped, err := w.queryVoters(ctx, query, w.batch-len(voters), "", sql.NullString{String: *cursor, Valid: true}, args)
		if err != nil {
			return nil, err
		}
		voters = append(voters, wrapped...)
	}
	if len(voters) > 0 {
		*cursor = voters[len(voters)-1]
	}
	return voters, nil
}

func (w *RecordWriter) queryVoters(ctx context.Context, query string, limit int, after string, through sql.NullString, args []any) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, query, append([]any{limit, after, through}, args...)...)
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

// convertVoter marks up to maxRecordOpsPerCommit of an eligible voter's
// verified live unrecorded votes pending, under their aggregates' locks taken in subject
// order (the scrub's discipline), and recomputes those aggregates.
func (w *RecordWriter) convertVoter(ctx context.Context, voter string, upOnly bool) error {
	did, eligible, err := eligibleDID(ctx, w.db, voter)
	if err != nil || !eligible {
		return err
	}
	candidates, err := w.convertibleVotes(ctx, voter, upOnly)
	if err != nil || len(candidates) == 0 {
		return err
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
			// Guarded: a vote superseded since the read above stays as it is,
			// and only a vote verified at its origin becomes a record.
			result, err := tx.ExecContext(ctx, `
				UPDATE vote_events SET record_at_uri = $2, record_state = 'pending'
				WHERE id = $1 AND NOT undone AND record_at_uri IS NULL AND origin_check = 'verified'`, c.id, atURI)
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

// convertCandidate is one live unrecorded vote convertVoter may mark.
type convertCandidate struct {
	id         int64
	subject    string
	activityID string
	createdAt  time.Time
}

// convertibleVotes reads up to maxRecordOpsPerCommit of the voter's verified
// live unrecorded votes on live subjects, in subject order.
func (w *RecordWriter) convertibleVotes(ctx context.Context, voter string, upOnly bool) ([]convertCandidate, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT e.id, e.subject_ap_id, e.activity_id, e.created_at FROM vote_events e
		WHERE e.voter_ap_id = $1 AND NOT e.undone AND e.record_at_uri IS NULL AND e.origin_check = 'verified'
		  AND (NOT $2 OR e.direction = 'up')
		  AND `+hostedSubjectSQL+`
		ORDER BY e.subject_ap_id, e.id
		LIMIT $3`, voter, upOnly, maxRecordOpsPerCommit)
	if err != nil {
		return nil, fmt.Errorf("votes: select convertible votes of %q: %w", voter, err)
	}
	defer rows.Close()
	var candidates []convertCandidate
	for rows.Next() {
		var c convertCandidate
		if err := rows.Scan(&c.id, &c.subject, &c.activityID, &c.createdAt); err != nil {
			return nil, fmt.Errorf("votes: scan convertible vote of %q: %w", voter, err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("votes: iterate convertible votes of %q: %w", voter, err)
	}
	return candidates, nil
}

// verifyCandidateSQL selects the verification queue: live unrecorded votes
// inside the window ($1 seconds) never checked, or 'unavailable' and checked
// more than $2 seconds ago, in an allowed direction ($3 upOnly), on a live
// subject, by an eligible voter. Within a host, never-checked votes go before
// retryable ones (one failing activity cannot block a host's fresh votes),
// then oldest first; at most $4 per voter host. Rows are taken round-robin
// across hosts (each host's first, then each host's second, ...) up to $5, so
// hosts with many old votes cannot fill the batch.
const verifyCandidateSQL = `
	SELECT id, activity_id, voter_ap_id, subject_ap_id, direction, host FROM (
		SELECT e.id, e.activity_id, e.voter_ap_id, e.subject_ap_id, e.direction, e.created_at,
			split_part(e.voter_ap_id, '/', 3) AS host,
			row_number() OVER (PARTITION BY split_part(e.voter_ap_id, '/', 3) ORDER BY e.origin_check IS NOT NULL, e.created_at, e.id) AS n
		FROM vote_events e
		WHERE NOT e.undone AND e.record_at_uri IS NULL
		  AND (e.origin_check IS NULL
		       OR (e.origin_check = 'unavailable' AND e.origin_checked_at < now() - make_interval(secs => $2)))
		  AND e.created_at > now() - make_interval(secs => $1)
		  AND (NOT $3 OR e.direction = 'up')
		  AND ` + hostedSubjectSQL + `
		  AND ` + eligibleVoterSQL + `
	) c
	WHERE n <= $4
	ORDER BY n, created_at, id
	LIMIT $5`

// verifyCandidate is one vote waiting for its origin check.
type verifyCandidate struct {
	id    int64
	claim VoteClaim
}

// verifyOrigins is the sweep's verification pass. It expires 'unavailable'
// votes that left the window, then checks the queue in rounds: each round
// tries the first vote of each voter host, so a host in backoff holds only its
// own queue; a deferred vote waits for the next round. Rounds are
// verifyRoundPause apart, through the sleep seam, until the wall-clock budget
// (verifyBudgetPercent of the interval, fetch time included) is spent or the
// queue is empty. No fetch may outlast the budget. On every way out it
// publishes the backlog.
func (w *RecordWriter) verifyOrigins(ctx context.Context, upOnly bool) (err error) {
	var errs []error
	defer func() {
		if reportErr := w.reportVerifyBacklog(ctx, upOnly); reportErr != nil {
			errs = append(errs, reportErr)
		}
		err = stderrors.Join(errs...)
	}()
	if err := w.expireUnavailable(ctx); err != nil {
		errs = append(errs, err)
	}

	queues, err := w.verifyQueues(ctx, upOnly)
	if err != nil {
		errs = append(errs, err)
		return nil
	}
	deadline := w.now().Add(w.interval * verifyBudgetPercent / 100)
	remaining := 0
	for _, queue := range queues {
		remaining += len(queue)
	}
	defer func() { voteVerifyVerdicts.Add("deferred", int64(remaining)) }()
	for remaining > 0 {
		for i, queue := range queues {
			if len(queue) == 0 {
				continue
			}
			left := deadline.Sub(w.now())
			if left <= 0 || ctx.Err() != nil {
				return nil
			}
			c := queue[0]
			fetchCtx, cancel := context.WithTimeout(ctx, min(sweepVerifyTimeout, left))
			verdict, deferred := w.verifier.Verify(fetchCtx, c.claim)
			cancel()
			if deferred {
				continue
			}
			queues[i] = queue[1:]
			remaining--
			if err := w.storeVerdict(ctx, c, verdict); err != nil {
				errs = append(errs, err)
			}
		}
		if remaining == 0 || w.now().Add(verifyRoundPause).After(deadline) || ctx.Err() != nil {
			return nil
		}
		if err := w.sleep(ctx, verifyRoundPause); err != nil {
			errs = append(errs, err)
			return nil
		}
	}
	return nil
}

// verifyQueues reads the verification queue grouped by voter host, hosts in
// order of their first queued vote.
func (w *RecordWriter) verifyQueues(ctx context.Context, upOnly bool) ([][]verifyCandidate, error) {
	rows, err := w.db.QueryContext(ctx, verifyCandidateSQL,
		w.window.Seconds(), verifyRetryAfter.Seconds(), upOnly, verifyPerHost, w.verifyMax)
	if err != nil {
		return nil, fmt.Errorf("votes: select origin check candidates: %w", err)
	}
	defer rows.Close()
	var queues [][]verifyCandidate
	byHost := map[string]int{}
	for rows.Next() {
		var c verifyCandidate
		var voter, subject, direction, host string
		if err := rows.Scan(&c.id, &c.claim.ActivityID, &voter, &subject, &direction, &host); err != nil {
			return nil, fmt.Errorf("votes: scan origin check candidate: %w", err)
		}
		c.claim = voteClaim(c.claim.ActivityID, voter, subject, direction)
		i, ok := byHost[host]
		if !ok {
			i = len(queues)
			byHost[host] = i
			queues = append(queues, nil)
		}
		queues[i] = append(queues[i], c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("votes: iterate origin check candidates: %w", err)
	}
	return queues, nil
}

// storeVerdict records a vote's origin verdict unless the vote changed since
// it was selected (undone, recorded, or checked meanwhile).
func (w *RecordWriter) storeVerdict(ctx context.Context, c verifyCandidate, verdict OriginVerdict) error {
	result, err := w.db.ExecContext(ctx, `
		UPDATE vote_events SET origin_check = $2, origin_checked_at = now()
		WHERE id = $1 AND NOT undone AND record_at_uri IS NULL
		  AND (origin_check IS NULL OR origin_check = 'unavailable')`, c.id, string(verdict))
	if err != nil {
		return fmt.Errorf("votes: store origin check of vote %d: %w", c.id, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("votes: store origin check of vote %d: rows affected: %w", c.id, err)
	}
	if n == 1 {
		voteVerifyVerdicts.Add(string(verdict), 1)
	}
	if verdict != OriginVerified {
		w.logger.Debug("vote not verified at its origin; it stays aggregate",
			"activity", c.claim.ActivityID, "verdict", string(verdict))
	}
	return nil
}

// expireUnavailable ends 'unavailable' votes that left the window at
// 'expired'. Never-checked votes outside the window are left alone: they are
// simply never candidates.
func (w *RecordWriter) expireUnavailable(ctx context.Context) error {
	result, err := w.db.ExecContext(ctx, `
		UPDATE vote_events SET origin_check = 'expired', origin_checked_at = now()
		WHERE id IN (
			SELECT id FROM vote_events
			WHERE NOT undone AND record_at_uri IS NULL AND origin_check = 'unavailable'
			  AND created_at <= now() - make_interval(secs => $1)
			LIMIT $2)`, w.window.Seconds(), maxExpirePerSweep)
	if err != nil {
		return fmt.Errorf("votes: expire unavailable origin checks: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("votes: expire unavailable origin checks: rows affected: %w", err)
	}
	voteVerifyVerdicts.Add(string(OriginExpired), n)
	return nil
}

// reportVerifyBacklog publishes how many candidates in the window are still
// unchecked or retryable and the age of the oldest, and warns on each sweep
// while that vote is within min(verifyWarnMargin, Window/4) of leaving the
// window unchecked.
func (w *RecordWriter) reportVerifyBacklog(ctx context.Context, upOnly bool) error {
	var backlog int64
	var oldest float64
	if err := w.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(EXTRACT(EPOCH FROM now() - MIN(e.created_at)), 0)::float8
		FROM vote_events e
		WHERE NOT e.undone AND e.record_at_uri IS NULL
		  AND (e.origin_check IS NULL OR e.origin_check = 'unavailable')
		  AND e.created_at > now() - make_interval(secs => $1)
		  AND (NOT $2 OR e.direction = 'up')
		  AND `+hostedSubjectSQL+`
		  AND `+eligibleVoterSQL, w.window.Seconds(), upOnly).Scan(&backlog, &oldest); err != nil {
		return fmt.Errorf("votes: count origin check backlog: %w", err)
	}
	voteVerifyBacklog.Set(backlog)
	voteVerifyOldestPending.Set(int64(oldest))
	oldestAge := time.Duration(oldest * float64(time.Second))
	if backlog > 0 && oldestAge > w.window-min(verifyWarnMargin, w.window/4) {
		w.logger.Warn("origin verification is falling behind: the oldest unchecked vote will leave the window and stay aggregate",
			"backlog", backlog, "oldest_pending", oldestAge.Round(time.Second).String(), "window", w.window.String())
	}
	return nil
}
