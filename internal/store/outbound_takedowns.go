package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// TakedownSource is one member of an outbound object's takedown source set.
type TakedownSource struct{ Kind, Key string }

// Takedown source kinds.
const (
	TakedownSourceRecordLabel    = "record-label"
	TakedownSourceAccountLabel   = "account-label"
	TakedownSourceAccountStatus  = "account-status"
	TakedownSourceCommunityLabel = "community-label"
)

// TakedownTransition is what a source-set change did to the set.
type TakedownTransition int

// Takedown transitions.
const (
	TakedownUnchanged TakedownTransition = iota
	TakedownBecameRemoved
	TakedownBecameClear
)

// OutboundTakedowns is the per-object takedown source set.
type OutboundTakedowns interface {
	AddSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src TakedownSource) (TakedownTransition, error)
	RemoveSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src TakedownSource) (TakedownTransition, error)
	IsRemoved(ctx context.Context, atURI string) (bool, error)
	// IsRemovedTx is IsRemoved read in tx.
	IsRemovedTx(ctx context.Context, tx *sql.Tx, atURI string) (bool, error)
	// EpisodeTx reads atURI's current takedown episode in tx: its number and
	// the takedown Delete sent in it, if any. An object with no takedown row
	// has episode 0 and no Delete.
	EpisodeTx(ctx context.Context, tx *sql.Tx, atURI string) (TakedownEpisode, error)
	// RecordDeleteTx records activityID as the takedown Delete of atURI's
	// current episode.
	RecordDeleteTx(ctx context.Context, tx *sql.Tx, atURI, activityID string) error
	// IsParentRemoved reports whether a reply to atURI must wait: the parent's
	// takedown set is non-empty, or a `!takedown` record label stands on it.
	// The label covers a bridged parent, which has no takedown set of its own.
	IsParentRemoved(ctx context.Context, atURI string) (bool, error)
	// IsParentRemovedTx is IsParentRemoved read in tx.
	IsParentRemovedTx(ctx context.Context, tx *sql.Tx, atURI string) (bool, error)
	// WasEverRemoved reports whether atURI has had at least one takedown
	// episode.
	WasEverRemoved(ctx context.Context, atURI string) (bool, error)
	// CommunitySubjects reads the DIDs of the bridged communities that native
	// work on communityAPID's ordering key, replying to parentATURI (either
	// may be empty), is bound to: the community with that AP id, and the
	// community of a bridged parent (ap_objects.community_did). Each is a
	// takedown subject: a hold that reads a community's removal takes its
	// lock, as does the community's release.
	CommunitySubjects(ctx context.Context, communityAPID, parentATURI string) ([]string, error)
	// CommunitySubjectsTx is CommunitySubjects read in tx.
	CommunitySubjectsTx(ctx context.Context, tx *sql.Tx, communityAPID, parentATURI string) ([]string, error)
	// RemovedCommunity returns the first of communityDIDs (in sorted order)
	// whose latest `!takedown` on its bare DID stands, "" when none does.
	RemovedCommunity(ctx context.Context, communityDIDs []string) (string, error)
	// RemovedCommunityTx is RemovedCommunity read in tx.
	RemovedCommunityTx(ctx context.Context, tx *sql.Tx, communityDIDs []string) (string, error)
}

// takedownLabelValue is the record label that takes a subject down. It is
// materialize.LabelTakedown, spelled here because store sits below materialize.
const takedownLabelValue = "!takedown"

// parentRemovedPredicate is the SQL condition that a reply to the at-uri in
// the given expression is blocked: the parent's takedown set is non-empty, its
// latest `!takedown` record label stands (not a negation, not lapsed), the
// parent is a bridged person's record and the latest account `!takedown` on
// that person's bare DID stands, or the parent is bound to a bridged community
// (ap_objects.community_did) whose latest `!takedown` on its bare DID stands.
// The community test reads the label alone, not the ingest side's removal
// record: in a negation's tx the appliers run in registration order, and the
// label is the one fact every one of them already reads as negated. A parent
// row with no stored community (pre-016 postv2 and comment rows) is bound
// from its record by the materializer's label applier in the label's own
// transaction, so the column is set wherever the label is visible; a parent
// whose community cannot be derived is counted there and is not blocked here.
func parentRemovedPredicate(uri string) string {
	return `(EXISTS (SELECT 1 FROM outbound_takedown_sources s WHERE s.at_uri = ` + uri + `)
		OR ` + standingTakedownLabel(uri) + `
		OR EXISTS (SELECT 1 FROM bridged_actors b
		           WHERE b.did = substring((` + uri + `)::text FROM '^at://([^/]+)/')
		             AND b.actor_type = '` + string(ActorTypePerson) + `'
		             AND ` + standingTakedownLabel("b.did") + `)
		OR EXISTS (SELECT 1 FROM ap_objects o
		           WHERE o.at_uri = ` + uri + ` AND o.community_did IS NOT NULL
		             AND ` + standingTakedownLabel("o.community_did") + `))`
}

// standingTakedownLabel is the SQL condition that the latest `!takedown`
// label on the subject in the given expression stands.
func standingTakedownLabel(subject string) string {
	return `COALESCE((SELECT ` + StandingLabelCondition("l") + ` FROM coves_labels l
		             WHERE l.val = '` + takedownLabelValue + `' AND l.uri = ` + subject + `
		             ORDER BY l.reset_epoch DESC, l.seq DESC LIMIT 1), false)`
}

// AccountSubjectOf is the bare DID that authors the record at-uri atURI, ""
// for anything else. It is the takedown subject a bridged account's label
// holds and releases replies on: every hold on a parent takes it with the
// parent, and the account's release (ReleaseAccountHeldTx) takes it, so a
// reply held on any record of the account either commits before the
// release reads the held rows or rechecks after the release commits.
func AccountSubjectOf(atURI string) string {
	rest, ok := strings.CutPrefix(atURI, "at://")
	if !ok {
		return ""
	}
	authority, _, found := strings.Cut(rest, "/")
	if !found || !strings.HasPrefix(authority, "did:") {
		return ""
	}
	return authority
}

// takedownSubjectLockNamespace scopes the takedown-subject advisory locks.
// The two-argument form keeps them apart from every single-key advisory lock
// (the test harness's, the repo commit lock) and from the other two-key
// namespaces ("ac" account claims, "bl" blob store).
const takedownSubjectLockNamespace = 0x7464 // "td"

// LockTakedownSubjectsTx takes, in tx, the transaction-scoped advisory lock of
// each takedown subject (an at-uri a held delivery can wait on), in the sorted
// order of the lock keys (the subjects' hashtext) with duplicate keys and
// empty subjects dropped. Every hold takes it on the subject it checks, then
// rechecks that subject's blocker in a later statement of the same tx; every
// release (ReleaseHeldTx, ReleaseConsentHeldTx, ReleaseAccountHeldTx) takes
// it, before its read, on every subject whose blocker it reads; and a
// takedown (Takedowns.AddSourceTx) takes it on its object before it enqueues
// the Delete. So a release either commits before the
// hold's recheck, which then reads it, or runs after the hold commits and
// finds the held row: a hold never lands on a subject whose release already
// ran. And a consent release either reads a takedown of its object, or
// commits before that takedown enqueues its Delete.
//
// Lock order, so no transaction waits in a cycle. Each transaction takes the
// steps it needs in this order:
//
//  1. the label cursor row (a label's tx: labels Commit and a lapse);
//  2. the account advisory lock (LockAccountTx: the account label applier,
//     the #account handler, and every account-wide source change);
//  3. the persona's ap_actors row (a pause or consent write updates it; an
//     account-wide source change takes it FOR NO KEY UPDATE; the worker's
//     consent and pause holds share-lock it);
//  4. outbound_objects rows, then outbound_takedowns rows: one object's for a
//     record takedown or restore, every object of the actor (each in at-uri
//     order) for an account-wide source change;
//  5. this lock, in one sorted call;
//  6. delivery rows.
//
// The holders and releasers:
//   - A native takedown (Takedowns.AddSourceTx) takes the object's
//     outbound_objects row, then its outbound_takedowns row, then this lock,
//     then the object's pending delivery rows (the hold) and its new Delete.
//   - A native restore (Takedowns.RemoveSourceTx) takes the object's
//     outbound_objects row, then its outbound_takedowns row, then enqueues
//     the Undo (new rows only), then this lock (ReleaseHeldTx: the object
//     and its held rows' own objects, parents and parents' accounts in one
//     call per attempt; an attempt whose held rows outgrew the call rolls
//     back to a savepoint, dropping its locks), then the subject's held
//     delivery rows. A record label takes the label cursor row first; a
//     bridged record's negation then takes only this lock and the held rows.
//   - An account-wide source change (Takedowns.Add/RemoveAccountSourceTx,
//     from the account label applier and the #account handler) takes steps
//     2 to 5 in lockAccountObjectsTx, with every subject its per-object changes
//     and the consent release after them read (ConsentHeldSubjectsTx) in its
//     one call; the per-object changes and the consent release then retake
//     subjects it already holds. The label applier writes the pause source
//     (step 3) before it, and the #account handler the status pause source.
//   - A community-wide source change (Takedowns.Add/RemoveCommunitySourceTx,
//     from the takedown label applier on a bridged community's DID) takes the
//     community's outbound_objects rows, then their outbound_takedowns rows,
//     then, in one call per attempt (an attempt whose held rows outgrew the
//     call rolls back to a savepoint), the objects, the community DID, the
//     community's records replies are held on, and every held row's object,
//     parent and parent's account; then the delivery rows. An enqueue and the
//     worker's hold take the DIDs of the communities their work is bound to
//     (CommunitySubjects) with their other subjects.
//   - A bridged account's release (ReleaseAccountHeldTx, from the account
//     label applier) takes the label cursor row and the account lock, then
//     the account, its held rows' parents and objects in one call per attempt
//     (an attempt whose held rows outgrew the call rolls back to a savepoint,
//     dropping its locks), then the held rows.
//   - An enqueue tx has already written its own object's outbound_objects row
//     (the consumer upserts or tombstones it before EnqueueActivity), so a
//     takedown or restore of that object queues on the row before it reaches
//     this lock. It takes no row lock on a parent's outbound_objects or
//     outbound_takedowns row, and after this lock it writes only its own new
//     and pending delivery rows, which no release locks (a release locks only
//     held rows).
//   - The worker's takedown hold takes this lock first in its own tx, and
//     after it writes only the claimed, pending delivery row.
//   - The worker's consent hold (holdOnConsent) share-locks the author's
//     ap_actors row, then takes this lock, then delivery rows; its pause hold
//     (holdOnPause) share-locks the row, then writes the claimed delivery row
//     and takes no subject lock. A consent release (ReleaseConsentHeldTx:
//     the consumer's unpause and opt-in, the account label applier's lift,
//     the #account handler) runs after its tx's ap_actors write (or
//     share-locks the row), then takes this lock, then the held rows. Each
//     takes the actor row before this lock.
//   - Holds and releases take every subject in one call (a takedown hold and
//     an enqueue their object, its parent and the parent's account,
//     AccountSubjectOf; a consent hold its object; the consent release its
//     rows' objects, their parents and the parents' accounts; a takedown
//     release its subject and its held rows' objects, parents and parents'
//     accounts); a takedown takes one.
//
// One exception: in an account-wide tx, the consent release can find a
// consent-held row committed after lockAccountObjectsTx's subject call (the
// actor row it holds shuts out the worker's holds, so only the enqueuer's
// holdIfRestoreHeldOnConsent adds one, for a brand-new object). That row's
// subjects are then taken in a second call, out of the sorted order, which
// can close a cycle with another multi-subject tx. Postgres detects that
// deadlock (40P01) and aborts one tx; both callers of an account-wide change
// (the label consumer replays from its cursor, the #account event is
// retryable) roll back and retry, so no hold or release is lost. The same
// retried 40P01 covers the precollection window: a reply of another author
// held on one of the actor's objects after lockAccountObjectsTx read the held
// rows but before its subject call has its subjects taken later, out of the
// sorted order, by the per-object restore.
//
// So a takedown hold waits only on a release's or a takedown's subject lock,
// a consent or pause hold also on the actor row a pause, consent or
// account-wide write holds, and no transaction a hold waits on is itself
// waiting on that hold.
func LockTakedownSubjectsTx(ctx context.Context, tx *sql.Tx, subjects ...string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	subjects = slices.DeleteFunc(slices.Clone(subjects), func(subject string) bool { return subject == "" })
	if len(subjects) == 0 {
		return nil
	}
	// The order is that of the keys actually locked, not of the subjects: two
	// subjects that hash alike are one lock, taken once.
	rows, err := tx.QueryContext(ctx,
		`SELECT DISTINCT hashtext(subject) FROM unnest($1::text[]) AS subject ORDER BY 1`, pq.Array(subjects))
	if err != nil {
		return fmt.Errorf("hash takedown subjects %q: %w", subjects, err)
	}
	var keys []int32
	for rows.Next() {
		var key int32
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan takedown subject key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("hash takedown subjects %q: %w", subjects, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("hash takedown subjects %q: %w", subjects, err)
	}
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`,
			takedownSubjectLockNamespace, key); err != nil {
			return fmt.Errorf("lock takedown subject key %d of %q: %w", key, subjects, err)
		}
	}
	return nil
}

// LockVoteSubjectsTx takes, in tx and in one sorted call, the takedown-subject
// locks the enqueue of a Like/Dislike on subjectATURI into communityAPID takes
// (Enqueuer.EnqueueActivity): the subject, its account, and its communities.
// A vote re-cast takes them before it cancels the replaced vote's held rows: a
// release holds these locks while it row-locks the held rows, so a held row
// locked first would close a cycle with it (40P01). The enqueue retakes the
// same locks, which is a no-op within one tx.
func LockVoteSubjectsTx(ctx context.Context, tx *sql.Tx, communityAPID, subjectATURI string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	communities, err := communitySubjects(ctx, tx, communityAPID, subjectATURI)
	if err != nil {
		return err
	}
	subjects := append([]string{subjectATURI, AccountSubjectOf(subjectATURI)}, communities...)
	return LockTakedownSubjectsTx(ctx, tx, subjects...)
}

// TakedownEpisode is one removal episode of an object: Number counts the
// object's removals, and the Delete sent in it, when one was, is
// DeleteActivityID with its stored payload.
type TakedownEpisode struct {
	Number           int
	DeleteActivityID string
	DeletePayload    []byte
}

// NewOutboundTakedowns builds the Postgres OutboundTakedowns.
func NewOutboundTakedowns(db *sql.DB) OutboundTakedowns {
	return &postgresOutboundTakedowns{db: db}
}

type postgresOutboundTakedowns struct {
	db *sql.DB
}

func (r *postgresOutboundTakedowns) AddSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src TakedownSource) (TakedownTransition, error) {
	if err := checkTakedownArgs(tx, atURI, src); err != nil {
		return TakedownUnchanged, err
	}
	// The upsert takes the object's outbound_takedowns row lock, so concurrent
	// changes to one object's set serialize and exactly one of them sees the
	// set go from empty to non-empty.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbound_takedowns (at_uri) VALUES ($1)
		ON CONFLICT (at_uri) DO UPDATE SET updated_at = now()`, atURI); err != nil {
		return TakedownUnchanged, fmt.Errorf("lock takedown of %q: %w", atURI, err)
	}
	wasEmpty, err := takedownSetEmpty(ctx, tx, atURI)
	if err != nil {
		return TakedownUnchanged, err
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO outbound_takedown_sources (at_uri, source_kind, source_key)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, atURI, src.Kind, src.Key)
	if err != nil {
		return TakedownUnchanged, fmt.Errorf("add takedown source %s %q to %q: %w", src.Kind, src.Key, atURI, err)
	}
	added, err := result.RowsAffected()
	if err != nil {
		return TakedownUnchanged, fmt.Errorf("add takedown source to %q: rows affected: %w", atURI, err)
	}
	if added == 0 || !wasEmpty {
		return TakedownUnchanged, nil
	}
	// A new removal episode: no Delete has been sent in it yet.
	if _, err := tx.ExecContext(ctx, `
		UPDATE outbound_takedowns
		SET episode = episode + 1, delete_activity_id = NULL, updated_at = now()
		WHERE at_uri = $1`, atURI); err != nil {
		return TakedownUnchanged, fmt.Errorf("start takedown episode of %q: %w", atURI, err)
	}
	return TakedownBecameRemoved, nil
}

func (r *postgresOutboundTakedowns) RemoveSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src TakedownSource) (TakedownTransition, error) {
	if err := checkTakedownArgs(tx, atURI, src); err != nil {
		return TakedownUnchanged, err
	}
	// Every add writes the outbound_takedowns row first, so an object with no
	// row has no sources to remove.
	var locked string
	err := tx.QueryRowContext(ctx,
		`SELECT at_uri FROM outbound_takedowns WHERE at_uri = $1 FOR UPDATE`, atURI).Scan(&locked)
	if stderrors.Is(err, sql.ErrNoRows) {
		return TakedownUnchanged, nil
	}
	if err != nil {
		return TakedownUnchanged, fmt.Errorf("lock takedown of %q: %w", atURI, err)
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM outbound_takedown_sources
		WHERE at_uri = $1 AND source_kind = $2 AND source_key = $3`, atURI, src.Kind, src.Key)
	if err != nil {
		return TakedownUnchanged, fmt.Errorf("remove takedown source %s %q from %q: %w", src.Kind, src.Key, atURI, err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return TakedownUnchanged, fmt.Errorf("remove takedown source from %q: rows affected: %w", atURI, err)
	}
	if removed == 0 {
		return TakedownUnchanged, nil
	}
	empty, err := takedownSetEmpty(ctx, tx, atURI)
	if err != nil {
		return TakedownUnchanged, err
	}
	if !empty {
		return TakedownUnchanged, nil
	}
	return TakedownBecameClear, nil
}

func (r *postgresOutboundTakedowns) IsRemoved(ctx context.Context, atURI string) (bool, error) {
	empty, err := takedownSetEmpty(ctx, r.db, atURI)
	if err != nil {
		return false, err
	}
	return !empty, nil
}

func (r *postgresOutboundTakedowns) IsRemovedTx(ctx context.Context, tx *sql.Tx, atURI string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	empty, err := takedownSetEmpty(ctx, tx, atURI)
	if err != nil {
		return false, err
	}
	return !empty, nil
}

func (r *postgresOutboundTakedowns) IsParentRemoved(ctx context.Context, atURI string) (bool, error) {
	return parentRemoved(ctx, r.db, atURI)
}

func (r *postgresOutboundTakedowns) IsParentRemovedTx(ctx context.Context, tx *sql.Tx, atURI string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	return parentRemoved(ctx, tx, atURI)
}

func parentRemoved(ctx context.Context, ex execer, atURI string) (bool, error) {
	var removed bool
	if err := ex.QueryRowContext(ctx, `SELECT `+parentRemovedPredicate("$1"), atURI).Scan(&removed); err != nil {
		return false, fmt.Errorf("read whether parent %q is removed: %w", atURI, err)
	}
	return removed, nil
}

func (r *postgresOutboundTakedowns) CommunitySubjects(ctx context.Context, communityAPID, parentATURI string) ([]string, error) {
	return communitySubjects(ctx, r.db, communityAPID, parentATURI)
}

func (r *postgresOutboundTakedowns) CommunitySubjectsTx(ctx context.Context, tx *sql.Tx, communityAPID, parentATURI string) ([]string, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return communitySubjects(ctx, tx, communityAPID, parentATURI)
}

func communitySubjects(ctx context.Context, ex execer, communityAPID, parentATURI string) ([]string, error) {
	var dids pq.StringArray
	if err := ex.QueryRowContext(ctx, `
		SELECT COALESCE(array_agg(DISTINCT s.did ORDER BY s.did), '{}')
		FROM (
			SELECT did FROM communities WHERE ap_group_id = $1
			UNION
			SELECT community_did FROM ap_objects
			WHERE at_uri = $2 AND community_did IS NOT NULL AND community_did <> ''
		) s`, communityAPID, parentATURI).Scan(&dids); err != nil {
		return nil, fmt.Errorf("read communities of %q replying to %q: %w", communityAPID, parentATURI, err)
	}
	return dids, nil
}

func (r *postgresOutboundTakedowns) RemovedCommunity(ctx context.Context, communityDIDs []string) (string, error) {
	return removedCommunity(ctx, r.db, communityDIDs)
}

func (r *postgresOutboundTakedowns) RemovedCommunityTx(ctx context.Context, tx *sql.Tx, communityDIDs []string) (string, error) {
	if tx == nil {
		return "", errors.NewValidationError("tx", "must not be nil")
	}
	return removedCommunity(ctx, tx, communityDIDs)
}

func removedCommunity(ctx context.Context, ex execer, communityDIDs []string) (string, error) {
	if len(communityDIDs) == 0 {
		return "", nil
	}
	var did string
	err := ex.QueryRowContext(ctx, `
		SELECT c.did FROM unnest($1::text[]) AS c(did)
		WHERE `+standingTakedownLabel("c.did")+`
		ORDER BY c.did LIMIT 1`, pq.Array(communityDIDs)).Scan(&did)
	if stderrors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read removal of communities %q: %w", communityDIDs, err)
	}
	return did, nil
}

func (r *postgresOutboundTakedowns) WasEverRemoved(ctx context.Context, atURI string) (bool, error) {
	var removed bool
	if err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM outbound_takedowns WHERE at_uri = $1 AND episode > 0)`, atURI).
		Scan(&removed); err != nil {
		return false, fmt.Errorf("read takedown episodes of %q: %w", atURI, err)
	}
	return removed, nil
}

func (r *postgresOutboundTakedowns) EpisodeTx(ctx context.Context, tx *sql.Tx, atURI string) (TakedownEpisode, error) {
	if tx == nil {
		return TakedownEpisode{}, errors.NewValidationError("tx", "must not be nil")
	}
	var episode TakedownEpisode
	var deleteID sql.NullString
	var payload []byte
	err := tx.QueryRowContext(ctx, `
		SELECT t.episode, t.delete_activity_id, a.payload
		FROM outbound_takedowns t
		LEFT JOIN outbound_activities a ON a.activity_id = t.delete_activity_id
		WHERE t.at_uri = $1`, atURI).Scan(&episode.Number, &deleteID, &payload)
	if stderrors.Is(err, sql.ErrNoRows) {
		return TakedownEpisode{}, nil
	}
	if err != nil {
		return TakedownEpisode{}, fmt.Errorf("read takedown episode of %q: %w", atURI, err)
	}
	if deleteID.Valid {
		if payload == nil {
			return TakedownEpisode{}, fmt.Errorf("takedown of %q records Delete %q with no stored activity",
				atURI, deleteID.String)
		}
		episode.DeleteActivityID, episode.DeletePayload = deleteID.String, payload
	}
	return episode, nil
}

func (r *postgresOutboundTakedowns) RecordDeleteTx(ctx context.Context, tx *sql.Tx, atURI, activityID string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if activityID == "" {
		return errors.NewValidationError("activity_id", "must not be empty")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE outbound_takedowns SET delete_activity_id = $2, updated_at = now()
		WHERE at_uri = $1`, atURI, activityID)
	if err != nil {
		return fmt.Errorf("record takedown Delete of %q: %w", atURI, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record takedown Delete of %q: rows affected: %w", atURI, err)
	}
	if n == 0 {
		return errors.NewNotFoundError("outbound_takedown", atURI)
	}
	return nil
}

// takedownSetEmpty reports whether atURI's takedown source set is empty.
func takedownSetEmpty(ctx context.Context, ex execer, atURI string) (bool, error) {
	var hasSource bool
	if err := ex.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM outbound_takedown_sources WHERE at_uri = $1)`, atURI).Scan(&hasSource); err != nil {
		return false, fmt.Errorf("read takedown sources of %q: %w", atURI, err)
	}
	return !hasSource, nil
}

func checkTakedownArgs(tx *sql.Tx, atURI string, src TakedownSource) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if atURI == "" {
		return errors.NewValidationError("at_uri", "must not be empty")
	}
	switch src.Kind {
	case TakedownSourceRecordLabel, TakedownSourceAccountLabel, TakedownSourceAccountStatus, TakedownSourceCommunityLabel:
	default:
		return errors.NewValidationError("source_kind", "unknown takedown source kind "+src.Kind)
	}
	if src.Key == "" {
		return errors.NewValidationError("source_key", "must not be empty")
	}
	return nil
}
