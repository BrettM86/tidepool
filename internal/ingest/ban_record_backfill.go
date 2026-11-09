package ingest

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// BanRecordBackfillReport is what one ban-record back-fill run did (or, in a
// dry run, would do).
type BanRecordBackfillReport struct {
	DryRun bool `json:"dry_run"`
	// Published lists each ban whose record was written (dry run: would be).
	Published []BanRecordBackfillEntry `json:"published"`
	// AlreadyPublished counts bans in force whose record already stands,
	// whatever it says: a standing record was written by the live path (or an
	// earlier run) and is never overwritten.
	AlreadyPublished int `json:"already_published"`
	// Undecidable lists bridged bans the stored history cannot settle: a later
	// poisoned or unparseable ban-shaped event for the same community and
	// subject, or one still pending or recorded after the run read the
	// history, may have lifted or replaced them. They are not published.
	Undecidable []BanRecordBackfillUndecidable `json:"undecidable"`
	// SkippedUnminted counts the distinct (community, actor) pairs whose
	// recorded ban names a Lemmy user this bridge never minted.
	SkippedUnminted int `json:"skipped_unminted"`
	// Failed counts bans whose record could not be read or written; each is
	// logged and the run goes on.
	Failed int `json:"failed"`
}

// BanRecordBackfillEntry is one published ban.
type BanRecordBackfillEntry struct {
	Community string `json:"community"`
	Subject   string `json:"subject"`
}

// BanRecordBackfillUndecidable is one ban left unpublished, with the recorded
// event that makes it undecidable.
type BanRecordBackfillUndecidable struct {
	Community  string `json:"community"`
	Subject    string `json:"subject"`
	ActivityID string `json:"activity_id"`
}

// backfillBan is a ban the back-fill may publish.
type backfillBan struct {
	communityDID string
	subjectDID   string
	reason       string
	expiresAt    *time.Time
	// createdAt is when the deciding ban was received (a bridged person) or
	// first recorded (a native persona).
	createdAt time.Time
	// bridged is true for a ban rebuilt from the inbox history, false for a
	// native persona's community_bans row.
	bridged bool
}

// bridgedBanHistory is one (community, bridged person) pair's replayed state.
type bridgedBanHistory struct {
	communityDID string
	subjectDID   string
	// standing is the ban standing after the events replayed so far, or nil.
	standing *backfillBan
	// undecidableBy is the activity id of the event that made the standing ban
	// unknowable, or "" while it is known.
	undecidableBy string
	// pendingBy is the activity id of the first ban-shaped event for the pair
	// still waiting in the queue, or "": once it is processed it may lift or
	// replace whatever the replay ends on.
	pendingBy string
}

// BackfillBanRecords publishes a ban record for every community ban in force
// now that has none (or has one that differs), for bans accepted before
// community bans became ban records.
//
// Native personas come from the community_bans rows in force. Bridged persons
// hold no row, so their bans are rebuilt by replaying the recorded Block and
// Undo{Block} events in inbox id order under the live rules, each judged at
// its own received_at: a Block in force at receipt replaces the standing ban
// (even if it has lapsed since), a Block already over at receipt replaces
// nothing, and an Undo{Block} lifts the standing ban unless the stale-unban
// rule keeps it. The final standing ban is published only if it is in force
// now; a lapsed one publishes nothing and does not revive the ban it replaced.
//
// The route runs beside the ingest queue, so live moderation can land at any
// point of a run. A record is written only where none stands (a standing one
// is the live path's, or an earlier run's), and only create-only, so a record
// written live meanwhile fails the write instead of being overwritten. Right
// before each write the source is read again: a native ban must still be in
// force in community_bans, and a bridged pair must have no ban-shaped event
// above the highest inbox id the replay read (any status), else it is left
// undecidable. A second run publishes nothing. A dry run writes nothing.
func (r *BanReplayer) BackfillBanRecords(ctx context.Context, dryRun bool) (*BanRecordBackfillReport, error) {
	h := r.handler
	if h.bans == nil {
		return nil, fmt.Errorf("ingest: no community-ban store is wired, so native bans cannot be listed")
	}
	publisher, err := h.banRecordPublisher()
	if err != nil {
		return nil, err
	}
	report := &BanRecordBackfillReport{
		DryRun:      dryRun,
		Published:   []BanRecordBackfillEntry{},
		Undecidable: []BanRecordBackfillUndecidable{},
	}

	natives, err := h.bans.ListInForce(ctx)
	if err != nil {
		return nil, fmt.Errorf("ingest: list native bans in force: %w", err)
	}
	var candidates []backfillBan
	for _, ban := range natives {
		candidates = append(candidates, backfillBan{
			communityDID: ban.CommunityDID,
			subjectDID:   ban.SubjectDID,
			reason:       ban.Reason,
			expiresAt:    ban.ExpiresAt,
			createdAt:    ban.BannedAt,
		})
	}

	histories, lastEventID, err := r.replayBridgedBans(ctx, report)
	if err != nil {
		return nil, err
	}
	now := timeNow()
	for _, history := range histories {
		switch {
		case history.pendingBy != "":
			report.Undecidable = append(report.Undecidable, BanRecordBackfillUndecidable{
				Community: history.communityDID, Subject: history.subjectDID, ActivityID: history.pendingBy,
			})
		case history.undecidableBy != "":
			report.Undecidable = append(report.Undecidable, BanRecordBackfillUndecidable{
				Community: history.communityDID, Subject: history.subjectDID, ActivityID: history.undecidableBy,
			})
		case history.standing != nil && banInForce(history.standing.expiresAt, now):
			candidates = append(candidates, *history.standing)
		}
	}

	for _, ban := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("ingest: ban record back-fill stopped: %w", err)
		}
		outcome, activityID, err := r.publishBackfillBan(ctx, publisher, ban, lastEventID, dryRun)
		if err != nil {
			report.Failed++
			h.logger.Error("ban record back-fill failed a ban; continuing with the next",
				"community_did", ban.communityDID, "subject_did", ban.subjectDID, "error", err)
			continue
		}
		switch outcome {
		case backfillAlreadyPublished:
			report.AlreadyPublished++
		case backfillUndecidable:
			report.Undecidable = append(report.Undecidable, BanRecordBackfillUndecidable{
				Community: ban.communityDID, Subject: ban.subjectDID, ActivityID: activityID,
			})
		case backfillLifted:
			h.logger.Info("ban record back-fill skipped a native ban lifted since the run listed it",
				"community_did", ban.communityDID, "subject_did", ban.subjectDID)
		case backfillPublished:
			report.Published = append(report.Published, BanRecordBackfillEntry{
				Community: ban.communityDID, Subject: ban.subjectDID,
			})
			if !dryRun {
				h.logger.Info("ban record back-fill published a ban",
					"community_did", ban.communityDID, "subject_did", ban.subjectDID)
			}
		}
	}
	return report, nil
}

// backfillOutcome is what the back-fill did with one candidate ban.
type backfillOutcome int

const (
	// backfillPublished: the record was written (dry run: would be).
	backfillPublished backfillOutcome = iota
	// backfillAlreadyPublished: a record stands, or one was written live
	// between the read and the create-only write.
	backfillAlreadyPublished
	// backfillLifted: the native ban is no longer in force.
	backfillLifted
	// backfillUndecidable: a bridged pair has a ban-shaped event newer than
	// the history the replay read.
	backfillUndecidable
)

// publishBackfillBan writes ban's record where none stands, after reading the
// ban's source again. activityID names the newer event when the outcome is
// backfillUndecidable.
func (r *BanReplayer) publishBackfillBan(ctx context.Context, publisher BanRecordPublisher, ban backfillBan,
	lastEventID int64, dryRun bool,
) (outcome backfillOutcome, activityID string, err error) {
	standing, err := publisher.StandingBanRecord(ctx, ban.communityDID, ban.subjectDID)
	if err != nil {
		return 0, "", err
	}
	if standing != nil {
		return backfillAlreadyPublished, "", nil
	}
	if ban.bridged {
		activityID, err := r.newerBanEvent(ctx, ban, lastEventID)
		if err != nil {
			return 0, "", err
		}
		if activityID != "" {
			return backfillUndecidable, activityID, nil
		}
	} else {
		inForce, err := r.handler.bans.Standing(ctx, ban.communityDID, ban.subjectDID)
		if err != nil {
			return 0, "", fmt.Errorf("ingest: re-read native ban: %w", err)
		}
		if !inForce {
			return backfillLifted, "", nil
		}
	}
	if dryRun {
		return backfillPublished, "", nil
	}
	if _, err := publisher.WriteBanRecordIfUnchanged(ctx, ban.communityDID, ban.subjectDID, ban.reason,
		ban.expiresAt, ban.createdAt, ""); err != nil {
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			// The live path wrote a record after the read; it stands.
			return backfillAlreadyPublished, "", nil
		}
		return 0, "", err
	}
	return backfillPublished, "", nil
}

// newerBanEvent returns the activity id of the first ban-shaped inbox event,
// of any status, above lastEventID that names ban's community and subject, or
// "" when there is none.
func (r *BanReplayer) newerBanEvent(ctx context.Context, ban backfillBan, lastEventID int64) (string, error) {
	h := r.handler
	events, err := r.events.ListBanEventsAfter(ctx, lastEventID)
	if err != nil {
		return "", fmt.Errorf("ingest: list ban events after %d: %w", lastEventID, err)
	}
	for _, event := range events {
		recorded, err := h.recordedCommunityBan(ctx, event)
		if err != nil {
			return "", fmt.Errorf("ingest: decide ban event %s: %w", event.ActivityID, err)
		}
		if recorded == nil || recorded.announcer.DID != ban.communityDID {
			continue
		}
		actor, kind, err := h.banSubject(ctx, recorded.subjectAPID)
		if err != nil {
			return "", err
		}
		if kind == banSubjectMinted && actor.DID == ban.subjectDID {
			return event.ActivityID, nil
		}
	}
	return "", nil
}

// banInForce reports whether a ban expiring at expiresAt (nil: permanent) is in
// force at when.
func banInForce(expiresAt *time.Time, when time.Time) bool {
	return expiresAt == nil || expiresAt.After(when)
}

// replayBridgedBans replays the community Block and Undo{Block} events naming
// minted bridged persons, in inbox id order, and returns each pair's final
// state in the order the pairs first appear, with the highest inbox id read.
// A ban-shaped event still pending marks its pair (see pendingBy).
func (r *BanReplayer) replayBridgedBans(ctx context.Context, report *BanRecordBackfillReport) ([]*bridgedBanHistory, int64, error) {
	h := r.handler
	events, err := r.events.ListBanEventsAfter(ctx, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("ingest: list recorded bans: %w", err)
	}
	histories := map[string]*bridgedBanHistory{}
	var order []*bridgedBanHistory
	unminted := map[string]bool{}
	var lastEventID int64
	for _, event := range events {
		lastEventID = max(lastEventID, event.ID)
		if err := ctx.Err(); err != nil {
			return nil, 0, fmt.Errorf("ingest: ban record back-fill stopped at %s: %w", event.ActivityID, err)
		}
		recorded, err := h.recordedCommunityBan(ctx, event)
		if err != nil {
			return nil, 0, fmt.Errorf("ingest: decide recorded ban %s: %w", event.ActivityID, err)
		}
		if recorded == nil {
			continue
		}
		actor, kind, err := h.banSubject(ctx, recorded.subjectAPID)
		if err != nil {
			return nil, 0, err
		}
		pairKey := recorded.announcer.DID + " " + recorded.subjectAPID
		switch kind {
		case banSubjectNotPerson:
			continue
		case banSubjectUnminted:
			if !unminted[pairKey] {
				unminted[pairKey] = true
				report.SkippedUnminted++
			}
			continue
		}
		history := histories[pairKey]
		if history == nil {
			history = &bridgedBanHistory{communityDID: recorded.announcer.DID, subjectDID: actor.DID}
			histories[pairKey] = history
			order = append(order, history)
		}
		if event.ProcessedAt == nil && event.FailedAt == nil {
			if history.pendingBy == "" {
				history.pendingBy = event.ActivityID
			}
			continue
		}
		history.apply(event, recorded)
	}
	return order, lastEventID, nil
}

// apply replays one recorded event onto the pair's state, judged at the
// event's received_at.
func (history *bridgedBanHistory) apply(event *store.InboxEvent, recorded *recordedBan) {
	if event.FailedAt != nil || recorded.block == nil {
		// Poisoned, or a payload that does not parse: whether it banned, lifted
		// or replaced anything cannot be read from what was stored.
		history.undecidableBy = event.ActivityID
		return
	}
	expiry := recorded.block.BanExpiry()
	if expiry != nil && !expiry.Valid {
		history.undecidableBy = event.ActivityID
		return
	}
	var expiresAt *time.Time
	if expiry != nil {
		when := expiry.Time
		expiresAt = &when
	}
	if !recorded.undo {
		if !banInForce(expiresAt, event.ReceivedAt) {
			// Over before it arrived: it replaces nothing (see applyBan).
			return
		}
		// A Block in force replaces whatever stood, a ban nobody can read
		// included.
		history.standing = &backfillBan{
			communityDID: history.communityDID,
			subjectDID:   history.subjectDID,
			reason:       recorded.block.Summary,
			expiresAt:    expiresAt,
			createdAt:    event.ReceivedAt,
			bridged:      true,
		}
		history.undecidableBy = ""
		return
	}
	if expiresAt == nil {
		// An Undo naming no expiry lifts whatever stands.
		history.standing = nil
		history.undecidableBy = ""
		return
	}
	if history.undecidableBy != "" {
		// Whether this Undo is stale depends on the ban nobody can read.
		return
	}
	if standing := history.standing; standing != nil && banInForce(standing.expiresAt, event.ReceivedAt) &&
		(standing.expiresAt == nil || standing.expiresAt.After(*expiresAt)) {
		return // stale: the standing ban outlives the one this Undo reverses
	}
	history.standing = nil
}

// recordedBan is a recorded announced community Block or Undo{Block} that
// passed the live gates, naming a subject that is not one of our personas.
type recordedBan struct {
	announcer   *store.Community
	subjectAPID string
	undo        bool
	// block is the Block (for an Undo, the undone Block), or nil when the
	// payload did not parse and only its community and subject could be read.
	block *ap.Object
}

// recordedCommunityBan reads one recorded event as an announced community ban
// or unban of a subject that is not one of our personas, or returns nil.
func (h *Handler) recordedCommunityBan(ctx context.Context, event *store.InboxEvent) (*recordedBan, error) {
	if len(event.Payload) == 0 || event.ActorID == "" {
		return nil, nil
	}
	activity, err := ap.ParseObject(event.Payload)
	if err != nil {
		target, subjectAPID, undo, ok := looseRecordedBlock(event.Payload)
		if !ok {
			return nil, nil //nolint:nilerr // a payload naming no Block is not a ban
		}
		announcer, err := h.recordedCommunityBlockAnnouncer(ctx, event, nil, target, subjectAPID)
		if announcer == nil || err != nil {
			return nil, err
		}
		return &recordedBan{announcer: announcer, subjectAPID: subjectAPID, undo: undo}, nil
	}
	if activity.Type != ap.TypeAnnounce || activity.Object == nil {
		return nil, nil
	}
	block, undo := activity.Object, false
	if block.Type == ap.TypeUndo {
		block, undo = block.Object, true
	}
	if block == nil || block.Type != ap.TypeBlock {
		return nil, nil
	}
	subjectAPID := refID(block.Object)
	announcer, err := h.recordedCommunityBlockAnnouncer(ctx, event, activity, refID(block.Target), subjectAPID)
	if announcer == nil || err != nil {
		return nil, err
	}
	return &recordedBan{announcer: announcer, subjectAPID: subjectAPID, undo: undo, block: block}, nil
}

// looseRecordedBlock reads the target and subject out of a stored Announce of
// a Block (or of an Undo carrying one) whose payload does not parse as an
// activity, e.g. one with a typed field of the wrong JSON type. ok is false
// when the payload is not that shape at all.
func looseRecordedBlock(payload []byte) (target, subjectAPID string, undo, ok bool) {
	type looseObject struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
		Target json.RawMessage `json:"target"`
	}
	var envelope looseObject
	if json.Unmarshal(payload, &envelope) != nil || envelope.Type != ap.TypeAnnounce {
		return "", "", false, false
	}
	var block looseObject
	if json.Unmarshal(envelope.Object, &block) != nil {
		return "", "", false, false
	}
	if block.Type == ap.TypeUndo {
		undo = true
		inner := block.Object
		block = looseObject{}
		if json.Unmarshal(inner, &block) != nil {
			return "", "", false, false
		}
	}
	if block.Type != ap.TypeBlock {
		return "", "", false, false
	}
	target, subjectAPID = looseID(block.Target), looseID(block.Object)
	return target, subjectAPID, undo, target != "" && subjectAPID != ""
}

// looseID is the id a JSON reference names: a bare string, or an object's id.
func looseID(raw json.RawMessage) string {
	var id string
	if json.Unmarshal(raw, &id) == nil {
		return id
	}
	var object struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &object) == nil {
		return object.ID
	}
	return ""
}

// handleBackfillBanRecords serves POST /admin/moderation/backfill-ban-records
// {"dry_run": bool}: the one-time publication of ban records for community
// bans accepted before bans became records. dry_run is required, as for the
// ban replay, and the write deadline is extended as the replay's is.
func (a *Admin) handleBackfillBanRecords(w http.ResponseWriter, r *http.Request) {
	if a.banReplayer == nil {
		http.Error(w, "ban record back-fill is not configured", http.StatusNotImplemented)
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(banReplayWriteTimeout)); err != nil {
		a.logger.WarnContext(r.Context(), "extend ban record back-fill write deadline", "error", err)
	}
	var req struct {
		DryRun *bool `json:"dry_run"`
	}
	if err := decodeJSONBody(r, &req); err != nil || req.DryRun == nil {
		http.Error(w, `body must be {"dry_run":true} or {"dry_run":false}`, http.StatusBadRequest)
		return
	}
	report, err := a.banReplayer.BackfillBanRecords(r.Context(), *req.DryRun)
	if err != nil {
		a.logger.Error("ban record back-fill failed", "dry_run", *req.DryRun, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	a.logger.Info("ban record back-fill finished",
		"dry_run", report.DryRun, "published", len(report.Published),
		"already_published", report.AlreadyPublished, "undecidable", len(report.Undecidable),
		"skipped_unminted", report.SkippedUnminted, "failed", report.Failed)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		a.logger.Warn("write ban record back-fill report", "dry_run", report.DryRun, "error", err)
	}
}
