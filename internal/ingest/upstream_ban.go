package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// BannedContentRemover takes a BRIDGED Lemmy author's content down when an
// upstream ban carries `removeData: true`. *materialize.Materializer implements
// it, and the dispatcher reads it off the Materializer it already drives, for
// AuthorModerator's reason: only the materializer owns the record deletes, the
// community removal records and the repo walks the blob check needs.
//
// Selection and removal are separate so the operator replay can run the first
// alone as a dry run: what it lists is exactly what the second would act on.
type BannedContentRemover interface {
	// SelectBannedContent lists the author's live posts and comments published
	// strictly before `before`, in every community when communityDID is empty
	// or in that one community.
	SelectBannedContent(ctx context.Context, authorDID, communityDID string, before time.Time) ([]*store.APObjectMapping, error)
	// RemoveBannedContent deletes the selection (a site ban), or under a
	// community ban withdraws each postv2 with an author-banned removal and
	// deletes the rest (see materialize.BanWithdraws). communityDID only
	// selects the mode — each postv2's community is resolved from the post
	// itself — so it must be the communityDID SelectBannedContent was given.
	RemoveBannedContent(ctx context.Context, mappings []*store.APObjectMapping, communityDID, reason string) error
}

// Upstream ban scopes, as the replay reports them.
const (
	upstreamBanScopeSite      = "site"
	upstreamBanScopeCommunity = "community"
)

// upstreamBan is an upstream ban DECIDED to remove a bridged author's content:
// whose content, where, and published before when. It is what both the live
// dispatch and the operator replay act on; the replay mirrors the live gates
// in front of the decision rather than sharing them (see decideRecordedBan).
type upstreamBan struct {
	// activityID is the inbox event's activity id: the Announce for an
	// announced ban, the Block itself for a direct one.
	activityID string
	// blockID is the Block's own id.
	blockID     string
	subjectAPID string
	authorDID   string
	// community is the banning community, or nil for a site ban.
	community *store.Community
	// cutoff is the ban time: content published at or after it stays.
	cutoff time.Time
	// reason is the moderator's own words, written into any removal record.
	reason string
}

func (b *upstreamBan) scope() string {
	if b.community == nil {
		return upstreamBanScopeSite
	}
	return upstreamBanScopeCommunity
}

// communityDID is the scope the selection is narrowed to; empty for a site ban.
func (b *upstreamBan) communityDID() string {
	if b.community == nil {
		return ""
	}
	return b.community.DID
}

// communityAPID is the banning community's AP id; empty for a site ban.
func (b *upstreamBan) communityAPID() string {
	if b.community == nil {
		return ""
	}
	return b.community.APGroupID
}

// upstreamBanCutoff is when the ban took effect upstream: the Block's own
// `published` when it carries one earlier than the moment the inbox received
// it, otherwise that receipt time. A `published` after receipt claims a ban not
// yet made, so receipt bounds it. Never the processing time — a ban that waited
// in the queue must not reach content the author wrote while it waited.
func upstreamBanCutoff(block *ap.Object, receivedAt time.Time) time.Time {
	if block.Published.OK() && block.Published.Before(receivedAt) {
		return block.Published.Time
	}
	return receivedAt
}

// removesData reports a Block's `removeData: true`; absent is false.
func removesData(block *ap.Object) bool {
	return block.RemoveData != nil && *block.RemoveData
}

// decideSiteBan decides a DIRECTLY delivered Block: a home instance's site ban
// of one of its own users, carrying removeData. It returns nil (and no error)
// for every Block that is not one, which the caller ignores as before.
//
// A site ban is ordered under the signer's key, so it can overtake a pre-ban
// Create still retrying under its community's key; that post then lands after
// the ban and stays live. Re-running the replay removes it.
func (h *Handler) decideSiteBan(ctx context.Context, block *ap.Object, signer string, receivedAt time.Time) (*upstreamBan, error) {
	if !isSiteBanShape(block, signer) {
		return nil, nil
	}
	subjectAPID := refID(block.Object)
	author, err := h.bridgedPerson(ctx, subjectAPID)
	if author == nil || err != nil {
		return nil, err
	}
	return &upstreamBan{
		activityID:  block.ID,
		blockID:     block.ID,
		subjectAPID: subjectAPID,
		authorDID:   author.DID,
		cutoff:      upstreamBanCutoff(block, receivedAt),
		reason:      block.Summary,
	}, nil
}

// isSiteBanShape reports whether a directly delivered Block has a home site
// ban's shape, whoever its subject turns out to be.
//
// The authorization is the same-authority rule content deletes already rest
// on: an instance is the origin of its users' content and could delete each
// piece of it itself, so its VERIFIED signer may take that content down in one
// activity. All three of signer, subject and target must share one authority
// (scheme and host), and the target must be the instance root (Lemmy's site
// actor) — a Block aimed at a community, or at another instance, is not a site
// ban of that host's user.
func isSiteBanShape(block *ap.Object, signer string) bool {
	target := refID(block.Target)
	return removesData(block) && isInstanceRoot(target) &&
		ap.SameAuthority(signer, target) && ap.SameAuthority(refID(block.Object), target)
}

// decideCommunityBan decides an announced Block whose target is the announcing
// community and whose subject is not one of our personas: it removes content
// only when the subject is a bridged Lemmy user and the ban carries removeData.
// It returns nil (and no error) otherwise.
//
// No community_bans row is written for such a subject: that table gates our
// personas' future posts, and a bridged user's future content reaches us only
// through the community that banned them, which will not announce it.
// activityID is the id of the Announce that carried the Block.
func (h *Handler) decideCommunityBan(ctx context.Context, block *ap.Object, announcer *store.Community, activityID string, receivedAt time.Time) (*upstreamBan, error) {
	if !removesData(block) {
		return nil, nil
	}
	subjectAPID := refID(block.Object)
	author, err := h.bridgedPerson(ctx, subjectAPID)
	if author == nil || err != nil {
		return nil, err
	}
	return &upstreamBan{
		activityID:  activityID,
		blockID:     block.ID,
		subjectAPID: subjectAPID,
		authorDID:   author.DID,
		community:   announcer,
		cutoff:      upstreamBanCutoff(block, receivedAt),
		reason:      block.Summary,
	}, nil
}

// bridgedPerson returns the bridged Person behind apID, or nil when there is
// none. A bridged GROUP is refused as a ban subject: its DID is a community
// repo, and selecting "its content" would select the whole community's.
func (h *Handler) bridgedPerson(ctx context.Context, apID string) (*store.BridgedActor, error) {
	if apID == "" {
		return nil, nil
	}
	actor, err := h.actors.GetByAPActorID(ctx, apID)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ingest: look up ban subject %s: %w", apID, err)
	}
	if actor.ActorType != store.ActorTypePerson || actor.DID == "" {
		return nil, nil
	}
	return actor, nil
}

// isInstanceRoot reports whether id is an instance's root URL — Lemmy's site
// actor, `https://host/` — with no path, query or fragment.
func isInstanceRoot(id string) bool {
	parsed, err := url.Parse(id)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return false
	}
	return (parsed.Path == "" || parsed.Path == "/") && parsed.RawQuery == "" && parsed.Fragment == ""
}

// selectBannedContent is the read half of acting on a ban.
func (h *Handler) selectBannedContent(ctx context.Context, ban *upstreamBan) ([]*store.APObjectMapping, error) {
	if h.bannedContent == nil {
		// Retryable, never a skip: a ban we cannot act on must stay visible.
		return nil, fmt.Errorf(
			"ingest: no banned-content surface is wired, so removeData for %s cannot be applied", ban.subjectAPID)
	}
	mappings, err := h.bannedContent.SelectBannedContent(ctx, ban.authorDID, ban.communityDID(), ban.cutoff)
	if err != nil {
		return nil, fmt.Errorf("ingest: select %s's content for ban %s: %w", ban.subjectAPID, ban.activityID, err)
	}
	return mappings, nil
}

// removeBannedContent applies a decided ban: the selection, then its removal.
func (h *Handler) removeBannedContent(ctx context.Context, ban *upstreamBan) error {
	mappings, err := h.selectBannedContent(ctx, ban)
	if err != nil {
		return err
	}
	if err := h.bannedContent.RemoveBannedContent(ctx, mappings, ban.communityDID(), ban.reason); err != nil {
		return fmt.Errorf("ingest: remove %s's content for ban %s: %w", ban.subjectAPID, ban.activityID, err)
	}
	h.logger.Info("upstream ban carried removeData; the bridged author's earlier content was removed",
		"activity", ban.activityID, "block", ban.blockID, "subject", ban.subjectAPID, "subject_did", ban.authorDID,
		"scope", ban.scope(), "community", ban.communityAPID(), "cutoff", ban.cutoff,
		"removed", len(mappings))
	return nil
}

// decideRecordedBan re-decides one recorded inbox event as the live dispatch
// would decide it now, from the stored payload, verified signer and
// received_at — except that an Announce whose object is only an id is not
// fetched and decides nothing (production holds none). It returns nil for
// anything that is not a qualifying upstream ban — including a ban of one of
// our personas, which the live path recorded through applyBan and the replay
// must not touch.
func (h *Handler) decideRecordedBan(ctx context.Context, event *store.InboxEvent) (*upstreamBan, error) {
	if len(event.Payload) == 0 || event.ActorID == "" {
		return nil, nil
	}
	activity, err := ap.ParseObject(event.Payload)
	if err != nil {
		// It decided nothing when it arrived either.
		return nil, nil //nolint:nilerr // an unparseable payload is not a ban
	}
	var ban *upstreamBan
	switch activity.Type {
	case ap.TypeBlock:
		ban, err = h.decideSiteBan(ctx, activity, event.ActorID, event.ReceivedAt)
	case ap.TypeAnnounce:
		ban, err = h.decideRecordedAnnouncedBan(ctx, activity, event)
	}
	if ban != nil {
		// The inbox event's own id — for an announced ban, the Announce rather
		// than the Block inside it — which is what an operator looks the row up by.
		ban.activityID = event.ActivityID
	}
	return ban, err
}

// decideRecordedAnnouncedBan is decideRecordedBan for a stored Announce: the
// gates handleAnnounce (including its echo suppression) and handleBlock apply
// before decideCommunityBan, in the same order.
func (h *Handler) decideRecordedAnnouncedBan(ctx context.Context, activity *ap.Object, event *store.InboxEvent) (*upstreamBan, error) {
	block := activity.Object
	if block == nil || block.Type != ap.TypeBlock {
		return nil, nil
	}
	announcer, err := h.communities.GetByAPGroupID(ctx, event.ActorID)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ingest: look up announcing community %s: %w", event.ActorID, err)
	}
	if announcer.FollowState == store.FollowStateNone {
		return nil, nil
	}
	// handleAnnounce's echo policy on the raw envelope: an Announce of our own
	// persona's Block was dropped live and must not be acted on here.
	envelope, err := h.classifyEcho(ctx, event.ActivityID, activity)
	if err != nil {
		return nil, err
	}
	if envelope.Class != echo.ClassNone || refID(block.Target) != announcer.APGroupID {
		return nil, nil
	}
	subjectAPID := refID(block.Object)
	if subjectAPID == "" {
		return nil, nil
	}
	identity, err := h.classifier.Classify(ctx, &ap.Object{ID: subjectAPID})
	if err != nil {
		return nil, fmt.Errorf("ingest: identify ban subject %s: %w", subjectAPID, err)
	}
	if identity.Class == echo.ClassLocalActor && identity.DID != "" {
		return nil, nil
	}
	return h.decideCommunityBan(ctx, block, announcer, event.ActivityID, event.ReceivedAt)
}

// BanEventLister lists the recorded (processed or poisoned) inbox events that
// may carry a ban. store.InboxEvents implements it.
type BanEventLister interface {
	ListRecordedBans(ctx context.Context) ([]*store.InboxEvent, error)
}

// BanReplayer re-reads recorded inbox events through the upstream ban logic —
// the cleanup for bans received before Tidepool acted on removeData for
// bridged users.
type BanReplayer struct {
	handler *Handler
	events  BanEventLister
}

// NewBanReplayer builds the replay over the dispatcher that decides each ban
// and the event store it reads them from.
func NewBanReplayer(handler *Handler, events BanEventLister) *BanReplayer {
	return &BanReplayer{handler: handler, events: events}
}

// BanReplayReport is the replay's answer: each qualifying ban that selected
// anything or failed, in inbox event order, with the at-uris it removed (or, in
// a dry run, would remove).
type BanReplayReport struct {
	DryRun       bool             `json:"dry_run"`
	Bans         []BanReplayEntry `json:"bans"`
	RemovedTotal int              `json:"removed_total"`
}

// BanReplayEntry is one replayed ban. Error is set when deciding, selecting
// or removing it failed; such a ban removed nothing the report counts.
type BanReplayEntry struct {
	ActivityID string   `json:"activity_id"`
	Subject    string   `json:"subject"`
	Scope      string   `json:"scope"`
	Community  string   `json:"community"`
	Removed    []string `json:"removed"`
	Error      string   `json:"error,omitempty"`
}

// banReplayCarry is what earlier bans in one run took: records deleted, and
// postv2s withdrawn from a community (keyed by community DID and at-uri).
type banReplayCarry struct {
	deleted   map[string]bool
	withdrawn map[string]bool
}

// ReplayBans decides every recorded ban event in id order and, unless dryRun,
// removes what each selects before deciding the next. It is idempotent:
// removed and deleted records drop out of later selections, so a second run
// removes nothing.
//
// A ban that fails is reported with its error and the run moves on to the
// next: one broken ban must not strand every ban after it. Only a failure to
// list the events, or the request's context ending, stops the run.
//
// A dry run mutates nothing, and to list what a real run WOULD do it carries
// forward what each earlier ban in the run would have taken: a record an
// earlier ban deletes is not listed again, nor a postv2 an earlier ban already
// withdrew from the same community.
func (r *BanReplayer) ReplayBans(ctx context.Context, dryRun bool) (*BanReplayReport, error) {
	events, err := r.events.ListRecordedBans(ctx)
	if err != nil {
		return nil, fmt.Errorf("ingest: list recorded bans: %w", err)
	}
	report := &BanReplayReport{DryRun: dryRun, Bans: []BanReplayEntry{}}
	carry := &banReplayCarry{deleted: map[string]bool{}, withdrawn: map[string]bool{}}
	for _, event := range events {
		entry, err := r.replayBan(ctx, event, dryRun, carry)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("ingest: ban replay stopped at %s: %w", event.ActivityID, ctxErr)
		}
		if err != nil {
			r.handler.logger.Error("ban replay failed a ban; continuing with the next",
				"activity", event.ActivityID, "error", err)
			entry.Error = err.Error()
			report.Bans = append(report.Bans, *entry)
			continue
		}
		if entry == nil {
			continue
		}
		report.Bans = append(report.Bans, *entry)
		report.RemovedTotal += len(entry.Removed)
	}
	return report, nil
}

// replayBan decides and applies one recorded event. It returns nil for an
// event that is not a qualifying ban or selects nothing new. On error the
// entry it returns names the ban as far as it was decided and lists nothing
// removed, though a removal that failed partway may have applied some of it;
// carry is left as it was.
func (r *BanReplayer) replayBan(ctx context.Context, event *store.InboxEvent, dryRun bool, carry *banReplayCarry) (*BanReplayEntry, error) {
	failed := &BanReplayEntry{ActivityID: event.ActivityID, Removed: []string{}}
	ban, err := r.handler.decideRecordedBan(ctx, event)
	if err != nil {
		return failed, fmt.Errorf("ingest: decide recorded ban %s: %w", event.ActivityID, err)
	}
	if ban == nil {
		return nil, nil
	}
	failed.Subject, failed.Scope, failed.Community = ban.subjectAPID, ban.scope(), ban.communityAPID()
	selected, err := r.handler.selectBannedContent(ctx, ban)
	if err != nil {
		return failed, err
	}
	var mappings []*store.APObjectMapping
	var deletions, withdrawals []string
	removed := []string{}
	for _, mapping := range selected {
		withdrawal := materialize.BanWithdraws(ban.communityDID(), mapping)
		withdrawalKey := ban.communityDID() + " " + mapping.ATURI
		if carry.deleted[mapping.ATURI] || (withdrawal && carry.withdrawn[withdrawalKey]) {
			continue
		}
		if withdrawal {
			withdrawals = append(withdrawals, withdrawalKey)
		} else {
			deletions = append(deletions, mapping.ATURI)
		}
		mappings = append(mappings, mapping)
		removed = append(removed, mapping.ATURI)
	}
	if len(mappings) == 0 {
		return nil, nil
	}
	if !dryRun {
		if err := r.handler.bannedContent.RemoveBannedContent(ctx, mappings, ban.communityDID(), ban.reason); err != nil {
			return failed, fmt.Errorf("ingest: replay ban %s: %w", ban.activityID, err)
		}
	}
	// Carried forward only once the ban succeeded. A ban that failed partway
	// may already have removed some records; the next run re-selects whatever
	// is still live, so its carry stays empty here.
	for _, uri := range deletions {
		carry.deleted[uri] = true
	}
	for _, key := range withdrawals {
		carry.withdrawn[key] = true
	}
	sort.Strings(removed)
	entry := &BanReplayEntry{
		ActivityID: ban.activityID,
		Subject:    ban.subjectAPID,
		Scope:      ban.scope(),
		Community:  ban.communityAPID(),
		Removed:    removed,
	}
	if !dryRun {
		// Logged as each ban is applied, so a run cut short still leaves a
		// record of what it already removed.
		r.handler.logger.Info("ban replay applied a ban",
			"activity", entry.ActivityID, "block", ban.blockID, "subject", entry.Subject,
			"scope", entry.Scope, "community", entry.Community, "removed", len(entry.Removed))
	}
	return entry, nil
}

// banReplayWriteTimeout bounds how long POST /admin/inbox/replay-bans may take
// to write its response. A replay walks every recorded ban and, in a real run,
// deletes records and blobs as it goes; the server's default 30s WriteTimeout
// would drop the report of a long run after its removals were already made.
const banReplayWriteTimeout = 10 * time.Minute

// handleReplayBans serves POST /admin/inbox/replay-bans {"dry_run": bool}.
// dry_run is required: an empty body must never be read as "remove". It
// extends this response's write deadline to banReplayWriteTimeout.
func (a *Admin) handleReplayBans(w http.ResponseWriter, r *http.Request) {
	if a.banReplayer == nil {
		http.Error(w, "ban replay is not configured", http.StatusNotImplemented)
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(banReplayWriteTimeout)); err != nil {
		a.logger.WarnContext(r.Context(), "extend ban replay write deadline", "error", err)
	}
	var req struct {
		DryRun *bool `json:"dry_run"`
	}
	if err := decodeJSONBody(r, &req); err != nil || req.DryRun == nil {
		http.Error(w, `body must be {"dry_run":true} or {"dry_run":false}`, http.StatusBadRequest)
		return
	}
	report, err := a.banReplayer.ReplayBans(r.Context(), *req.DryRun)
	if err != nil {
		a.logger.Error("ban replay failed", "dry_run", *req.DryRun, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	a.logger.Info("ban replay finished",
		"dry_run", report.DryRun, "bans", len(report.Bans), "removed_total", report.RemovedTotal)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		a.logger.Warn("write ban replay report", "dry_run", report.DryRun, "error", err)
	}
}
