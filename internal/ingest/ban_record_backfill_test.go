package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// BACK-FILLING BAN RECORDS.
//
// Bans accepted before community bans became ban records exist only in
// community_bans (native personas) or in the recorded inbox history (bridged
// persons). POST /admin/moderation/backfill-ban-records publishes a ban record
// for every ban in force now: native personas from community_bans rows in
// force; bridged persons by replaying recorded Block and Undo{Block} events in
// inbox id order, each judged at its own received_at under the live rules
// (stale-unban refusal included). A final standing ban is published only if it
// is in force at back-fill time; a ban whose lifting the history cannot rule
// out is reported undecidable and not published.
//
// The response contract:
//
//	{"dry_run": bool,
//	 "published": [{"community": did, "subject": did}],
//	 "already_published": int,
//	 "undecidable": [{"community": did, "subject": did, "activity_id": id}],
//	 "skipped_unminted": int,
//	 "failed": int}
const backfillBanRecordsPath = "/admin/moderation/backfill-ban-records"

type banBackfillReport struct {
	DryRun    bool `json:"dry_run"`
	Published []struct {
		Community string `json:"community"`
		Subject   string `json:"subject"`
	} `json:"published"`
	AlreadyPublished int `json:"already_published"`
	Undecidable      []struct {
		Community  string `json:"community"`
		Subject    string `json:"subject"`
		ActivityID string `json:"activity_id"`
	} `json:"undecidable"`
	SkippedUnminted int `json:"skipped_unminted"`
	Failed          int `json:"failed"`
}

// banKey is one (community, subject) pair; for undecidable entries,
// activityID is the recorded event that keeps the ban undecidable.
type banKey struct {
	community  string
	subject    string
	activityID string
}

func runBanBackfill(t *testing.T, h *harness, body map[string]any) banBackfillReport {
	t.Helper()
	response := h.adminRequest(http.MethodPost, backfillBanRecordsPath, body)
	require.Equal(t, http.StatusOK, response.Code, "the ban back-fill answers its report: %s", response.Body.String())
	var report banBackfillReport
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &report), response.Body.String())
	return report
}

func publishedOf(report banBackfillReport) []banKey {
	var keys []banKey
	for _, entry := range report.Published {
		keys = append(keys, banKey{community: entry.Community, subject: entry.Subject})
	}
	return keys
}

func undecidableOf(report banBackfillReport) []banKey {
	var keys []banKey
	for _, entry := range report.Undecidable {
		keys = append(keys, banKey{community: entry.Community, subject: entry.Subject, activityID: entry.ActivityID})
	}
	return keys
}

// seedNativeBan writes a community_bans row straight to the table: a native
// persona's ban accepted before ban records existed.
func seedNativeBan(t *testing.T, h *harness, communityDID, communityAPID, subjectDID, reason string, expiresAt *time.Time) {
	t.Helper()
	_, err := h.db.ExecContext(context.Background(), `
		INSERT INTO community_bans (community_did, subject_did, community_ap_id, banned_at, expires_at, reason)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		communityDID, subjectDID, communityAPID, time.Now().Add(-40*24*time.Hour), expiresAt, reason)
	require.NoError(t, err, "seed the community_bans row for %s", subjectDID)
}

// bridgedBackfillPerson is one minted bridged person on programming.dev, the
// subject of one recorded ban history in community C.
type bridgedBackfillPerson struct {
	id  string
	did string
}

func mintBackfillPerson(w *upstreamBanWorld, name string) bridgedBackfillPerson {
	w.h.t.Helper()
	id := ubHomeHost + "/u/" + name
	w.h.newRemoteActor(id, person(id, name, nil))
	w.postBy(id, w.communityC, "br-b10-mint-"+name, ubBeforeBan, false)
	minted, err := w.h.actors.GetByAPActorID(context.Background(), id)
	require.NoError(w.h.t, err, "precondition: %s is minted", id)
	require.Equal(w.h.t, testDIDFor(name, "programming.dev"), minted.DID, "precondition: %s's DID", id)
	return bridgedBackfillPerson{id: id, did: minted.DID}
}

// TestTheBanRecordBackfillPublishesEveryBanInForceOnce is B10.
//
// GIVEN native community_bans rows in C (permanent, expired, in force with an
// expiry) and recorded inbox histories h1–h11 for eleven minted bridged
// persons in C, two of them (h10, h11) with a ban record already in C's repo;
// WHEN the back-fill runs as a dry run THEN it lists exactly the bans in force
// now that hold no record (native permanent and in force, h2, h3, h7), reports
// h5 and h9 undecidable, counts h10 and h11 already published (a standing
// record is the live path's, never overwritten), and writes nothing; WHEN it runs
// for real THEN exactly those records exist once each with the deciding ban's
// subject, reason and expiry; WHEN it runs again THEN nothing is published and
// all seven count as already published. A request without dry_run is refused.
func TestTheBanRecordBackfillPublishesEveryBanInForceOnce(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	community := world.communityC
	communityDID := ubCommunityCDID

	now := time.Now().UTC().Truncate(time.Second)
	// base is the history's start: every recorded event is received around
	// thirty days ago, so "in force at receipt" and "lapsed now" are both
	// unambiguous for the expiries below.
	base := now.Add(-30 * 24 * time.Hour)
	hour := time.Hour
	day := 24 * time.Hour
	expires := func(when time.Time) string { return when.Format(time.RFC3339) }

	// --- Native personas: community_bans rows, no records yet.
	const (
		nativePermanentDID = "did:plc:brb10nativepermanentaaaa"
		nativeExpiredDID   = "did:plc:brb10nativeexpiredaaaaaa"
		nativeInForceDID   = "did:plc:brb10nativeinforceaaaaaa"
	)
	nativeInForceExpiry := now.Add(10 * day)
	nativeExpiredExpiry := now.Add(-hour)
	seedNativeBan(t, h, communityDID, community.id, nativePermanentDID, "native: permanent", nil)
	seedNativeBan(t, h, communityDID, community.id, nativeExpiredDID, "native: served", &nativeExpiredExpiry)
	seedNativeBan(t, h, communityDID, community.id, nativeInForceDID, "native: in force", &nativeInForceExpiry)

	// --- Bridged persons, one per history, all minted before any history is
	// recorded (minting drains the queue; recorded events are already
	// processed or poisoned, so the drain leaves them alone).
	p := map[string]bridgedBackfillPerson{}
	for _, name := range []string{"BrB10H1", "BrB10H2", "BrB10H3", "BrB10H4", "BrB10H5", "BrB10H6",
		"BrB10H7", "BrB10H8", "BrB10H9", "BrB10H10", "BrB10H11"} {
		p[name] = mintBackfillPerson(world, name)
	}

	block := func(id, subject string, fields map[string]any) map[string]any {
		extra := map[string]any{"removeData": false}
		for key, value := range fields {
			extra[key] = value
		}
		return communityBlockAnnounce(community, id, subject, extra)
	}
	undo := func(id, blockID, subject string, undone map[string]any) map[string]any {
		extra := map[string]any{"removeData": false}
		for key, value := range undone {
			extra[key] = value
		}
		return communityUndoBlockAnnounce(community, id, blockID, subject, extra)
	}
	const act = "https://lemmy.world/activities/announce/"

	// h1: ban, then unban (an Undo naming no expiry) -> nothing stands.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h1", p["BrB10H1"].id,
		map[string]any{"summary": "h1"}), base)
	enqueueProcessed(t, h, community, undo(act+"undo/br-b10-h1", act+"block/br-b10-h1", p["BrB10H1"].id,
		map[string]any{"expires": nil}), base.Add(hour))

	// h2: ban only, in force with an expiry -> published.
	h2Expiry := now.Add(30 * day)
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h2", p["BrB10H2"].id,
		map[string]any{"summary": "h2: spam", "expires": expires(h2Expiry)}), base)

	// h3: ban to T2, then a stale unban naming T1 < T2 -> the T2 ban stands.
	h3Expiry := now.Add(60 * day)
	h3Earlier := now.Add(20 * day)
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h3", p["BrB10H3"].id,
		map[string]any{"summary": "h3: harassment", "expires": expires(h3Expiry)}), base)
	enqueueProcessed(t, h, community, undo(act+"undo/br-b10-h3", act+"block/br-b10-h3-earlier", p["BrB10H3"].id,
		map[string]any{"expires": expires(h3Earlier)}), base.Add(hour))

	// h4: a ban whose expiry had already passed at its received_at -> nothing.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h4", p["BrB10H4"].id,
		map[string]any{"summary": "h4", "expires": expires(base.Add(-hour))}), base)

	// h5: ban, then a poisoned ban-shaped event for the same subject ->
	// undecidable.
	const h5Poisoned = act + "undo/br-b10-h5"
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h5", p["BrB10H5"].id,
		map[string]any{"summary": "h5"}), base)
	enqueuePoisoned(t, h, community, undo(h5Poisoned, act+"block/br-b10-h5", p["BrB10H5"].id,
		map[string]any{"expires": nil}), base.Add(hour))

	// h6: permanent ban, then a temporary Block received before its expiry,
	// which has since passed -> the replacement lapsed, nothing stands.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h6-permanent", p["BrB10H6"].id,
		map[string]any{"summary": "h6: permanent"}), base)
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h6-temporary", p["BrB10H6"].id,
		map[string]any{"summary": "h6: temporary", "expires": expires(base.Add(2 * day))}), base.Add(hour))

	// h7: permanent ban, then a temporary Block already past its expiry when
	// received -> the later Block replaces nothing; the permanent ban stands.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h7-permanent", p["BrB10H7"].id,
		map[string]any{"summary": "h7: permanent"}), base)
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h7-lapsed", p["BrB10H7"].id,
		map[string]any{"summary": "h7: lapsed", "expires": expires(base.Add(-hour))}), base.Add(hour))

	// h8: permanent ban, then a LATER inbox id temporary Block whose
	// received_at is EARLIER, in force at its receipt and lapsed now. Inbox
	// id order decides: the temporary Block replaces the permanent one and
	// has lapsed -> nothing stands.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h8-permanent", p["BrB10H8"].id,
		map[string]any{"summary": "h8: permanent"}), base.Add(2*hour))
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h8-temporary", p["BrB10H8"].id,
		map[string]any{"summary": "h8: temporary", "expires": expires(base.Add(day))}), base)

	// h9: ban, then a later processed Undo{Block} for the same subject whose
	// undone Block's expires is unparseable -> undecidable.
	const h9Unparseable = act + "undo/br-b10-h9"
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h9", p["BrB10H9"].id,
		map[string]any{"summary": "h9"}), base)
	enqueueProcessed(t, h, community, undo(h9Unparseable, act+"block/br-b10-h9", p["BrB10H9"].id,
		map[string]any{"expires": "not-a-date"}), base.Add(hour))

	// h10: ban only; a record already stands with the same subject, reason
	// and expiry (a different createdAt) -> already published.
	h10Expiry := now.Add(40 * day)
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h10", p["BrB10H10"].id,
		map[string]any{"summary": "h10: spam", "expires": expires(h10Expiry)}), base)
	h10CID := seedBanRecord(t, h, communityDID, p["BrB10H10"].did, "h10: spam", &h10Expiry)

	// h11: ban only; a record already stands with a different reason ->
	// already published: a standing record is the live path's to change, so
	// the back-fill never overwrites it.
	enqueueProcessed(t, h, community, block(act+"block/br-b10-h11", p["BrB10H11"].id,
		map[string]any{"summary": "h11: the Block's reason"}), base)
	h11StaleCID := seedBanRecord(t, h, communityDID, p["BrB10H11"].did, "h11: an older reason", nil)

	wantPublished := []banKey{
		{community: communityDID, subject: nativePermanentDID},
		{community: communityDID, subject: nativeInForceDID},
		{community: communityDID, subject: p["BrB10H2"].did},
		{community: communityDID, subject: p["BrB10H3"].did},
		{community: communityDID, subject: p["BrB10H7"].did},
	}
	wantUndecidable := []banKey{
		{community: communityDID, subject: p["BrB10H5"].did, activityID: h5Poisoned},
		{community: communityDID, subject: p["BrB10H9"].did, activityID: h9Unparseable},
	}
	notPublished := []string{nativeExpiredDID, p["BrB10H1"].did, p["BrB10H4"].did, p["BrB10H5"].did,
		p["BrB10H6"].did, p["BrB10H8"].did, p["BrB10H9"].did}

	head := repoHead(t, h, communityDID)
	require.Equal(t, 2, banRecordCount(t, h, communityDID), "precondition: only h10's and h11's records stand")

	// --- Dry run: the listing, and nothing written.
	before := replayStateSnapshot(t, h)
	dryRun := runBanBackfill(t, h, map[string]any{"dry_run": true})
	assert.True(t, dryRun.DryRun)
	assert.ElementsMatch(t, wantPublished, publishedOf(dryRun),
		"the dry run lists exactly the bans in force now that lack a matching record")
	assert.ElementsMatch(t, wantUndecidable, undecidableOf(dryRun),
		"h5 (a later poisoned ban event) and h9 (a later unparseable Undo) cannot be decided")
	assert.Equal(t, 2, dryRun.AlreadyPublished, "h10's and h11's records already stand")
	assert.Zero(t, dryRun.SkippedUnminted)
	assert.Zero(t, dryRun.Failed)
	after := replayStateSnapshot(t, h)
	for table, rows := range before {
		assert.Equal(t, rows, after[table], "a dry run writes nothing: %s changed", table)
	}
	assert.Equal(t, head, repoHead(t, h, communityDID), "a dry run commits nothing to C's repo")
	assert.Equal(t, 2, banRecordCount(t, h, communityDID), "a dry run writes no ban record")
	_, h11CID, _ := banRecord(t, h, communityDID, p["BrB10H11"].did)
	assert.Equal(t, h11StaleCID, h11CID, "a dry run leaves h11's record uncorrected")

	// --- Real run: exactly those records, once each.
	first := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.False(t, first.DryRun)
	assert.ElementsMatch(t, wantPublished, publishedOf(first), "the real run publishes what the dry run listed")
	assert.ElementsMatch(t, wantUndecidable, undecidableOf(first))
	assert.Equal(t, 2, first.AlreadyPublished)
	assert.Zero(t, first.SkippedUnminted)
	assert.Zero(t, first.Failed)

	for _, want := range []struct {
		name    string
		subject string
		reason  string
		expiry  *time.Time
	}{
		{"native permanent", nativePermanentDID, "native: permanent", nil},
		{"native in force", nativeInForceDID, "native: in force", &nativeInForceExpiry},
		{"h2 ban only", p["BrB10H2"].did, "h2: spam", &h2Expiry},
		{"h3 ban outlasting a stale unban", p["BrB10H3"].did, "h3: harassment", &h3Expiry},
		{"h7 permanent ban after a Block lapsed at receipt", p["BrB10H7"].did, "h7: permanent", nil},
		{"h11 record left as it stood", p["BrB10H11"].did, "h11: an older reason", nil},
		{"h10 record untouched", p["BrB10H10"].did, "h10: spam", &h10Expiry},
	} {
		record, _, found := banRecord(t, h, communityDID, want.subject)
		if assert.True(t, found, "%s: the back-fill publishes its ban record", want.name) {
			assertBanRecordFields(t, record, want.subject, want.reason, want.expiry)
		}
	}
	_, h10After, _ := banRecord(t, h, communityDID, p["BrB10H10"].did)
	assert.Equal(t, h10CID, h10After, "h10's matching record is left as it was")
	_, h11After, _ := banRecord(t, h, communityDID, p["BrB10H11"].did)
	assert.Equal(t, h11StaleCID, h11After, "h11's standing record is not overwritten")
	for _, subject := range notPublished {
		_, _, found := banRecord(t, h, communityDID, subject)
		assert.False(t, found, "no ban record for %s: not in force now, or undecidable", subject)
	}
	assert.Equal(t, 7, banRecordCount(t, h, communityDID), "one record per ban in force, nothing else")

	// --- Again: nothing left to publish.
	headAfterFirst := repoHead(t, h, communityDID)
	second := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.Empty(t, second.Published, "a second run has nothing to publish")
	assert.Equal(t, 7, second.AlreadyPublished, "every ban in force already has its record")
	assert.ElementsMatch(t, wantUndecidable, undecidableOf(second))
	assert.Equal(t, headAfterFirst, repoHead(t, h, communityDID), "a second run commits nothing")

	// --- dry_run is required, and a refused request writes nothing.
	for _, body := range []map[string]any{nil, {}} {
		response := h.adminRequest(http.MethodPost, backfillBanRecordsPath, body)
		assert.Equal(t, http.StatusBadRequest, response.Code, "dry_run is required (body %v): %s", body, response.Body.String())
	}
	assert.Equal(t, headAfterFirst, repoHead(t, h, communityDID), "a refused request commits nothing")
}

// liveCommunityBans is the community-ban store with afterListInForce run once
// the back-fill has read its native snapshot: live moderation landing between
// the snapshot and the back-fill's writes.
type liveCommunityBans struct {
	store.CommunityBans
	afterListInForce func()
}

func (bans *liveCommunityBans) ListInForce(ctx context.Context) ([]store.CommunityBan, error) {
	inForce, err := bans.CommunityBans.ListInForce(ctx)
	if err == nil && bans.afterListInForce != nil {
		bans.afterListInForce()
	}
	return inForce, err
}

// liveBanRecords is the ban-record publisher with afterStandingRead run once
// the back-fill has read a pair's standing record, just before it writes.
type liveBanRecords struct {
	BanRecordPublisher
	afterStandingRead func(communityDID, subjectDID string)
}

func (records *liveBanRecords) StandingBanRecord(ctx context.Context, communityDID, subjectDID string) (*materialize.BanRecord, error) {
	standing, err := records.BanRecordPublisher.StandingBanRecord(ctx, communityDID, subjectDID)
	if err == nil && records.afterStandingRead != nil {
		records.afterStandingRead(communityDID, subjectDID)
	}
	return standing, err
}

// TestTheBanRecordBackfillDoesNotPublishANativeBanLiftedAfterItsSnapshot:
// GIVEN a native ban in force when the back-fill lists community_bans, WHEN a
// live Undo{Block} lifts it before the back-fill writes (the live unban finds
// no record to delete) THEN the back-fill re-checks the ban, publishes nothing
// and Coves is left enforcing no ban the moderators lifted.
func TestTheBanRecordBackfillDoesNotPublishANativeBanLiftedAfterItsSnapshot(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const subjectDID = "did:plc:brliftedaftersnapshotaaa"
	seedNativeBan(t, h, ubCommunityCDID, world.communityC.id, subjectDID, "native: lifted meanwhile", nil)
	h.handler.bans = &liveCommunityBans{CommunityBans: h.handler.bans, afterListInForce: func() {
		_, err := h.db.ExecContext(context.Background(),
			`DELETE FROM community_bans WHERE community_did = $1 AND subject_did = $2`, ubCommunityCDID, subjectDID)
		require.NoError(t, err, "the live unban lifts the row")
	}}

	report := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.Empty(t, report.Published, "a ban lifted after the snapshot is not published")
	assert.Zero(t, report.Failed)
	_, _, found := banRecord(t, h, ubCommunityCDID, subjectDID)
	assert.False(t, found, "no ban record for a lifted ban")
}

// TestTheBanRecordBackfillNeverOverwritesAStandingRecord: GIVEN native bans in
// force, one whose record already stands with a different reason (a live
// re-ban wrote it) and one whose record a live re-ban writes after the
// back-fill read none, WHEN the back-fill runs THEN neither record is
// overwritten, both count as already published, and nothing failed.
func TestTheBanRecordBackfillNeverOverwritesAStandingRecord(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const (
		standingDID  = "did:plc:brstandingrecordaaaaaaaa"
		meanwhileDID = "did:plc:brrecordwrittenmeanwhile"
	)
	stale := time.Now().Add(3 * 24 * time.Hour)
	seedNativeBan(t, h, ubCommunityCDID, world.communityC.id, standingDID, "native: stale 3-day ban", &stale)
	seedNativeBan(t, h, ubCommunityCDID, world.communityC.id, meanwhileDID, "native: stale 3-day ban", &stale)
	standingCID := seedBanRecord(t, h, ubCommunityCDID, standingDID, "live: permanent re-ban", nil)

	live := h.handler.banRecords
	h.handler.banRecords = &liveBanRecords{BanRecordPublisher: live, afterStandingRead: func(_, subjectDID string) {
		if subjectDID != meanwhileDID {
			return
		}
		_, err := live.WriteBanRecord(context.Background(), ubCommunityCDID, meanwhileDID,
			"live: permanent re-ban", nil, time.Now())
		require.NoError(t, err, "the live re-ban writes its record")
	}}
	report := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.Empty(t, report.Published, "a standing record is never overwritten")
	assert.Equal(t, 2, report.AlreadyPublished, "both standing records count as already published")
	assert.Zero(t, report.Failed, "a record the live path wrote meanwhile is not a failure")

	record, cid, found := banRecord(t, h, ubCommunityCDID, standingDID)
	require.True(t, found)
	assert.Equal(t, standingCID, cid, "the standing record is untouched")
	assertBanRecordFields(t, record, standingDID, "live: permanent re-ban", nil)
	record, _, found = banRecord(t, h, ubCommunityCDID, meanwhileDID)
	require.True(t, found)
	assertBanRecordFields(t, record, meanwhileDID, "live: permanent re-ban", nil)
}

// TestTheBanRecordBackfillLeavesABridgedBanWithANewerEventUndecided: GIVEN two
// bridged persons each banned by a processed Block in force, WHEN one has an
// Undo{Block} still pending when the back-fill starts, and the other's
// Undo{Block} arrives after the back-fill read the history but before it
// writes, THEN both are reported undecidable with that Undo's activity id and
// no record is written for either.
func TestTheBanRecordBackfillLeavesABridgedBanWithANewerEventUndecided(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	community := world.communityC
	pending := mintBackfillPerson(world, "BrPendingUndo")
	arriving := mintBackfillPerson(world, "BrArrivingUndo")
	const act = "https://lemmy.world/activities/announce/"
	base := time.Now().Add(-24 * time.Hour)
	noData := map[string]any{"removeData": false}

	enqueueProcessed(t, h, community, communityBlockAnnounce(community, act+"block/br-pending", pending.id, noData), base)
	enqueueProcessed(t, h, community, communityBlockAnnounce(community, act+"block/br-arriving", arriving.id, noData), base)
	const pendingUndo = act + "undo/br-pending"
	require.Equal(t, http.StatusAccepted, h.deliver(community,
		communityUndoBlockAnnounce(community, pendingUndo, act+"block/br-pending", pending.id, noData)),
		"precondition: the Undo is queued, not yet processed")

	const arrivingUndo = act + "undo/br-arriving"
	h.handler.banRecords = &liveBanRecords{BanRecordPublisher: h.handler.banRecords, afterStandingRead: func(_, subjectDID string) {
		if subjectDID != arriving.did {
			return
		}
		require.Equal(t, http.StatusAccepted, h.deliver(community,
			communityUndoBlockAnnounce(community, arrivingUndo, act+"block/br-arriving", arriving.id, noData)),
			"the live Undo arrives after the history was read")
	}}

	report := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.Empty(t, report.Published, "a ban with a newer or pending ban event is not published")
	assert.ElementsMatch(t, []banKey{
		{community: ubCommunityCDID, subject: pending.did, activityID: pendingUndo},
		{community: ubCommunityCDID, subject: arriving.did, activityID: arrivingUndo},
	}, undecidableOf(report))
	assert.Zero(t, report.Failed)
	for _, subject := range []string{pending.did, arriving.did} {
		_, _, found := banRecord(t, h, ubCommunityCDID, subject)
		assert.False(t, found, "no ban record for %s", subject)
	}
}

// insertRecordedEvent writes an inbox row straight to the table, for a
// payload the inbox would refuse to parse: poisoned at a minute after receipt.
func insertRecordedEvent(t *testing.T, h *harness, actorID, activityID string, payload []byte, receivedAt time.Time) {
	t.Helper()
	_, err := h.db.ExecContext(context.Background(), `
		INSERT INTO inbox_events (activity_id, type, payload, actor_id, ordering_key, received_at, failed_at, error)
		VALUES ($1, 'Announce', $2, $3, $3, $4, $5, 'gave up after repeated failures')`,
		activityID, payload, actorID, receivedAt, receivedAt.Add(time.Minute))
	require.NoError(t, err, "seed the recorded event %s", activityID)
}

// TestTheBanRecordBackfillLeavesABanWithAnUnparseableLaterEventUndecided is h12:
// GIVEN a bridged person banned in C, then a poisoned Announce{Undo{Block}} for
// the same subject whose payload does not parse as an activity (the undone
// Block's id is a number), WHEN the back-fill runs THEN the ban is reported
// undecidable with that activity id and no record is written. The same
// unparseable Undo targeting another community is ignored: that person's ban
// in C is published.
func TestTheBanRecordBackfillLeavesABanWithAnUnparseableLaterEventUndecided(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	community := world.communityC
	undecided := mintBackfillPerson(world, "BrH12Unparseable")
	elsewhere := mintBackfillPerson(world, "BrH12Elsewhere")
	const act = "https://lemmy.world/activities/announce/"
	base := time.Now().Add(-24 * time.Hour)
	noData := map[string]any{"removeData": false}

	unparseableUndo := func(activityID, subject, target string) []byte {
		payload, err := json.Marshal(map[string]any{
			"id": activityID, "type": "Announce", "actor": community.id,
			"object": map[string]any{
				"id": activityID + "/undo", "type": "Undo", "actor": modActorID,
				"object": map[string]any{
					"id": 12345, "type": "Block", "actor": modActorID,
					"object": subject, "target": target,
				},
			},
		})
		require.NoError(t, err)
		_, parseErr := ap.ParseObject(payload)
		require.Error(t, parseErr, "precondition: the payload does not parse as an activity")
		return payload
	}

	enqueueProcessed(t, h, community, communityBlockAnnounce(community, act+"block/br-h12", undecided.id, noData), base)
	const h12Undo = act + "undo/br-h12"
	insertRecordedEvent(t, h, community.id, h12Undo, unparseableUndo(h12Undo, undecided.id, community.id), base.Add(time.Hour))

	enqueueProcessed(t, h, community, communityBlockAnnounce(community, act+"block/br-h12-elsewhere", elsewhere.id, noData), base)
	const elsewhereUndo = act + "undo/br-h12-elsewhere"
	insertRecordedEvent(t, h, community.id, elsewhereUndo,
		unparseableUndo(elsewhereUndo, elsewhere.id, world.communityD.id), base.Add(time.Hour))

	report := runBanBackfill(t, h, map[string]any{"dry_run": false})
	assert.Equal(t, []banKey{{community: ubCommunityCDID, subject: undecided.did, activityID: h12Undo}},
		undecidableOf(report), "a later unparseable Undo for the same community and subject makes the ban undecidable")
	assert.Equal(t, []banKey{{community: ubCommunityCDID, subject: elsewhere.did}}, publishedOf(report),
		"an unparseable Undo targeting another community is ignored")
	_, _, found := banRecord(t, h, ubCommunityCDID, undecided.did)
	assert.False(t, found, "no ban record for the undecidable ban")
	_, _, found = banRecord(t, h, ubCommunityCDID, elsewhere.did)
	assert.True(t, found, "the ban in C stands and is published")
}
