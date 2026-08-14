package votes

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/echo"
	"tidepool/internal/store"
)

// THE CLAMP IS THE ONE SIGNAL THIS SUBSYSTEM PRODUCES.
//
// GREATEST(0, …) floors a baseline that came out negative — the origin's total
// is smaller than the votes we can already account for on that subject. That is
// not arithmetic noise: the shape it has is a peer accepting our votes,
// answering 200, and counting none of them (Lemmy's FederationMode discard).
// After 17b this is the ONLY place in the entire system where that becomes
// observable, so a clamp that fires silently is the difference between a
// diagnosable outage and a number that is quietly wrong.
//
// Which is why the negative control below matters as much as the positive one:
// a counter that advances on EVERY seed is a seed-rate meter wearing an
// incident's name, and it would read as "healthy, no clamps" exactly never.

// clampWorld is a fresh database, aggregator and log sink. Each case gets its
// own aggregator on purpose: the Warn is sampled per-aggregator, so two clamps
// through one instance would suppress the second line and the assertions would
// be measuring the sampler.
//
// The outbound rows here are written through the store rather than the full
// consumer→worker path (which reseed_temporal_test.go drives end to end): the
// clamp is arithmetic over a state that file already proves is reachable.
func clampWorld(t *testing.T) (*Aggregator, *tpLogBuffer, store.APObjects) {
	t.Helper()
	database := testDB(t)
	objects := store.NewAPObjects(database)
	logs := &tpLogBuffer{}
	probe, err := echo.New(echo.Options{
		Objects:         objects,
		OutboundObjects: store.NewOutboundObjects(database),
		Activities:      store.NewOutboundActivities(database),
		Actors:          store.NewAPActors(database),
	})
	require.NoError(t, err)
	agg, err := NewAggregator(database, objects, store.NewCommunities(database),
		&fakeRecords{records: map[string]map[string]any{}}, probe,
		slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	require.NoError(t, err)
	return agg, logs, objects
}

// deliverOutbound writes one DELIVERED outbound vote for a distinct persona.
func deliverOutbound(t *testing.T, agg *Aggregator, subjectAPID, subjectATURI, direction string, n int) {
	t.Helper()
	ctx := context.Background()
	votes := store.NewOutboundVotes(agg.db)
	did := fmt.Sprintf("did:plc:clamppersona%04d", n)
	voteATURI := fmt.Sprintf("at://%s/social.coves.interaction.vote/3lzclampvote%d", did, n)
	_, err := votes.Upsert(ctx, store.OutboundVote{
		VoteATURI:         voteATURI,
		ActorDID:          did,
		SubjectATURI:      subjectATURI,
		SubjectAPID:       subjectAPID,
		CommunityDID:      rsCommunityDID,
		Direction:         direction,
		CurrentActivityID: fmt.Sprintf("https://coves.social/ap/activity/clamp-%d", n),
		DeliveredState:    store.DeliveredStatePending,
	})
	require.NoError(t, err)
	require.NoError(t, votes.SetDeliveredState(ctx, voteATURI, store.DeliveredStateDelivered))
}

// TestClampIsObservableAndOnlyWhenItFires carries its own negative control.
//
// Without the control, moving SeedBaselineClamped.Add(1) above reportSeed's
// `if rawUp >= 0 && rawDown >= 0 { return }` guard — or deleting the guard —
// still produces exactly +1 for the clamping seed and still logs. The guard is
// what makes the counter mean something, and nothing else here protects it.
func TestClampIsObservableAndOnlyWhenItFires(t *testing.T) {
	agg, logs, objects := clampWorld(t)
	ctx := context.Background()
	subjectATURI := bridgeSubject(t, objects, subjectPost, "3lzclampsubj01")

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	deliverOutbound(t, agg, subjectPost, subjectATURI, directionDown, 1)
	deliverOutbound(t, agg, subjectPost, subjectATURI, directionDown, 2)

	// --- NEGATIVE CONTROL: a seed whose baselines are both non-negative.
	//     3 − 1 live = 2 up, 2 − 0 live − 2 ours = 0 down. Nothing is floored.
	before := SeedBaselineClamped.Value()
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 3, 2))
	assert.Equal(t, before, SeedBaselineClamped.Value(),
		"a HEALTHY seed must not touch the counter: one that advances on every seed is a "+
			"seed-rate meter, and it would read 'healthy' exactly never")
	assert.NotContains(t, logs.String(), "clamped",
		"and it must not log an incident either")
	up, down, found := counts(t, agg.db, subjectPost)
	require.True(t, found)
	require.Equal(t, 3, up)
	require.Equal(t, 0, down, "precondition: 2 origin − 2 delivered ours = 0, exactly at the floor")

	// --- THE CLAMP: the origin now reports FEWER votes than we can account for.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 1, 0))

	seededUp, seededDown := seededCounts(t, agg.db, subjectPost)
	assert.Equal(t, 0, seededUp, "1 origin − 1 live inbound = 0")
	assert.Equal(t, 0, seededDown,
		"0 origin − 2 delivered ours = −2, floored: a negative baseline would serve as a "+
			"negative score")

	up, down, _ = counts(t, agg.db, subjectPost)
	assert.Equal(t, 1, up, "the served total is the live inbound vote and nothing else")
	assert.Equal(t, 0, down)

	assert.Equal(t, before+1, SeedBaselineClamped.Value(),
		"the clamp must COUNT: it is the only signal that the origin's total cannot cover "+
			"what we believe we delivered")

	// The line has to carry what makes the counter actionable — WHICH subject,
	// HOW short, and how many of the votes were ours. reportSeed threads those
	// parameters through for this; a bare "clamped" is a number nobody can act
	// on.
	line := logs.String()
	assert.Contains(t, line, "clamped", "the clamp logs at Warn")
	assert.Contains(t, line, subjectPost, "with the subject, or nobody can find the post")
	assert.Contains(t, line, "deficit_down=-2", "with the size of the shortfall")
	assert.Contains(t, line, "ours_down=2",
		"and with how much of it we are responsible for — the difference between 'the origin "+
			"lost our votes' and 'the origin lost everyone's'")
}

// TestClampNamesBothBreachedDirections covers reportSeed's label branching,
// which only ever runs its down-only arm in the tests above.
//
// The composite case is the one that matters: when both directions breach,
// naming one hides the other, and whether the cause is directional is the first
// question anyone asks of this signal.
func TestClampNamesBothBreachedDirections(t *testing.T) {
	t.Run("up only", func(t *testing.T) {
		agg, logs, objects := clampWorld(t)
		ctx := context.Background()
		subjectATURI := bridgeSubject(t, objects, subjectPost, "3lzclampsubj02")
		require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
		deliverOutbound(t, agg, subjectPost, subjectATURI, directionUp, 1)

		// 0 origin up − 1 live − 1 ours = −2; down is 0 − 0 − 0 = 0.
		require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 0, 0))

		line := logs.String()
		assert.Contains(t, line, "direction=up", "only the up direction breached")
		assert.Contains(t, line, "deficit_up=-2")
		assert.Contains(t, line, "deficit_down=0",
			"a direction that did not breach reports 0, not its positive baseline — which "+
				"would read as a deficit")
	})

	t.Run("both directions", func(t *testing.T) {
		agg, logs, objects := clampWorld(t)
		ctx := context.Background()
		subjectATURI := bridgeSubject(t, objects, subjectPost, "3lzclampsubj03")
		require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
		deliverOutbound(t, agg, subjectPost, subjectATURI, directionDown, 1)

		// up: 0 − 1 live − 0 = −1. down: 0 − 0 − 1 ours = −1.
		require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 0, 0))

		line := logs.String()
		assert.Contains(t, line, "direction=up+down",
			"both breached: naming one direction hides the other, and the pair is what says "+
				"whether the cause is directional")
		assert.Contains(t, line, "deficit_up=-1")
		assert.Contains(t, line, "deficit_down=-1",
			"and BOTH deficits are reported — a composite label with one number is worse "+
				"than either alone, because it looks complete")
	})
}

// TestSeedMetricsAreServedByTheAdminEndpoint pins the PUBLISHED NAME, not the Go
// variable. The prefix is load-bearing: ingest.scopedMetrics serves only
// tidepool_*, so a counter registered without it is published to expvar and then
// filtered straight out of /admin/metrics — indistinguishable from a counter
// that never fires, which is the exact failure this signal exists to avoid.
//
// Reading SeedBaselineClamped.Value() (as every other test here does) cannot see
// that: renaming the expvar key changes nothing about the variable.
func TestSeedMetricsAreServedByTheAdminEndpoint(t *testing.T) {
	for _, counter := range []struct {
		what string
		v    *expvar.Int
	}{
		{"the clamp counter", SeedBaselineClamped},
		{"the ours-subtracted counter", SeedOursSubtracted},
	} {
		var published string
		expvar.Do(func(kv expvar.KeyValue) {
			if kv.Value == expvar.Var(counter.v) {
				published = kv.Key
			}
		})
		require.NotEmpty(t, published,
			"%s must be REGISTERED with expvar; an unpublished counter is a local variable",
			counter.what)
		assert.True(t, strings.HasPrefix(published, "tidepool_"),
			"%s is published as %q: /admin/metrics serves only the tidepool_ prefix, so a "+
				"counter named without it is silently filtered out and reads as one that "+
				"never fires", counter.what, published)
	}
}
