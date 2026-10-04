package delegation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

func testRun(t *testing.T, reconciler *Reconciler) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		reconciler.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	return cancel, done
}

func TestRunStartsImmediatelyAndRepeats(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		listings int
	}{
		{name: "immediate startup pass", interval: time.Hour, listings: 1},
		{name: "repeats on interval", interval: 10 * time.Millisecond, listings: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			reconciler, _ := newTestReconciler(t, fake, fakeLabelSource{}, tc.interval)
			cancel, done := testRun(t, reconciler)
			require.Eventually(t, func() bool {
				return len(requestsByMethod(fake.snapshot(), http.MethodGet)) >= tc.listings
			}, 2*time.Second, 5*time.Millisecond, "Run must list the zone without waiting for its first interval, then repeat")
			cancel()
			require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, 5*time.Millisecond)
			require.Empty(t, postNames(fake.snapshot()))
		})
	}
}

func TestNewReconcilerDefaultsRunInterval(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	reconciler, _ := newTestReconciler(t, fake, fakeLabelSource{})
	require.Equal(t, 15*time.Minute, reconciler.interval)
}

func TestRunRetriesAfterListingFailureAndLogsError(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	fake.failFirstListing = true
	fake.listingFailCode = http.StatusInternalServerError
	fake.listingFailBody = `{"success":false,"errors":[{"message":"temporary listing failure"}],"result":null}`
	reconciler, log := newTestReconciler(t, fake, fakeLabelSource{
		labels: []store.InstanceLabel{{Label: "retry", HasLiveActor: true}},
	}, 10*time.Millisecond)
	cancel, done := testRun(t, reconciler)
	require.Eventually(t, func() bool {
		return len(requestsByMethod(fake.snapshot(), http.MethodGet)) >= 2 &&
			len(postNames(fake.snapshot())) >= 1
	}, 2*time.Second, 5*time.Millisecond, "a failed first listing must not stop later passes")
	cancel()
	require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, 5*time.Millisecond)
	require.ElementsMatch(t, []string{"retry.tdpl.example"}, postNames(fake.snapshot()))
	require.Len(t, delegationLogLines(log.String(), "ERROR", `msg="delegation pass failed"`), 1)
	require.Contains(t, delegationLogLines(log.String(), "ERROR", `msg="delegation pass failed"`)[0], `error="`)
	require.Contains(t, delegationLogLines(log.String(), "ERROR", `msg="delegation pass failed"`)[0], "temporary listing failure")
}

func channelClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

type heldPassAnswer struct {
	result Result
	err    error
}

// Every row starts a pass, observes the first request inside the fake handler,
// and leaves it blocked while exercising cancellation or a competing caller.
func TestHeldDelegationPass(t *testing.T) {
	for _, tc := range []struct {
		name         string
		blockMethod  string
		labelCount   int
		shutdownRun  bool
		cancelWaiter bool
	}{
		{name: "run cancellation interrupts held listing", blockMethod: http.MethodGet, labelCount: 1, shutdownRun: true},
		{name: "serializes listing GET", blockMethod: http.MethodGet, labelCount: 1},
		{name: "serializes create POST", blockMethod: http.MethodPost, labelCount: 1},
		{name: "shared hourly budget across serialized passes", blockMethod: http.MethodGet, labelCount: 25},
		{name: "cancelled waiter sends no request", blockMethod: http.MethodGet, labelCount: 1, cancelWaiter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			fake.pageSize = 100 // One listing GET per pass, even with 20 newly created records.
			entered, release := fake.holdFirstRequest(t, tc.blockMethod)
			source := fakeLabelSource{}
			for index := range tc.labelCount {
				source.labels = append(source.labels, store.InstanceLabel{
					Label: fmt.Sprintf("label%02d", index), HasLiveActor: true,
				})
			}
			reconciler, log := newTestReconciler(t, fake, source, 10*time.Millisecond)
			if tc.shutdownRun {
				cancel, done := testRun(t, reconciler)
				require.Eventually(t, func() bool { return channelClosed(entered) }, 2*time.Second, 5*time.Millisecond,
					"startup pass must reach the held listing")
				cancel()
				require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, 5*time.Millisecond,
					"Run must return before the held handler is released")
				require.Never(t, func() bool { return len(fake.snapshot()) > 1 }, 80*time.Millisecond, 5*time.Millisecond)
				require.Empty(t, delegationLogLines(log.String(), "ERROR", `msg="delegation pass failed"`))
				release()
				// label00 qualifies, so a pass still running after Run returned would list again or POST it.
				require.Never(t, func() bool { return len(fake.snapshot()) > 1 }, 200*time.Millisecond, 5*time.Millisecond,
					"no request may reach the API once Run has returned")
				require.Empty(t, postNames(fake.snapshot()))
				return
			}

			firstContext, cancelFirst := context.WithCancel(context.Background())
			firstDone := make(chan struct{})
			var first heldPassAnswer
			go func() {
				defer close(firstDone)
				first.result, first.err = reconciler.Reconcile(firstContext)
			}()
			secondContext, cancelSecond := context.WithCancel(context.Background())
			secondDone := make(chan struct{})
			var second heldPassAnswer
			secondStarted := false
			t.Cleanup(func() {
				release()
				cancelFirst()
				cancelSecond()
				<-firstDone
				if secondStarted {
					<-secondDone
				}
			})
			require.Eventually(t, func() bool { return channelClosed(entered) }, 2*time.Second, 5*time.Millisecond,
				"first pass must enter the held handler")
			aboutToCall := make(chan struct{})
			secondStarted = true
			go func() {
				defer close(secondDone)
				close(aboutToCall)
				second.result, second.err = reconciler.Reconcile(secondContext)
			}()
			require.Eventually(t, func() bool { return channelClosed(aboutToCall) }, time.Second, 5*time.Millisecond)
			require.Never(t, func() bool {
				return channelClosed(secondDone) || len(requestsByMethod(fake.snapshot(), http.MethodGet)) > 1
			}, 120*time.Millisecond, 5*time.Millisecond,
				"waiting caller must not list or return while the first pass holds the lock")
			if tc.cancelWaiter {
				cancelSecond()
				require.Eventually(t, func() bool { return channelClosed(secondDone) }, time.Second, 5*time.Millisecond,
					"cancelled waiter must return without the first pass finishing")
				require.ErrorIs(t, second.err, context.Canceled)
				require.Equal(t, Result{}, second.result)
				require.False(t, channelClosed(firstDone), "first pass is still held")
			}
			release()
			require.Eventually(t, func() bool { return channelClosed(firstDone) }, 2*time.Second, 5*time.Millisecond)
			require.NoError(t, first.err)
			if tc.cancelWaiter {
				require.Len(t, requestsByMethod(fake.snapshot(), http.MethodGet), 1)
				require.ElementsMatch(t, []string{"label00.tdpl.example"}, postNames(fake.snapshot()))
				return
			}
			require.Eventually(t, func() bool { return channelClosed(secondDone) }, 2*time.Second, 5*time.Millisecond)
			require.NoError(t, second.err)
			require.Len(t, requestsByMethod(fake.snapshot(), http.MethodGet), 2, "second caller must run its own pass")
			if tc.labelCount == 1 {
				require.ElementsMatch(t, []string{"label00"}, first.result.Created)
				require.Empty(t, second.result.Created)
				require.ElementsMatch(t, []string{"label00"}, second.result.AlreadyDelegated)
				require.ElementsMatch(t, []string{"label00.tdpl.example"}, postNames(fake.snapshot()))
				return
			}
			require.ElementsMatch(t, []string{
				"label00.tdpl.example", "label01.tdpl.example", "label02.tdpl.example", "label03.tdpl.example", "label04.tdpl.example",
				"label05.tdpl.example", "label06.tdpl.example", "label07.tdpl.example", "label08.tdpl.example", "label09.tdpl.example",
				"label10.tdpl.example", "label11.tdpl.example", "label12.tdpl.example", "label13.tdpl.example", "label14.tdpl.example",
				"label15.tdpl.example", "label16.tdpl.example", "label17.tdpl.example", "label18.tdpl.example", "label19.tdpl.example",
			}, postNames(fake.snapshot()))
			require.Len(t, first.result.Created, 20)
			require.ElementsMatch(t, []string{
				"label00", "label01", "label02", "label03", "label04", "label05", "label06", "label07", "label08", "label09",
				"label10", "label11", "label12", "label13", "label14", "label15", "label16", "label17", "label18", "label19",
			}, second.result.AlreadyDelegated)
			require.Empty(t, second.result.Created)
			require.ElementsMatch(t, []string{"label20", "label21", "label22", "label23", "label24"}, second.result.Deferred)
			require.Equal(t, 20, second.result.DelegatedRecords)
			require.Equal(t, 1000, second.result.Ceiling)
		})
	}
}
