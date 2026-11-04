package identity

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// One budget serves a rename and a mint at once. Each 12 second step releases
// exactly one POST: a double charge misses the next slot, a missing charge
// lands two POSTs in one slot. The order of the three POSTs is unfixed.
func TestPLCWriteBudget_SharedByUpdateAndMint(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{
		post: func(path string, attempt int) plcResponse {
			if path == "/"+budgetTestDID && attempt == 1 {
				return plcResponse{status: http.StatusInternalServerError}
			}
			return plcResponse{}
		},
	})
	ctx, _ := cancellableContext(t)

	update, result := fixture.startUpdate(ctx)
	mint := fixture.startMint(ctx, budgetMintRequest)
	calls := []*blockingCall{update, mint}

	for slot := 1; slot <= 3; slot++ {
		awaitCondition(t, func() bool {
			unfinished := 0
			for _, call := range calls {
				if !call.returned() {
					unfinished++
				}
			}
			return fixture.plc.postCount() >= slot && fixture.clock.waiterCount() == unfinished
		}, "slot %d: want its POST seen and every unfinished call in budget Wait", slot)
		require.Equal(t, slot, fixture.plc.postCount(), "each slot releases exactly one POST")
		if slot < 3 {
			fixture.clock.Advance(12 * time.Second)
		}
	}

	require.NoError(t, update.result(t))
	require.Equal(t, HandleUpdateApplied, result.Outcome)
	require.NoError(t, mint.result(t))
	require.Equal(t, []time.Time{
		clockStart, clockStart.Add(12 * time.Second), clockStart.Add(24 * time.Second),
	}, fixture.plc.postTimes())
}

func TestMintActor_BareMintChargesOneSlot(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{})

	_, err := fixture.minter.MintActor(t.Context(), budgetMintRequest)

	require.NoError(t, err)
	require.Equal(t, []time.Time{clockStart}, fixture.plc.postTimes(), "a fresh budget needs no clock advance")
	fixture.clock.Advance(11999 * time.Millisecond)
	require.False(t, fixture.budget.Allow(), "the mint spent the slot")
	fixture.clock.Advance(time.Millisecond)
	require.True(t, fixture.budget.Allow(), "the next slot is at +12s")
}
