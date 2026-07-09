package ingest

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/materialize"
)

type countingMinter struct{ mints int }

func (m *countingMinter) MintActor(_ context.Context, _ identity.MintRequest) (*identity.Identity, error) {
	m.mints++
	return &identity.Identity{DID: "did:plc:fake", Handle: "fake.handle"}, nil
}

// TestMintGateBoundsAmplification: the deep-thread attack scenario — many
// distinct authors arriving at once — is capped at the burst size, refused
// mints are retryable (not skips), and the inner minter is never called for
// a refused mint.
func TestMintGateBoundsAmplification(t *testing.T) {
	inner := &countingMinter{}
	gate, err := NewMintGate(inner, 60, 3, nil)
	require.NoError(t, err)

	ctx := context.Background()
	var refused int
	for i := 0; i < 50; i++ {
		_, err := gate.MintActor(ctx, identity.MintRequest{PreferredUsername: "troll", Instance: "evil.example"})
		if err != nil {
			require.True(t, stderrors.Is(err, ErrMintRateExceeded))
			assert.False(t, materialize.IsSkip(err), "a refused mint must be retryable, not a skip")
			refused++
		}
	}
	assert.Equal(t, 3, inner.mints, "mints are capped at the burst size")
	assert.Equal(t, 47, refused)
}

func TestMintGateRejectsBadConfig(t *testing.T) {
	inner := &countingMinter{}
	_, err := NewMintGate(nil, 60, 10, nil)
	assert.Error(t, err)
	_, err = NewMintGate(inner, 0, 10, nil)
	assert.Error(t, err)
	_, err = NewMintGate(inner, 60, 0, nil)
	assert.Error(t, err)
}
