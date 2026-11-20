package ap

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// TestParseObject_ShapeRejectionsAreErrPayloadShape: the depth and element
// caps reject with ErrPayloadShape, which is a validation error (consumers
// treat it as terminal); malformed JSON is not a shape rejection.
func TestParseObject_ShapeRejectionsAreErrPayloadShape(t *testing.T) {
	cases := []struct {
		name      string
		payload   string
		wantShape bool
	}{
		{name: "depth 33", payload: nestedObjects(33, `"https://x.example/a"`), wantShape: true},
		{name: "30001 elements", payload: emptyStringOutbox(30000), wantShape: true},
		{name: "malformed JSON", payload: `{"id":`, wantShape: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseObject([]byte(tc.payload))
			require.Error(t, err)
			assert.Equal(t, tc.wantShape, stderrors.Is(err, ErrPayloadShape),
				"errors.Is(err, ErrPayloadShape) for %q", err)
			if tc.wantShape {
				assert.True(t, errors.IsValidation(err), "a shape rejection is a validation error: %q", err)
			}
		})
	}
}
