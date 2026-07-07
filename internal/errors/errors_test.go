package errors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypedErrorsUnwrapToSentinels(t *testing.T) {
	assert.True(t, IsNotFound(NewNotFoundError("ap_object", "https://lemmy.world/post/1")))
	assert.True(t, IsAlreadyExists(NewConflictError("bridged_actor", "did", "did:plc:abc")))
	assert.True(t, IsValidation(NewValidationError("did", "must not be empty")))
	assert.True(t, IsTombstoned(NewTombstonedError("ap_object", "https://lemmy.world/post/1")))
}

func TestTombstonedIsDistinctFromNotFound(t *testing.T) {
	// The materializer branches on this distinction: missing objects are
	// fetched, tombstoned ones are dropped. Neither may masquerade as the
	// other.
	tombstoned := NewTombstonedError("ap_object", "https://lemmy.world/post/1")
	assert.False(t, IsNotFound(tombstoned), "tombstoned must not satisfy IsNotFound")

	missing := NewNotFoundError("ap_object", "https://lemmy.world/post/1")
	assert.False(t, IsTombstoned(missing), "not-found must not satisfy IsTombstoned")

	wrapped := fmt.Errorf("resolve strongRef: %w", tombstoned)
	assert.True(t, IsTombstoned(wrapped))
}

func TestHelpersMatchWrappedErrors(t *testing.T) {
	wrapped := fmt.Errorf("resolve strongRef: %w", NewNotFoundError("ap_object", "x"))
	assert.True(t, IsNotFound(wrapped), "%%w-wrapped typed errors must still match")

	doublyWrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrAlreadyExists))
	assert.True(t, IsAlreadyExists(doublyWrapped))

	assert.False(t, IsNotFound(ErrInvalidInput))
	assert.False(t, IsAlreadyExists(nil))
	assert.False(t, IsValidation(fmt.Errorf("plain error")))
}

func TestErrorMessages(t *testing.T) {
	assert.Equal(t,
		"validation error on field 'did': must not be empty",
		NewValidationError("did", "must not be empty").Error())
	assert.Equal(t,
		"ap_object with ID 'https://x/1' not found",
		NewNotFoundError("ap_object", "https://x/1").Error())
	assert.Equal(t,
		"community with did 'did:plc:abc' already exists",
		NewConflictError("community", "did", "did:plc:abc").Error())
	assert.Equal(t,
		"ap_object with ID 'https://x/1' is tombstoned",
		NewTombstonedError("ap_object", "https://x/1").Error())
}
