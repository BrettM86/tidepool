// Package errors provides sentinel and typed errors shared across Tidepool.
// It mirrors the Coves error conventions: sentinel values for errors.Is
// checks, small typed errors carrying context, and helpers for the common
// classification questions. Typed errors unwrap to their matching sentinel,
// so errors.Is(err, ErrNotFound) works on a NotFoundError and anything
// wrapping one with %w.
package errors

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
	ErrInvalidInput  = errors.New("invalid input")
	// ErrTombstoned marks a resource that existed but was soft-deleted
	// (AP Delete / Tombstone). It is deliberately distinct from ErrNotFound:
	// a missing object may be fetched and materialized, a tombstoned one
	// must not be. IsNotFound(tombstoned) is false.
	ErrTombstoned = errors.New("resource tombstoned")
	// ErrRecordGone marks a bridged-stats emission that cannot proceed
	// because the TARGET RECORD itself is gone — deleted out from under its
	// vote aggregate, or its mapping soft-deleted inside the commit
	// transaction. Deliberately distinct from ErrNotFound: the vote-stats
	// refresher advances its watermark on a real record-gone but NOT on an
	// unrelated NotFound surfaced from deeper in a commit (a missing
	// bridged_actor row or signing key — a key-escrow inconsistency to
	// retry, never to skip). IsNotFound(recordGone) is false.
	ErrRecordGone = errors.New("bridged record gone")
)

// ValidationError reports a rejected field value.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// Unwrap makes errors.Is(err, ErrInvalidInput) true for validation errors.
func (e ValidationError) Unwrap() error { return ErrInvalidInput }

// NotFoundError reports a missing resource with its identifier.
type NotFoundError struct {
	Resource string
	ID       any
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("%s with ID '%v' not found", e.Resource, e.ID)
}

// Unwrap makes errors.Is(err, ErrNotFound) true for not-found errors.
func (e NotFoundError) Unwrap() error { return ErrNotFound }

// ConflictError reports a uniqueness conflict on a resource field.
type ConflictError struct {
	Resource string
	Field    string
	Value    string
}

func (e ConflictError) Error() string {
	return fmt.Sprintf("%s with %s '%s' already exists", e.Resource, e.Field, e.Value)
}

// Unwrap makes errors.Is(err, ErrAlreadyExists) true for conflict errors.
func (e ConflictError) Unwrap() error { return ErrAlreadyExists }

// TombstonedError reports a soft-deleted resource with its identifier.
type TombstonedError struct {
	Resource string
	ID       any
}

func (e TombstonedError) Error() string {
	return fmt.Sprintf("%s with ID '%v' is tombstoned", e.Resource, e.ID)
}

// Unwrap makes errors.Is(err, ErrTombstoned) true for tombstoned errors.
func (e TombstonedError) Unwrap() error { return ErrTombstoned }

func NewValidationError(field, message string) error {
	return ValidationError{Field: field, Message: message}
}

func NewNotFoundError(resource string, id any) error {
	return NotFoundError{Resource: resource, ID: id}
}

func NewConflictError(resource, field, value string) error {
	return ConflictError{Resource: resource, Field: field, Value: value}
}

func NewTombstonedError(resource string, id any) error {
	return TombstonedError{Resource: resource, ID: id}
}

// IsNotFound reports whether err is, wraps, or unwraps to ErrNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

// IsAlreadyExists reports whether err is, wraps, or unwraps to ErrAlreadyExists.
func IsAlreadyExists(err error) bool { return errors.Is(err, ErrAlreadyExists) }

// IsValidation reports whether err is, wraps, or unwraps to ErrInvalidInput.
func IsValidation(err error) bool { return errors.Is(err, ErrInvalidInput) }

// IsTombstoned reports whether err is, wraps, or unwraps to ErrTombstoned.
func IsTombstoned(err error) bool { return errors.Is(err, ErrTombstoned) }

// IsRecordGone reports whether err is, wraps, or unwraps to ErrRecordGone.
func IsRecordGone(err error) bool { return errors.Is(err, ErrRecordGone) }
