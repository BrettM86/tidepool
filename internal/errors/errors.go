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
	// ErrCollectionImmutable marks a mapping write refused because the AP id
	// already maps a record in another collection. Deliberately distinct from
	// ErrAlreadyExists: that one is an at-uri collision (a deterministic-rkey
	// bug signal), this one is an id claimed as something it is not.
	// IsAlreadyExists(collectionImmutable) is false.
	ErrCollectionImmutable = errors.New("mapping collection is immutable")
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

// IsCollectionImmutable reports whether err is, wraps, or unwraps to
// ErrCollectionImmutable.
func IsCollectionImmutable(err error) bool { return errors.Is(err, ErrCollectionImmutable) }

// ErrBlobWithheld marks a blob store refused because an active withhold
// covers the (DID, CID).
var ErrBlobWithheld = errors.New("blob withheld")

// IsBlobWithheld reports a store refused by an active blob withhold.
func IsBlobWithheld(err error) bool { return errors.Is(err, ErrBlobWithheld) }

// ErrHostGone marks a fetch whose origin host looks gone or unreachable: the
// object's own host not resolving, refusing or timing out connections,
// failing TLS, answering a CDN "origin down" status, or parking every URL
// (HostGoneError.Reason names which). The condition may be temporary (a
// lapsed domain can come back), so it is deliberately not ErrNotFound and
// IsNotFound is false for it. Backfill skips the object and defers it for
// RetryDeferred once the failure is recorded into the host's streak (an
// unrecorded one is an ordinary failure); redirect_to_root, parked_redirect
// and too_many_redirects concern only the URL fetched, not the whole host. No live
// caller acts on host-gone.
var ErrHostGone = errors.New("host gone")

// IsHostGone reports whether err is, wraps, or unwraps to ErrHostGone.
func IsHostGone(err error) bool { return errors.Is(err, ErrHostGone) }

// Host-gone reason codes, carried in HostGoneError.Reason.
const (
	// HostGoneReasonDNSNotFound: the host's name does not resolve (NXDOMAIN).
	HostGoneReasonDNSNotFound = "dns_not_found"
	// HostGoneReasonParkedRedirect: a redirect off the host ends at a non-JSON
	// page (a lapsed domain parked elsewhere).
	HostGoneReasonParkedRedirect = "parked_redirect"
	// HostGoneReasonConnectTimeout: connecting to the host timed out.
	HostGoneReasonConnectTimeout = "connect_timeout"
	// HostGoneReasonConnectionRefused: the host refused the connection.
	HostGoneReasonConnectionRefused = "connection_refused"
	// HostGoneReasonConnectionReset: the host reset the connection while
	// connecting.
	HostGoneReasonConnectionReset = "connection_reset"
	// HostGoneReasonNoRoute: no route to the host or its network.
	HostGoneReasonNoRoute = "no_route"
	// HostGoneReasonTLSFailure: the TLS handshake failed (certificate
	// verification, handshake alert, or a non-TLS answer).
	HostGoneReasonTLSFailure = "tls_failure"
	// HostGoneReasonTooManyRedirects: the host redirects to itself past the
	// redirect limit.
	HostGoneReasonTooManyRedirects = "too_many_redirects"
	// HostGoneReasonEdgeStatus: the host's CDN edge reports its origin down
	// (HTTP 521, 522, 523, 525, 526 or 530).
	HostGoneReasonEdgeStatus = "edge_status"
	// HostGoneReasonRedirectToRoot: the host redirects the object to its own
	// root, which answers a non-JSON page.
	HostGoneReasonRedirectToRoot = "redirect_to_root"
)

// HostGoneError is a host-gone failure naming the dead host: the fetched
// IRI's own host, lowercased and without a port.
type HostGoneError struct {
	Host string
	// Reason is the HostGoneReason... code naming what failed.
	Reason string
}

// Error names the gone host and the reason.
func (e HostGoneError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("host gone: %s", e.Host)
	}
	return fmt.Sprintf("host gone: %s (%s)", e.Host, e.Reason)
}

// Unwrap makes errors.Is(err, ErrHostGone) true for host-gone errors.
func (e HostGoneError) Unwrap() error { return ErrHostGone }

// HostGoneHost returns the dead host carried by a HostGoneError in err's
// chain. ok is false when err carries none (including a bare ErrHostGone) or
// the host is empty: streaks are keyed on it, so "" never names a host.
func HostGoneHost(err error) (host string, ok bool) {
	var hostGone HostGoneError
	if !errors.As(err, &hostGone) || hostGone.Host == "" {
		return "", false
	}
	return hostGone.Host, true
}

// HostGoneReason returns the reason code carried by a HostGoneError in err's
// chain. ok is false when err carries none or the reason is empty.
func HostGoneReason(err error) (reason string, ok bool) {
	var hostGone HostGoneError
	if !errors.As(err, &hostGone) || hostGone.Reason == "" {
		return "", false
	}
	return hostGone.Reason, true
}
