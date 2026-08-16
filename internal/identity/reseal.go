package identity

import (
	"context"
	"database/sql"
)

// This file is the RED-phase surface of the KEK re-seal drill: the walk that
// moves every KEK-sealed blob from the previous KEK onto the current one, so
// an operator mid-rotation can finish the job and unset BRIDGE_KEK_PREVIOUS.
// Run 1 taught the bridge to READ under two keys; this is what lets it stop
// needing to. Everything below is a stub — the counts are the operator's
// gate, and the acceptance test in kek_reseal_test.go says what they mean.

// ResealCounts is one table's tally from a re-seal walk. The four buckets are
// disjoint and together account for every row the walk considered.
type ResealCounts struct {
	// Resealed opened under the previous KEK and was written back under the
	// current one.
	Resealed int
	// AlreadyCurrent opened under the current KEK and was left alone. A
	// second run reports everything here — that is the operator's zero-run
	// gate for unsetting BRIDGE_KEK_PREVIOUS.
	AlreadyCurrent int
	// Skipped had no sealed material to move (a NULL signing_key).
	Skipped int
	// Failed opened under neither KEK.
	Failed int
}

// ResealFailureReason classifies why a blob could not be re-sealed, because
// the two classes send an operator to different places: a wrong-key blob is a
// key-history question, a malformed one is storage corruption.
type ResealFailureReason string

const (
	// ResealFailureWrongKey means the blob is well formed but authenticates
	// under neither the current nor the previous KEK.
	ResealFailureWrongKey ResealFailureReason = "wrong-key"
	// ResealFailureMalformed means the blob is truncated or carries an
	// unknown version byte: it was never decrypted under either key, so
	// nothing about the KEKs is in question.
	ResealFailureMalformed ResealFailureReason = "malformed"
)

// ResealFailure names one row the walk could not move.
type ResealFailure struct {
	// Table is the postgres table name, as an operator would type it.
	Table string
	// ID is the row's did (bridged_actors, ap_actors) or name
	// (service_keys).
	ID string
	// Reason is the class of failure.
	Reason ResealFailureReason
}

// ResealReport is the inventory a re-seal walk hands back.
type ResealReport struct {
	BridgedActors ResealCounts
	APActors      ResealCounts
	ServiceKeys   ResealCounts
	Failures      []ResealFailure
}

// Reseal walks every KEK-sealed blob and re-seals it under current, so the
// previous KEK can be retired. It returns the report even when it also
// returns an error: an operator running the drill needs the full inventory
// from one pass, not the prefix before the first bad row.
func Reseal(ctx context.Context, db *sql.DB, current, previous []byte) (*ResealReport, error) {
	return &ResealReport{}, nil
}
