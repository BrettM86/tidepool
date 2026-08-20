package outbound

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// What the worker does when SignerFor fails, split by WHY it failed.
//
// The worker used to treat every signer error as transient — "a KEK blip, a
// not-yet-replicated actor: retry rather than poison". Two of those are not the
// same thing. A missing or not-yet-replicated actor really does resolve on its
// own. A BRIDGE_KEK that does not open the actor's sealed key never does: the
// key at rest and the key in the environment are simply different keys, and the
// delivery burns its whole retry budget before poisoning hours later under an
// excerpt reading "message authentication failed" — which reads as data
// corruption, not as the config change that caused it.
//
// The split matters most in the shape this failure actually takes in
// production: newly minted actors seal and open fine under the wrong key, while
// every actor that existed before the change fails. Half the bridge works. The
// operator needs the class to say KEK.

// unsealableSignerError is the error the real signer path produces when the
// configured BRIDGE_KEK does not open an actor's sealed AP key: a genuine
// custodian mismatch, wrapped exactly as personas.actorSigner wraps it, so this
// test cannot pass against a hand-written sentinel the production path never
// emits.
func unsealableSignerError(t *testing.T) error {
	t.Helper()
	kekA := sha256.Sum256([]byte("outbound-test-kek-at-rest"))
	kekB := sha256.Sum256([]byte("outbound-test-kek-configured"))

	atRest, err := identity.NewCustodian(kekA[:])
	require.NoError(t, err)
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	sealed, err := atRest.EncryptActorRSAKey(wActorDID, key)
	require.NoError(t, err)

	configured, err := identity.NewCustodian(kekB[:])
	require.NoError(t, err)
	_, err = configured.DecryptActorRSAKey(wActorDID, sealed)
	require.Error(t, err, "the fixture must really fail to unseal, or this test proves nothing")
	return fmt.Errorf("personas: unseal AP key for %s: %w", wActorDID, err)
}

// TestWorker_UnsealableSignerKeyPoisonsImmediately is the red for the
// misclassification.
//
// GIVEN a delivery whose actor's key will not open under the configured
// BRIDGE_KEK, WHEN the worker runs it with its retry budget untouched, THEN the
// delivery poisons on the FIRST attempt, under a class that names the
// configuration, and nothing is POSTed.
func TestWorker_UnsealableSignerKeyPoisonsImmediately(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.Signers = fakeSigners{err: unsealableSignerError(t)}
	})

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, d.State,
		"a key that does not open under the configured BRIDGE_KEK is not a blip: retrying it cannot change the answer, and the retry budget only delays the alarm by hours while every pre-existing actor fails the same way")
	assert.Equal(t, store.PoisonClassKEKMisconfigured, d.LastErrorClass,
		"the class an operator reads off the dead-letter table must say the KEK is wrong. Under the generic signer class this is indistinguishable from a replication lag, and the excerpt underneath it says 'message authentication failed', which reads as corrupted data")
	assert.Contains(t, d.ResponseExcerpt, "BRIDGE_KEK",
		"the excerpt is where the operator lands; it has to name the variable")
	assert.Zero(t, sender.count(),
		"nothing may be POSTed: there is no signature to send")
}

// TestWorker_MissingSignerStillRetries is the other half, and the reason the
// split is not simply "poison every signer error".
//
// A DID whose actor row has not replicated yet is exactly the transient case
// the old comment described. It must keep its retries: poisoning it would turn
// a few seconds of lag into a permanent non-delivery.
func TestWorker_MissingSignerStillRetries(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.Signers = fakeSigners{err: errors.NewNotFoundError("ap_actor", wActorDID)}
	})

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, d.State,
		"an actor row that has not arrived yet is genuinely transient: the delivery stays pending and retries")
	assert.Equal(t, store.PoisonClassSigner, d.LastErrorClass,
		"and it keeps the generic signer class, so the KEK class stays a signal rather than a synonym for 'signer trouble'")
	assert.Zero(t, sender.count())
}
