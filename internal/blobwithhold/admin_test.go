package blobwithhold_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/go-chi/chi/v5"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// Integration tests for the operator surface (B3-B9, B13, B15). Every CID
// below is the raw sha-256 CIDv1 of "tidepool blobwithhold admin <name>
// bytes", written as a literal computed once outside the code under test.
// Each test owns its bytes, so no CID is shared with another test.

const (
	adminToken = "blobwithhold-admin-test-token"

	ownerA        = "did:plc:bwadminownera"
	ownerB        = "did:plc:bwadminownerb"
	ownerNew      = "did:plc:bwadminownernew"
	communityDID  = "did:plc:bwadmincommunity"
	blobMimeType  = "image/png"
	statePath     = "/admin/blobs?cid="
	withholdPath  = "/admin/blobs/withhold"
	liftPath      = "/admin/blobs/lift"
	deletePath    = "/admin/blobs/delete-bytes"
	withholdsPath = "/admin/blobs/withholds"
)

// testBytes returns the unique payload whose CID literal a test uses.
func testBytes(name string) []byte {
	return []byte("tidepool blobwithhold admin " + name + " bytes")
}

// staticKeys signs every DID with one key; PutBlob/GetBlob never sign, but
// repo.NewManager needs a key source.
type staticKeys struct{ key atcrypto.PrivateKey }

func (s staticKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return s.key, nil
}

// adminWorld is the admin router, its store and a repo manager over the
// test database.
type adminWorld struct {
	db      *sql.DB
	store   *blobwithhold.Store
	manager *repo.Manager
	router  chi.Router
}

// newAdminWorld mounts a fresh Admin and removes the withhold rows and blobs
// rows of cids when the test ends. A nil logger discards.
func newAdminWorld(t *testing.T, logger *slog.Logger, cids ...string) *adminWorld {
	t.Helper()
	db := testutil.DB(t)
	t.Cleanup(func() {
		for _, c := range cids {
			if _, err := db.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, c); err != nil {
				t.Errorf("cleanup blob_withholds %s: %v", c, err)
			}
			if _, err := db.Exec(`DELETE FROM blobs WHERE cid = $1`, c); err != nil {
				t.Errorf("cleanup blobs %s: %v", c, err)
			}
		}
	})
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	store := blobwithhold.NewStore(db)
	admin, err := blobwithhold.NewAdmin(blobwithhold.AdminOptions{Token: adminToken, Store: store, Logger: logger})
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)

	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(db, staticKeys{key: key}, nil)
	require.NoError(t, err)
	return &adminWorld{db: db, store: store, manager: manager, router: router}
}

// adminRequest issues one request. An empty token omits Authorization.
func adminRequest(t *testing.T, router chi.Router, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "https://bridge.example"+path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// call issues an authorized request.
func (w *adminWorld) call(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return adminRequest(t, w.router, method, path, adminToken, body)
}

// state requires a 200 and decodes the State body.
func state(t *testing.T, rec *httptest.ResponseRecorder) blobwithhold.State {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var s blobwithhold.State
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &s), "body: %s", rec.Body.String())
	return s
}

// inspect reads the CID's state through GET /admin/blobs.
func (w *adminWorld) inspect(t *testing.T, cidStr string) blobwithhold.State {
	t.Helper()
	return state(t, w.call(t, http.MethodGet, statePath+cidStr, ""))
}

// listedCIDs reads GET /admin/blobs/withholds and returns the listed cids.
func (w *adminWorld) listedCIDs(t *testing.T) []string {
	t.Helper()
	rec := w.call(t, http.MethodGet, withholdsPath, "")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var body struct {
		Withholds []blobwithhold.Withhold `json:"withholds"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	var cids []string
	for _, row := range body.Withholds {
		cids = append(cids, row.CID)
	}
	return cids
}

// putBlob stores data under did and requires the expected CID.
func (w *adminWorld) putBlob(t *testing.T, did string, data []byte, wantCID string) {
	t.Helper()
	blob, err := w.manager.PutBlob(context.Background(), did, blobMimeType, data)
	require.NoError(t, err, "setup: store the blob for %s", did)
	require.Equal(t, wantCID, cid.Cid(blob.Ref).String(), "setup: blob cid")
}

// operatorWithhold places an operator withhold through the store (setup).
func (w *adminWorld) operatorWithhold(t *testing.T, cidStr, actor string) {
	t.Helper()
	_, err := w.store.Withhold(context.Background(), blobwithhold.OperatorRequest{
		CID: cidStr, ReasonCode: blobwithhold.ReasonIllegalContent, Actor: actor,
	})
	require.NoError(t, err, "setup: operator withhold")
}

// communityRemoval inserts an active community-removal every-owner row.
func (w *adminWorld) communityRemoval(t *testing.T, cidStr string) {
	t.Helper()
	_, err := w.store.Insert(context.Background(), blobwithhold.Withhold{
		CID:          cidStr,
		Source:       blobwithhold.SourceCommunityRemoval,
		Scope:        blobwithhold.ScopeEveryOwner,
		CommunityDID: communityDID,
		SubjectURI:   "at://" + ownerA + "/social.coves.community.post/" + cidStr[len(cidStr)-8:],
		ReasonCode:   "moderator-discretion",
	})
	require.NoError(t, err, "setup: community-removal row")
}

// storedOwners lists the DIDs holding a blobs row for the CID.
func (w *adminWorld) storedOwners(t *testing.T, cidStr string) []string {
	t.Helper()
	rows, err := w.db.Query(`SELECT did FROM blobs WHERE cid = $1 ORDER BY did`, cidStr)
	require.NoError(t, err)
	defer rows.Close()
	var dids []string
	for rows.Next() {
		var did string
		require.NoError(t, rows.Scan(&did))
		dids = append(dids, did)
	}
	require.NoError(t, rows.Err())
	return dids
}

// operatorRow requires exactly one withhold row, from the operator.
func operatorRow(t *testing.T, s blobwithhold.State) blobwithhold.Withhold {
	t.Helper()
	require.Len(t, s.Withholds, 1, "state: %+v", s)
	require.Equal(t, blobwithhold.SourceOperator, s.Withholds[0].Source)
	return s.Withholds[0]
}

func assert4xx(t *testing.T, rec *httptest.ResponseRecorder, msg string) {
	t.Helper()
	assert.GreaterOrEqual(t, rec.Code, 400, "%s: body %s", msg, rec.Body.String())
	assert.LessOrEqual(t, rec.Code, 499, "%s: body %s", msg, rec.Body.String())
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}

// TestAdminWithholdIsIdempotentAcrossEncodings (B3): withholding the same
// CID twice, the second time in base58btc, answers 200 both times with one
// row; the second answer is the existing state, created_by from the first.
func TestAdminWithholdIsIdempotentAcrossEncodings(t *testing.T) {
	const (
		cidBase32    = "bafkreiasajhb7vuvwwj4ty4gg35kkx4webn7avya7lgu752yhi6ym3seyi"
		cidBase58BTC = "zb2rhXraa3UV5dCEsqJmAQ4nQCWpKtdXKBfdc45Esy3Yn4fKo"
		cidBase16    = "f0155122012024e1fd695b593c9e38636faa55f96205bf05700facd4ff7583a3d866e44c2"
	)
	w := newAdminWorld(t, nil, cidBase32)

	first := state(t, w.call(t, http.MethodPost, withholdPath,
		`{"cid":"`+cidBase32+`","reason_code":"illegal-content","actor":"b3-first-operator"}`))
	firstRow := operatorRow(t, first)
	assert.Equal(t, cidBase32, first.CID)
	assert.Equal(t, "b3-first-operator", firstRow.CreatedBy)

	second := state(t, w.call(t, http.MethodPost, withholdPath,
		`{"cid":"`+cidBase58BTC+`","reason_code":"illegal-content","actor":"b3-second-operator"}`))
	secondRow := operatorRow(t, second)
	assert.Equal(t, cidBase32, second.CID, "the second answer reports the canonical cid")
	assert.Equal(t, firstRow.ID, secondRow.ID, "the second withhold is the existing row")
	assert.Equal(t, "b3-first-operator", secondRow.CreatedBy, "created_by stays the first actor")
	assert.JSONEq(t, jsonOf(t, first), jsonOf(t, second), "the second answer is the existing state")

	inspected := w.inspect(t, cidBase16)
	assert.Len(t, inspected.Withholds, 1, "one row after two withholds")
}

// TestAdminLiftWithoutActiveOperatorWithholdIsRefused (B4): lifting a CID
// with no active operator withhold is a 4xx and leaves its state unchanged.
func TestAdminLiftWithoutActiveOperatorWithholdIsRefused(t *testing.T) {
	cases := []struct {
		name         string
		cid          string
		setup        func(t *testing.T, w *adminWorld, cidStr string)
		wantRows     int
		wantLiftedBy string
	}{
		{
			name:  "no rows",
			cid:   "bafkreiewp2s4da7ezjdsjg5cpddrayi33y4mchvg3idpnqbsmdc7xbyrmy",
			setup: func(*testing.T, *adminWorld, string) {},
		},
		{
			name: "lifted row",
			cid:  "bafkreidzgq55qn7d6ss22yb33jiys4w4vag3fsvf4utvkj34jea22qxwle",
			setup: func(t *testing.T, w *adminWorld, cidStr string) {
				w.operatorWithhold(t, cidStr, "b4-operator")
				_, err := w.store.Lift(context.Background(), cidStr, "b4-first-lifter")
				require.NoError(t, err, "setup: lift")
			},
			wantRows:     1,
			wantLiftedBy: "b4-first-lifter",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdminWorld(t, nil, tc.cid)
			tc.setup(t, w, tc.cid)
			before, err := w.store.Inspect(context.Background(), tc.cid)
			require.NoError(t, err)

			rec := w.call(t, http.MethodPost, liftPath, `{"cid":"`+tc.cid+`","actor":"b4-second-lifter"}`)
			assert4xx(t, rec, "lift with no active operator withhold")

			after := w.inspect(t, tc.cid)
			assert.Equal(t, tc.cid, after.CID)
			require.Len(t, after.Withholds, tc.wantRows)
			if tc.wantRows == 1 {
				row := after.Withholds[0]
				assert.Equal(t, tc.wantLiftedBy, row.LiftedBy, "the lifted row is untouched")
				assert.NotNil(t, row.LiftedAt)
				require.Len(t, before.Withholds, 1)
				assert.JSONEq(t, jsonOf(t, before.Withholds[0]), jsonOf(t, row), "the lifted row is unchanged")
			}
		})
	}
}

// TestAdminLiftRestoresServingAndKeepsTheRow (B5): the withholds listing
// shows the active operator row and not an active non-operator row; lifting
// keeps the row (lifted_at, lifted_by set), drops it from the listing, and
// GetBlob serves the stored bytes again.
func TestAdminLiftRestoresServingAndKeepsTheRow(t *testing.T) {
	const (
		liftCID      = "bafkreiaz2lisg6vpz3iybieeglnhp2aovezfddnkxgjjijtwgid2moutwa"
		communityCID = "bafkreiefbgibbc37dfu46j2kh5s43fpzixzavbg7hxixv3swtxucpe76ai"
	)
	payload := testBytes("B5 lift")
	w := newAdminWorld(t, nil, liftCID, communityCID)
	w.putBlob(t, ownerA, payload, liftCID)
	w.operatorWithhold(t, liftCID, "b5-operator")
	w.communityRemoval(t, communityCID)

	listed := w.listedCIDs(t)
	assert.Contains(t, listed, liftCID, "the active operator withhold is listed")
	assert.NotContains(t, listed, communityCID, "a community-removal row is not an operator withhold")

	lifted := state(t, w.call(t, http.MethodPost, liftPath, `{"cid":"`+liftCID+`","actor":"b5-lifter"}`))
	row := operatorRow(t, lifted)
	assert.NotNil(t, row.LiftedAt, "lifted_at is set")
	assert.Equal(t, "b5-lifter", row.LiftedBy)
	assert.Equal(t, "b5-operator", row.CreatedBy)

	kept := operatorRow(t, w.inspect(t, liftCID))
	assert.NotNil(t, kept.LiftedAt, "the row is kept, inactive")

	assert.NotContains(t, w.listedCIDs(t), liftCID, "a lifted withhold is not listed")

	data, mimeType, err := w.manager.GetBlob(context.Background(), ownerA, liftCID)
	require.NoError(t, err, "a lifted blob is served again")
	assert.Equal(t, payload, data)
	assert.Equal(t, blobMimeType, mimeType)
}

// TestOperatorLiftLeavesCommunityRemovalWithholdInForce (B6): with an
// operator withhold and a community-removal every-owner row on one CID,
// lifting the operator withhold leaves the CID refused: PutBlob for a new
// DID stores nothing and returns the ref with IsBlobWithheld; GetBlob is
// not-found.
func TestOperatorLiftLeavesCommunityRemovalWithholdInForce(t *testing.T) {
	const blobCID = "bafkreiebhemxmkdmfeuuctqkbgmdb37rldvpfhbwflf7dy7novkzm22a3e"
	payload := testBytes("B6 two-source")
	w := newAdminWorld(t, nil, blobCID)
	ctx := context.Background()
	w.putBlob(t, ownerA, payload, blobCID)
	w.operatorWithhold(t, blobCID, "b6-operator")
	w.communityRemoval(t, blobCID)
	_, err := w.store.Lift(ctx, blobCID, "b6-lifter")
	require.NoError(t, err, "setup: operator lift")

	blob, err := w.manager.PutBlob(ctx, ownerNew, blobMimeType, payload)
	require.Error(t, err, "the community-removal row still refuses the store")
	assert.True(t, errors.IsBlobWithheld(err), "want IsBlobWithheld, got %v", err)
	if assert.NotNil(t, blob, "a refused store still returns the ref") {
		assert.Equal(t, blobCID, cid.Cid(blob.Ref).String())
		assert.Equal(t, blobMimeType, blob.MimeType)
		assert.Equal(t, int64(47), blob.Size)
	}
	assert.Equal(t, []string{ownerA}, w.storedOwners(t, blobCID), "nothing stored for the new DID")

	_, _, err = w.manager.GetBlob(ctx, ownerA, blobCID)
	assert.True(t, errors.IsNotFound(err), "still withheld, want not-found, got %v", err)
}

// TestSubjectScopeWithholdCoversOnlyItsOwner (B6 revision): a static
// subject-scope row for owner A covers (A, CID) and not (B, CID).
func TestSubjectScopeWithholdCoversOnlyItsOwner(t *testing.T) {
	const blobCID = "bafkreigjeautfi3rmnztid7lujymwmlq4gpnalmu2yasmv2fi6lbjbuexu"
	payload := testBytes("B6 subject-scope")
	w := newAdminWorld(t, nil, blobCID)
	ctx := context.Background()
	_, err := w.store.Insert(ctx, blobwithhold.Withhold{
		CID:          blobCID,
		Source:       blobwithhold.SourceCommunityRemoval,
		Scope:        blobwithhold.ScopeSubject,
		OwnerDID:     ownerA,
		SubjectURI:   "at://" + ownerA + "/social.coves.community.post/b6subject",
		CommunityDID: communityDID,
		ReasonCode:   "moderator-discretion",
	})
	require.NoError(t, err, "setup: subject-scope row")

	coveredA, err := w.store.Covers(ctx, ownerA, blobCID)
	require.NoError(t, err)
	assert.True(t, coveredA, "the subject row covers its owner")
	coveredB, err := w.store.Covers(ctx, ownerB, blobCID)
	require.NoError(t, err)
	assert.False(t, coveredB, "the subject row does not cover another owner")

	blob, err := w.manager.PutBlob(ctx, ownerB, blobMimeType, payload)
	require.NoError(t, err, "another owner may store the bytes")
	assert.Equal(t, blobCID, cid.Cid(blob.Ref).String())
	_, err = w.manager.PutBlob(ctx, ownerA, blobMimeType, payload)
	assert.True(t, errors.IsBlobWithheld(err), "the subject's owner is refused, got %v", err)
	assert.Equal(t, []string{ownerB}, w.storedOwners(t, blobCID))
}

// TestAdminDeleteBytesRefusedWithoutActiveOperatorWithholdOrConfirm (B7):
// delete-bytes is a 4xx and every owner keeps the bytes when there is no
// active operator withhold (none, community-removal only, lifted only), or
// when the withhold is active but confirm is absent or false.
func TestAdminDeleteBytesRefusedWithoutActiveOperatorWithholdOrConfirm(t *testing.T) {
	withhold := func(t *testing.T, w *adminWorld, cidStr string) { w.operatorWithhold(t, cidStr, "b7-operator") }
	cases := []struct {
		name    string
		cid     string
		setup   func(t *testing.T, w *adminWorld, cidStr string)
		confirm string // the JSON fragment after actor, empty for absent
	}{
		{"no withhold", "bafkreidst6aedhasadobqfj3oldwhzlpsfbenj73qdixmaslk5vetc7odi",
			func(*testing.T, *adminWorld, string) {}, `,"confirm":true`},
		{"confirm absent", "bafkreih7563nnmjylu4kyxtrfe52q2bhwfij53f2ptckvivsqeji3ezn3m",
			withhold, ``},
		{"confirm false", "bafkreifppx2oaqexhdeaxj7ljiwkwynaswl2tgraalbrozu5hibuwtfjom",
			withhold, `,"confirm":false`},
		{"community-removal only", "bafkreiaeer7f6mvfif2thx56vjt3erf3chauwz7cvf5zz2qwjlima74paa",
			func(t *testing.T, w *adminWorld, cidStr string) { w.communityRemoval(t, cidStr) }, `,"confirm":true`},
		{"lifted only", "bafkreigmupgxp2mrz7ziz3vhbwknzecjvncljasotu7bjbftfawnap5nca",
			func(t *testing.T, w *adminWorld, cidStr string) {
				w.operatorWithhold(t, cidStr, "b7-operator")
				_, err := w.store.Lift(context.Background(), cidStr, "b7-lifter")
				require.NoError(t, err, "setup: lift")
			}, `,"confirm":true`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := testBytes("B7 " + strings.ReplaceAll(tc.name, " ", "-"))
			w := newAdminWorld(t, nil, tc.cid)
			w.putBlob(t, ownerA, payload, tc.cid)
			w.putBlob(t, ownerB, payload, tc.cid)
			tc.setup(t, w, tc.cid)

			rec := w.call(t, http.MethodPost, deletePath, `{"cid":"`+tc.cid+`","actor":"b7-deleter"`+tc.confirm+`}`)
			assert4xx(t, rec, "refused delete-bytes")
			assert.Equal(t, []string{ownerA, ownerB}, w.storedOwners(t, tc.cid), "every owner keeps the bytes")

			after := w.inspect(t, tc.cid)
			assert.False(t, after.BytesDeleted)
			assert.Equal(t, 2, after.StoredOwners)
		})
	}
}

// TestAdminDeleteBytesRemovesEveryOwnersBytes (B8): with an active operator
// withhold and confirm, delete-bytes removes the blobs rows for every owner,
// sets deleted_at, keeps the withhold active and reports bytes_deleted with
// no stored owners.
func TestAdminDeleteBytesRemovesEveryOwnersBytes(t *testing.T) {
	const blobCID = "bafkreia6g7ry3yisovaid4hc7whljhcnhqrop2rwfrmawyleqfkczmbhqm"
	payload := testBytes("B8 delete-bytes")
	w := newAdminWorld(t, nil, blobCID)
	w.putBlob(t, ownerA, payload, blobCID)
	w.putBlob(t, ownerB, payload, blobCID)
	w.operatorWithhold(t, blobCID, "b8-operator")

	deleted := state(t, w.call(t, http.MethodPost, deletePath,
		`{"cid":"`+blobCID+`","actor":"b8-deleter","confirm":true}`))
	assert.True(t, deleted.BytesDeleted)
	assert.Equal(t, 0, deleted.StoredOwners)
	row := operatorRow(t, deleted)
	assert.Nil(t, row.LiftedAt, "the withhold stays active")
	assert.NotNil(t, row.DeletedAt, "deleted_at is set")
	assert.Equal(t, "b8-deleter", row.BytesDeletedBy)

	assert.Empty(t, w.storedOwners(t, blobCID), "no owner keeps the bytes")
	inspected := w.inspect(t, blobCID)
	assert.True(t, inspected.BytesDeleted)
	assert.Equal(t, 0, inspected.StoredOwners)
}

// TestAdminAuditTrailKeepsEachActorsLabel (B9): Alice withholds, Bob
// deletes bytes, Dave's second delete changes nothing, Carol lifts; each
// label and timestamp survives the later actions, in order, and the
// moderation_action_id and csam_case_id persist.
func TestAdminAuditTrailKeepsEachActorsLabel(t *testing.T) {
	const blobCID = "bafkreif6o4cnnvgr5znbh2ylmswxydchbdzwhzccpvcnssalumxjc7j4ha"
	w := newAdminWorld(t, nil, blobCID)
	w.putBlob(t, ownerA, testBytes("B9 audit-trail"), blobCID)

	withheld := operatorRow(t, state(t, w.call(t, http.MethodPost, withholdPath,
		`{"cid":"`+blobCID+`","reason_code":"csam","actor":"alice",`+
			`"moderation_action_id":"b9-modaction-17","csam_case_id":"b9-case-42"}`)))
	assert.Equal(t, "alice", withheld.CreatedBy)
	assert.Equal(t, blobwithhold.ReasonCSAM, withheld.ReasonCode)
	require.False(t, withheld.CreatedAt.IsZero(), "created_at is set")

	deleted := operatorRow(t, state(t, w.call(t, http.MethodPost, deletePath,
		`{"cid":"`+blobCID+`","actor":"bob","confirm":true}`)))
	assert.Equal(t, "alice", deleted.CreatedBy)
	assert.Equal(t, "bob", deleted.BytesDeletedBy)
	require.NotNil(t, deleted.DeletedAt, "deleted_at is set")
	bobDeletedAt := *deleted.DeletedAt

	w.call(t, http.MethodPost, deletePath, `{"cid":"`+blobCID+`","actor":"dave","confirm":true}`)
	afterDave := operatorRow(t, w.inspect(t, blobCID))
	assert.Equal(t, "bob", afterDave.BytesDeletedBy, "a second delete does not replace Bob")
	if assert.NotNil(t, afterDave.DeletedAt) {
		assert.True(t, bobDeletedAt.Equal(*afterDave.DeletedAt), "a second delete does not move deleted_at")
	}

	lifted := operatorRow(t, state(t, w.call(t, http.MethodPost, liftPath, `{"cid":"`+blobCID+`","actor":"carol"}`)))
	assert.Equal(t, "carol", lifted.LiftedBy)

	final := operatorRow(t, w.inspect(t, blobCID))
	assert.Equal(t, "alice", final.CreatedBy)
	assert.True(t, withheld.CreatedAt.Equal(final.CreatedAt), "created_at unchanged")
	assert.Equal(t, "bob", final.BytesDeletedBy)
	assert.Equal(t, "carol", final.LiftedBy)
	assert.Equal(t, "b9-modaction-17", final.ModerationActionID)
	assert.Equal(t, "b9-case-42", final.CSAMCaseID)
	require.NotNil(t, final.DeletedAt)
	require.NotNil(t, final.LiftedAt)
	assert.True(t, bobDeletedAt.Equal(*final.DeletedAt), "deleted_at unchanged by the lift")
	assert.False(t, final.DeletedAt.Before(final.CreatedAt), "created_at <= deleted_at")
	assert.False(t, final.LiftedAt.Before(*final.DeletedAt), "deleted_at <= lifted_at")
}

// TestAdminRoutesRequireBearer (B13): every admin route answers 401 with no
// bearer and with a wrong one.
func TestAdminRoutesRequireBearer(t *testing.T) {
	const blobCID = "bafkreihgk3yyzgryf6vr7lthc3m5m2y2dbum7uyyznr3agul2x77ltu5cy"
	w := newAdminWorld(t, nil, blobCID)
	routes := []struct{ name, method, path, body string }{
		{"withhold", http.MethodPost, withholdPath, `{"cid":"` + blobCID + `","reason_code":"csam","actor":"b13"}`},
		{"lift", http.MethodPost, liftPath, `{"cid":"` + blobCID + `","actor":"b13"}`},
		{"delete-bytes", http.MethodPost, deletePath, `{"cid":"` + blobCID + `","actor":"b13","confirm":true}`},
		{"inspect", http.MethodGet, statePath + blobCID, ""},
		{"list", http.MethodGet, withholdsPath, ""},
	}
	for _, route := range routes {
		for _, auth := range []struct{ name, token string }{{"no token", ""}, {"wrong token", "not-the-admin-token"}} {
			t.Run(route.name+"/"+auth.name, func(t *testing.T) {
				rec := adminRequest(t, w.router, route.method, route.path, auth.token, route.body)
				assert.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
			})
		}
	}
}

// TestAdminMutationLogsCarryCanonicalCIDButNoSecrets (B15): the log lines of
// withhold, delete-bytes and lift name the canonical cid (the request gave
// base58btc) and carry neither the bearer token nor the request's actor,
// moderation_action_id or csam_case_id text.
func TestAdminMutationLogsCarryCanonicalCIDButNoSecrets(t *testing.T) {
	const (
		cidBase32    = "bafkreihk4wdmwn4ulohba54nm6sm562gfazmtfzopq2go27borlfdirnti"
		cidBase58BTC = "zb2rhnTDb7X3JBYZiY7cEqCoBE4fAWcFd2pxEK43GXNtXPgjj"
		actor        = "b15-actor-label-qx7"
		moderationID = "b15-modaction-qx7"
		caseID       = "b15-case-qx7"
	)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newAdminWorld(t, logger, cidBase32)
	w.putBlob(t, ownerA, testBytes("B15 logging"), cidBase32)

	recs := map[string]*httptest.ResponseRecorder{
		"withhold": w.call(t, http.MethodPost, withholdPath,
			`{"cid":"`+cidBase58BTC+`","reason_code":"csam","actor":"`+actor+`",`+
				`"moderation_action_id":"`+moderationID+`","csam_case_id":"`+caseID+`"}`),
	}
	recs["delete-bytes"] = w.call(t, http.MethodPost, deletePath,
		`{"cid":"`+cidBase58BTC+`","actor":"`+actor+`","confirm":true}`)
	recs["lift"] = w.call(t, http.MethodPost, liftPath, `{"cid":"`+cidBase58BTC+`","actor":"`+actor+`"}`)

	logged := logs.String()
	assert.Contains(t, logged, cidBase32, "mutation logs name the canonical cid")
	assert.NotContains(t, logged, adminToken, "logs must not carry the bearer token")
	assert.NotContains(t, logged, actor, "logs must not carry the actor label")
	assert.NotContains(t, logged, moderationID, "logs must not carry the moderation_action_id")
	assert.NotContains(t, logged, caseID, "logs must not carry the csam_case_id")
	for name, rec := range recs {
		assert.Equal(t, http.StatusOK, rec.Code, "%s: body %s", name, rec.Body.String())
	}
}
