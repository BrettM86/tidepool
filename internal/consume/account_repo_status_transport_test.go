package consume

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B13 (chunk 04): RepoStatus is the getRepoStatus read AccountStatus already
// makes, returning the status itself so the #account handler can confirm a
// takendown/suspended restriction and its lift. Same pdsWorld fixture: the
// DID document names the PDS, the PDS answers getRepoStatus.

// resolverAllowingInsecure is pdsWorld.resolver with the development
// allowance for an http status endpoint set to allow.
func (w *pdsWorld) resolverAllowingInsecure(t *testing.T, allow bool) *HandleResolver {
	t.Helper()
	resolver, err := NewHandleResolver(ResolverOptions{
		PLCDirectoryURL:             w.server.URL,
		HTTPClient:                  &http.Client{Transport: &asRecordingTransport{world: w}},
		UserAgent:                   "tidepool-test/0.1",
		LookupTXT:                   func(context.Context, string) ([]string, error) { return nil, nil },
		AllowInsecureStatusEndpoint: allow,
	})
	require.NoError(t, err)
	return resolver
}

func TestRepoStatus_ReturnsTheStatusThePDSReports(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want RepoStatus
	}{
		{"takendown", `{"did":"` + asDID + `","active":false,"status":"takendown"}`,
			RepoStatus{Active: false, Status: "takendown"}},
		{"suspended", `{"did":"` + asDID + `","active":false,"status":"suspended"}`,
			RepoStatus{Active: false, Status: "suspended"}},
		{"active", `{"did":"` + asDID + `","active":true}`,
			RepoStatus{Active: true, Status: ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			world.body[asPDSHost] = tc.body

			got, err := world.resolver(t).RepoStatus(context.Background(), asDID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, []asAttempt{{scheme: "https", host: asPDSHost, path: asStatusPath}},
				world.statusAttempts(), "the status comes from the PDS the document names, over https")
		})
	}
}

func TestRepoStatus_ANon200AnswerIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"500", http.StatusInternalServerError, `{"active":false,"status":"takendown"}`},
		{"400 on an unrecognised repo", http.StatusBadRequest, `{"error":"RepoNotFound"}`},
		{"404", http.StatusNotFound, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			world.status[asPDSHost] = tc.status
			world.body[asPDSHost] = tc.body

			got, err := world.resolver(t).RepoStatus(context.Background(), asDID)
			require.Error(t, err, "a non-200 answer is no confirmation of any status")
			assert.Equal(t, RepoStatus{}, got)
		})
	}
}

// TestRepoStatus_AnHTTPEndpointIsReadOnlyUnderTheDevelopmentAllowance: an http
// PDS endpoint is refused before any request unless
// ResolverOptions.AllowInsecureStatusEndpoint is set, and then it is read.
func TestRepoStatus_AnHTTPEndpointIsReadOnlyUnderTheDevelopmentAllowance(t *testing.T) {
	for _, tc := range []struct {
		name         string
		allow        bool
		wantErr      bool
		want         RepoStatus
		wantAttempts []asAttempt
	}{
		{name: "refused without the allowance", allow: false, wantErr: true,
			want: RepoStatus{}, wantAttempts: nil},
		{name: "read under the allowance", allow: true, wantErr: false,
			want:         RepoStatus{Active: false, Status: "takendown"},
			wantAttempts: []asAttempt{{scheme: "http", host: asPDSHost, path: asStatusPath}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			world.endpoint = "http://" + asPDSHost
			world.body[asPDSHost] = `{"did":"` + asDID + `","active":false,"status":"takendown"}`

			got, err := world.resolverAllowingInsecure(t, tc.allow).RepoStatus(context.Background(), asDID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantAttempts, world.statusAttempts())
		})
	}
}

// TestRepoStatus_TheDevelopmentAllowanceWaivesOnlyTheScheme: an http endpoint
// admitted under the allowance still has to pass the host, userinfo and
// query/fragment checks, and one that fails them is refused before any request.
func TestRepoStatus_TheDevelopmentAllowanceWaivesOnlyTheScheme(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{"missing host", "http:///xrpc"},
		{"userinfo", "http://user:pw@" + asPDSHost},
		{"query", "http://" + asPDSHost + "?x=1"},
		{"fragment", "http://" + asPDSHost + "#f"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			world.endpoint = tc.endpoint
			world.body[asPDSHost] = `{"did":"` + asDID + `","active":false,"status":"takendown"}`

			got, err := world.resolverAllowingInsecure(t, true).RepoStatus(context.Background(), asDID)
			require.Error(t, err, "the allowance waives the scheme only")
			assert.Equal(t, RepoStatus{}, got)
			assert.Empty(t, world.statusAttempts(), "refused before any request")
		})
	}
}

// TestAccountStatus_StaysHTTPSOnlyUnderTheDevelopmentAllowance: the allowance
// is RepoStatus's alone; the terminal deleted path still refuses an http
// endpoint before any request.
func TestAccountStatus_StaysHTTPSOnlyUnderTheDevelopmentAllowance(t *testing.T) {
	world := newPDSWorld(t)
	world.endpoint = "http://" + asPDSHost
	world.body[asPDSHost] = `{"active":false,"status":"deleted"}`

	deleted, err := world.resolverAllowingInsecure(t, true).AccountStatus(context.Background(), asDID)
	require.Error(t, err, "the deleted verdict is never read over http")
	assert.False(t, deleted)
	assert.Empty(t, world.statusAttempts(), "refused before any request")
}
