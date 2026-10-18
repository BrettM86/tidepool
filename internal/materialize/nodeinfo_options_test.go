package materialize

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

func TestNewRequiresNodeInfoDependencies(t *testing.T) {
	// No database: the store constructors only hold the handle, and New
	// never touches it.
	validOptions := func() Options {
		return Options{
			Fetcher: ap.NewClient(ap.ClientOptions{
				HTTPClient: &http.Client{Transport: rewriteTransport{target: "127.0.0.1:1"}},
			}),
			Objects:          store.NewAPObjects(nil),
			Actors:           store.NewBridgedActors(nil),
			Communities:      store.NewCommunities(nil),
			Repos:            &repo.Manager{},
			Minter:           &fakeMinter{},
			ServiceDID:       testServiceDID,
			StrictValidation: true,
			Instances:        store.NewInstances(nil),
			NodeInfoClient:   &http.Client{Transport: rewriteTransport{target: "127.0.0.1:1"}},
			UserAgent:        "tidepool-test/1.0",
		}
	}

	tests := []struct {
		name           string
		mutate         func(*Options)
		wantValidation bool
	}{
		// Control: proves every other option is valid, so the rows below
		// fail on the nodeinfo dependency alone.
		{name: "all dependencies set", mutate: func(*Options) {}, wantValidation: false},
		{name: "nil NodeInfoClient", mutate: func(o *Options) { o.NodeInfoClient = nil }, wantValidation: true},
		{name: "nil Instances", mutate: func(o *Options) { o.Instances = nil }, wantValidation: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := validOptions()
			tt.mutate(&opts)

			m, err := New(opts)

			if !tt.wantValidation {
				require.NoError(t, err)
				assert.NotNil(t, m)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "want a validation error, got %v", err)
			assert.Nil(t, m)
		})
	}
}
