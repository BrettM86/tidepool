package handlemigration

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// TestOriginGaveNoAnswer builds each error the way ap.Client's
// FetchActorSameAuthority path returns it: transport failures from
// http.Client.Do arrive as a *url.Error wrapped in "ap: GET <target>: %w",
// statuses arrive as a bare ap.HTTPError or a mapped sentinel, and body read
// failures arrive wrapped twice (readBody's "read response: %w", then the GET
// prefix).
func TestOriginGaveNoAnswer(t *testing.T) {
	const target = "https://x.example/u/alice"
	getFailed := func(err error) error { return fmt.Errorf("ap: GET %s: %w", target, err) }
	doFailed := func(err error) error { return getFailed(&url.Error{Op: "Get", URL: target, Err: err}) }

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "HTTP 500", err: ap.HTTPError{URL: target, StatusCode: 500}, want: true},
		{name: "HTTP 503", err: ap.HTTPError{URL: target, StatusCode: 503}, want: true},
		{name: "HTTP 429", err: ap.HTTPError{URL: target, StatusCode: 429}, want: true},
		{name: "401/403/404 mapped to not found", err: errors.NewNotFoundError("ap_object", target), want: true},
		{name: "deadline exceeded", err: getFailed(context.DeadlineExceeded), want: true},
		{name: "connection refused", err: doFailed(&net.OpError{Op: "dial", Net: "tcp",
			Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}), want: true},
		{name: "DNS failure", err: doFailed(&net.DNSError{Err: "no such host", Name: "x.example", IsNotFound: true}),
			want: true},
		{name: "body cut short", err: getFailed(fmt.Errorf("read response: %w", io.ErrUnexpectedEOF)), want: true},

		{name: "tombstoned", err: errors.NewTombstonedError("ap_object", target)},
		// A pinned-authority redirect refusal is returned bare by getOnce.
		{name: "cross-authority redirect refused",
			err: errors.NewValidationError("redirect", "redirect leaves the authority")},
		// An https-to-http downgrade refusal is wrapped in the GET prefix.
		{name: "redirect downgrade refused",
			err: getFailed(errors.NewValidationError("redirect", "refusing https to http redirect"))},
		// The egress guard refuses in the dialer, so http.Client.Do returns it
		// inside a *url.Error, which itself satisfies net.Error.
		{name: "egress guard refusal inside url.Error",
			err: doFailed(errors.NewValidationError("url", "host resolves to a blocked address"))},
		{name: "HTTP 400", err: ap.HTTPError{URL: target, StatusCode: 400}},
		{name: "decode failure", err: stderrors.New("decode actor")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, originGaveNoAnswer(tc.err))
		})
	}
}
