package e2e

// No build tag: covesSignIn is client logic, a fixed sequence of HTTP
// exchanges, tested here against two httptest fakes (the AppView's OAuth
// routes and the PDS's authorization server) with no Docker and no network
// beyond loopback. TestCovesModeration_RemovalAndBanRecordsReachCovesViews
// drives it against the real stack.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// fakeIssuer is where the fake PDS claims to live, as the stack's PDS does
	// (its DID documents and OAuth metadata name http://localhost:3001). The
	// helper must keep this host in URL, Host and Origin and redirect only the
	// dial, through DialOverrides, to the fake's real listener.
	fakeIssuer     = "http://localhost:3001"
	fakeIssuerHost = "localhost:3001"

	fakeHandle   = "alice.pds.test"
	fakeDID      = "did:plc:fakecovessigninalice"
	fakeClientID = "http://127.0.0.1:8093/oauth/client-metadata.json"

	// The secrets. Each carries SENTINEL so that any fragment of one in an
	// error is unambiguous, and characters that change under query escaping
	// or Go quoting (space, &, +, /, ") so a scrubber that knows only one form
	// misses the others. Cookie values cannot hold a space or a double quote.
	fakePassword   = `pass word&+/"SENTINEL-1a`
	fakeRequestURI = "urn:ietf:params:oauth:request_uri:req+SENTINEL/2b &"
	fakeCode       = "code+SENTINEL/3c &x"
	fakeState      = `state&SENTINEL+4d/ "y`
	fakeEphemeral  = `ephemeral SENTINEL+5e/&"`
	fakeBinding    = "binding+SENTINEL/6f&"
	fakeDeviceID   = "device+SENTINEL/7a&"
	fakeSessionID  = "session+SENTINEL/8b&"
	fakeRotatedID  = "rotated+SENTINEL/9c&"
	// 24 characters, the length @atproto/oauth-provider accepts.
	fakeCSRF          = "csrf+SENTINEL/0123456789"
	fakeRefusalCookie = "refusal+SENTINEL/0d&"
	fakeSealedSession = "sealed+SENTINEL/1e&"
	// Secrets shaped like an OAuth error code (a real atproto code is
	// cod-<hex>), so covesSanitizeOAuthError repeats them and only the
	// scrubber can keep them out of an error.
	fakeErrorShapedCode    = "cod-SENTINEL3c"
	fakeErrorShapedBinding = "binding-SENTINEL6f"
	fakeErrorShapedState   = "state-SENTINEL4d"
	// fakeUnseenLeak is in a malformed Location the flow never parses, so it
	// is never remembered: only covesLocationGuard can keep it out.
	fakeUnseenLeak = "unseenSENTINEL"
	// fakeOffLoopbackHost is a TEST-NET-1 address (RFC 5737): not loopback,
	// and unroutable should a bug ever dial it.
	fakeOffLoopbackHost = "192.0.2.1:3001"
	fakeProviderPrefix  = "/@atproto/oauth-provider/~api"
)

// fakeSecrets is every secret the flow can handle.
var fakeSecrets = []string{
	fakePassword, fakeRequestURI, fakeCode, fakeState, fakeEphemeral, fakeBinding,
	fakeDeviceID, fakeSessionID, fakeRotatedID, fakeCSRF, fakeRefusalCookie, fakeSealedSession,
	fakeErrorShapedCode, fakeErrorShapedBinding, fakeErrorShapedState,
}

// fakeSignInFailure names the hop that goes wrong, and how.
type fakeSignInFailure int

const (
	fakeSignInSucceeds fakeSignInFailure = iota
	// /oauth/login answers a redirect whose Location is not a valid URL and
	// carries the password; net/http quotes such a header in its own error.
	fakeLoginMalformedLocation
	// /oauth/login sets its binding cookie and bounces to the login page with
	// that cookie's value as the error code.
	fakeLoginBouncesWithCookie
	// As fakeLoginBouncesWithCookie, with a binding cookie value shaped like an
	// OAuth error code.
	fakeLoginBouncesWithErrorShapedCookie
	// /oauth/login redirects to an authorize URL on a non-loopback host.
	fakeLoginRedirectsOffLoopback
	// The authorize page refuses with a 400 that sets a cookie and echoes it
	// and the request_uri in an OAuth error body.
	fakeAuthorizeRefusesEchoing
	// Sign-in refuses with a 401 whose error code is the password and whose
	// description echoes the password raw, query-escaped and Go-quoted, and
	// the session cookie.
	fakeSignInRefusesEchoing
	// Consent refuses with a 400 echoing the ephemeral token and the rotated
	// session cookie.
	fakeConsentRefusesEchoing
	// Consent answers a redirect url with code and state but no iss.
	fakeConsentOmitsIss
	// Consent answers a redirect url on an origin other than the AppView's.
	fakeConsentWrongOrigin
	// Consent answers a redirect url on the AppView's origin but not its
	// callback path.
	fakeConsentWrongPath
	// Consent answers a redirect url whose iss is not the issuer.
	fakeConsentWrongIss
	// Consent answers a url that is not the issuer's /oauth/authorize/redirect.
	fakeConsentWrongRedirectEndpoint
	// Consent answers a redirect_mode other than query.
	fakeConsentWrongRedirectMode
	// The callback bounces to the login page with the code as its error and
	// the state in the query, and echoes both in its body.
	fakeCallbackBouncesEchoing
	// As fakeCallbackBouncesEchoing, with an issued code shaped like an OAuth
	// error code.
	fakeCallbackBouncesWithErrorShapedCode
	// The callback bounces to the login page with the state as its error,
	// for an issued state shaped like an OAuth error code.
	fakeCallbackBouncesWithErrorShapedState
	// The callback drops the connection; Go's *url.Error quotes the request
	// URL, code and state included.
	fakeCallbackDropsConnection
)

// fakeOAuthPair is an AppView's OAuth routes and a PDS's authorization server
// wired to each other. Each answers 4xx naming the requirement a request
// broke, so a helper that gets a hop wrong never reaches the next one.
type fakeOAuthPair struct {
	appView *httptest.Server
	pds     *httptest.Server
	failure fakeSignInFailure

	mu   sync.Mutex
	hops []string
}

func newFakeOAuthPair(t *testing.T, failure fakeSignInFailure) *fakeOAuthPair {
	t.Helper()
	pair := &fakeOAuthPair{failure: failure}
	pair.pds = httptest.NewServer(http.HandlerFunc(pair.servePDS))
	t.Cleanup(pair.pds.Close)
	pair.appView = httptest.NewServer(http.HandlerFunc(pair.serveAppView))
	t.Cleanup(pair.appView.Close)
	return pair
}

func (p *fakeOAuthPair) record(hop string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hops = append(p.hops, hop)
}

func (p *fakeOAuthPair) lastHop() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.hops) == 0 {
		return ""
	}
	return p.hops[len(p.hops)-1]
}

func fakeAuthorizeURL() string {
	return fakeIssuer + "/oauth/authorize?" + url.Values{
		"client_id": {fakeClientID}, "request_uri": {fakeRequestURI},
	}.Encode()
}

// code is the authorization code consent issues and the callback accepts.
func (p *fakeOAuthPair) code() string {
	if p.failure == fakeCallbackBouncesWithErrorShapedCode {
		return fakeErrorShapedCode
	}
	return fakeCode
}

// state is the state the consent redirect issues and the callback accepts.
func (p *fakeOAuthPair) state() string {
	if p.failure == fakeCallbackBouncesWithErrorShapedState {
		return fakeErrorShapedState
	}
	return fakeState
}

// binding is the oauth_web_binding cookie value /oauth/login sets and the
// callback accepts.
func (p *fakeOAuthPair) binding() string {
	if p.failure == fakeLoginBouncesWithErrorShapedCookie {
		return fakeErrorShapedBinding
	}
	return fakeBinding
}

func fakeRefuse(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

func fakeProviderError(w http.ResponseWriter, status int, envelope map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope)
}

func (p *fakeOAuthPair) serveAppView(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/oauth/login":
		p.record("appview login")
		if got := r.URL.Query().Get("handle"); got != fakeHandle {
			fakeRefuse(w, http.StatusBadRequest, "appview login: handle = "+strconv.Quote(got))
			return
		}
		if p.failure == fakeLoginMalformedLocation {
			// Written by hand: http.Redirect would clean the URL up.
			w.Header().Set("Location", "/%zz?pw="+fakePassword+"&leak="+fakeUnseenLeak)
			w.WriteHeader(http.StatusFound)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "oauth_web_binding", Value: p.binding(), Path: "/oauth", HttpOnly: true})
		switch p.failure {
		case fakeLoginBouncesWithCookie, fakeLoginBouncesWithErrorShapedCookie:
			http.Redirect(w, r, "/login?error="+url.QueryEscape(p.binding()), http.StatusFound)
			return
		case fakeLoginRedirectsOffLoopback:
			http.Redirect(w, r, "http://"+fakeOffLoopbackHost+"/oauth/authorize?"+url.Values{
				"client_id": {fakeClientID}, "request_uri": {fakeRequestURI},
			}.Encode(), http.StatusFound)
			return
		}
		http.Redirect(w, r, fakeAuthorizeURL(), http.StatusFound)

	case "/oauth/callback":
		p.record("appview callback")
		binding, err := r.Cookie("oauth_web_binding")
		if err != nil || binding.Value != p.binding() {
			fakeRefuse(w, http.StatusBadRequest, "appview callback: missing or wrong oauth_web_binding cookie")
			return
		}
		query := r.URL.Query()
		if query.Get("code") != p.code() || query.Get("state") != p.state() || query.Get("iss") != fakeIssuer {
			fakeRefuse(w, http.StatusBadRequest, "appview callback: wrong code, state or iss")
			return
		}
		switch p.failure {
		case fakeCallbackBouncesEchoing, fakeCallbackBouncesWithErrorShapedCode:
			w.Header().Set("Location", "/login?"+url.Values{"error": {p.code()}, "state": {p.state()}}.Encode())
			w.WriteHeader(http.StatusFound)
			_, _ = w.Write([]byte("callback failed for code " + p.code() + " state " + p.state() + " password " + fakePassword))
			return
		case fakeCallbackBouncesWithErrorShapedState:
			w.Header().Set("Location", "/login?"+url.Values{"error": {p.state()}}.Encode())
			w.WriteHeader(http.StatusFound)
			return
		case fakeCallbackDropsConnection:
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				fakeRefuse(w, http.StatusInternalServerError, "appview callback: cannot hijack")
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				fakeRefuse(w, http.StatusInternalServerError, "appview callback: hijack failed")
				return
			}
			_ = connection.Close()
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "coves_session", Value: fakeSealedSession, Path: "/", HttpOnly: true})
		http.Redirect(w, r, "/", http.StatusFound)

	default:
		fakeRefuse(w, http.StatusNotFound, "appview: no route for "+r.URL.Path)
	}
}

// fromAuthorizePage reports whether a provider API call looks like the
// sign-in page's own script made it; when it does not, the refusal is written.
func fromAuthorizePage(w http.ResponseWriter, r *http.Request) bool {
	switch {
	case r.Host != fakeIssuerHost:
		fakeRefuse(w, http.StatusMisdirectedRequest, "pds api: Host = "+strconv.Quote(r.Host)+", want the issuer host")
	case r.Header.Get("Origin") != fakeIssuer:
		fakeRefuse(w, http.StatusBadRequest, "pds api: Origin is not the issuer")
	case r.Header.Get("Referer") != fakeAuthorizeURL():
		fakeRefuse(w, http.StatusBadRequest, "pds api: Referer is not the authorize page")
	case r.Header.Get("Sec-Fetch-Mode") != "same-origin" || r.Header.Get("Sec-Fetch-Site") != "same-origin":
		fakeRefuse(w, http.StatusBadRequest, "pds api: fetch metadata is not same-origin")
	case r.Header.Get("X-CSRF-Token") != fakeCSRF:
		fakeRefuse(w, http.StatusBadRequest, "pds api: X-CSRF-Token does not match the csrf-token cookie")
	default:
		csrf, csrfErr := r.Cookie("csrf-token")
		device, deviceErr := r.Cookie("dev-id")
		if csrfErr != nil || csrf.Value != fakeCSRF || deviceErr != nil || device.Value != fakeDeviceID {
			fakeRefuse(w, http.StatusBadRequest, "pds api: missing or wrong csrf-token or dev-id cookie")
			return false
		}
		return true
	}
	return false
}

func (p *fakeOAuthPair) servePDS(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/oauth/authorize":
		p.record("pds authorize")
		if r.Host != fakeIssuerHost {
			fakeRefuse(w, http.StatusMisdirectedRequest, "pds authorize: Host = "+strconv.Quote(r.Host)+", want the issuer host")
			return
		}
		if r.Header.Get("Sec-Fetch-Mode") != "navigate" || r.Header.Get("Sec-Fetch-Dest") != "document" {
			fakeRefuse(w, http.StatusBadRequest, "pds authorize: not a top-level navigation")
			return
		}
		if r.URL.Query().Get("request_uri") != fakeRequestURI {
			fakeRefuse(w, http.StatusBadRequest, "pds authorize: wrong request_uri")
			return
		}
		if p.failure == fakeAuthorizeRefusesEchoing {
			http.SetCookie(w, &http.Cookie{Name: "dev-id", Value: fakeRefusalCookie, Path: "/", HttpOnly: true})
			fakeProviderError(w, http.StatusBadRequest, map[string]string{
				"error":             "invalid_request",
				"error_description": "device " + fakeRefusalCookie + " cannot use request " + fakeRequestURI,
			})
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "dev-id", Value: fakeDeviceID, Path: "/", HttpOnly: true})
		http.SetCookie(w, &http.Cookie{Name: "ses-id", Value: fakeSessionID, Path: "/", HttpOnly: true})
		// Secure, as the real provider marks it, over plain http.
		http.SetCookie(w, &http.Cookie{Name: "csrf-token", Value: fakeCSRF, Path: "/", Secure: true})
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>sign in</body></html>"))

	case r.Method == http.MethodPost && r.URL.Path == fakeProviderPrefix+"/sign-in":
		p.record("pds sign-in")
		if !fromAuthorizePage(w, r) {
			return
		}
		if session, err := r.Cookie("ses-id"); err != nil || session.Value != fakeSessionID {
			fakeRefuse(w, http.StatusBadRequest, "pds sign-in: missing or wrong ses-id cookie")
			return
		}
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username != fakeHandle || body.Password != fakePassword {
			fakeRefuse(w, http.StatusBadRequest, "pds sign-in: body is not the account's username and password")
			return
		}
		if p.failure == fakeSignInRefusesEchoing {
			fakeProviderError(w, http.StatusUnauthorized, map[string]string{
				"error": body.Password,
				"error_description": "Invalid password " + body.Password + " (" + url.QueryEscape(body.Password) +
					") " + strconv.Quote(body.Password) + " for session " + fakeSessionID,
			})
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "ses-id", Value: fakeRotatedID, Path: "/", HttpOnly: true})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"account":        map[string]string{"did": fakeDID},
			"ephemeralToken": fakeEphemeral,
		})

	case r.Method == http.MethodPost && r.URL.Path == fakeProviderPrefix+"/consent":
		p.record("pds consent")
		if !fromAuthorizePage(w, r) {
			return
		}
		if session, err := r.Cookie("ses-id"); err != nil || session.Value != fakeRotatedID {
			fakeRefuse(w, http.StatusBadRequest, "pds consent: ses-id cookie is not the one sign-in rotated to")
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+fakeEphemeral {
			fakeRefuse(w, http.StatusUnauthorized, "pds consent: Authorization is not the ephemeral token")
			return
		}
		switch p.failure {
		case fakeConsentRefusesEchoing:
			fakeProviderError(w, http.StatusBadRequest, map[string]string{
				"error":             "access_denied",
				"error_description": "token " + fakeEphemeral + " and session " + fakeRotatedID + " refused",
			})
			return
		}
		// As @atproto/oauth-provider does: the url is the issuer's
		// /oauth/authorize/redirect, whose redirect_uri and redirect_mode say
		// where and how the browser is then sent.
		redirect := fakeIssuer + "/oauth/authorize/redirect"
		callback := p.appView.URL + "/oauth/callback"
		params := url.Values{"redirect_mode": {"query"}, "code": {p.code()}, "state": {p.state()}, "iss": {fakeIssuer}}
		switch p.failure {
		case fakeConsentOmitsIss:
			params.Del("iss")
		case fakeConsentWrongOrigin:
			// Loopback, so only the origin check can refuse it.
			callback = p.pds.URL + "/oauth/callback"
		case fakeConsentWrongPath:
			callback = p.appView.URL + "/oauth/elsewhere"
		case fakeConsentWrongIss:
			params.Set("iss", "http://localhost:3002")
		case fakeConsentWrongRedirectEndpoint:
			redirect = fakeIssuer + "/oauth/elsewhere"
		case fakeConsentWrongRedirectMode:
			params.Set("redirect_mode", "fragment")
		}
		params.Set("redirect_uri", callback)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"url": redirect + "?" + params.Encode()})

	default:
		p.record("pds " + r.URL.Path)
		fakeRefuse(w, http.StatusNotFound, "pds: no route for "+r.URL.Path)
	}
}

func (p *fakeOAuthPair) signIn(t *testing.T) (string, error) {
	t.Helper()
	return p.signInWith(t, map[string]string{fakeIssuerHost: strings.TrimPrefix(p.pds.URL, "http://")})
}

func (p *fakeOAuthPair) signInWith(t *testing.T, dialOverrides map[string]string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	signIn := covesSignIn{AppViewURL: p.appView.URL, DialOverrides: dialOverrides}
	return signIn.SignIn(ctx, fakeHandle, fakePassword)
}

// pdsHops counts the requests the fake PDS received.
func (p *fakeOAuthPair) pdsHops() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, hop := range p.hops {
		if strings.HasPrefix(hop, "pds ") {
			count++
		}
	}
	return count
}

// assertNoSecrets fails t if message holds any fake secret in raw,
// query-escaped or Go-quoted form, or any SENTINEL fragment.
func assertNoSecrets(t *testing.T, message string) {
	t.Helper()
	for _, secret := range fakeSecrets {
		forms := map[string]string{"raw": secret, "query-escaped": url.QueryEscape(secret)}
		if quoted := strconv.Quote(secret); quoted[1:len(quoted)-1] != secret {
			forms["Go-quoted"] = quoted[1 : len(quoted)-1]
		}
		for form, value := range forms {
			if strings.Contains(message, value) {
				t.Errorf("error carries the %s form of secret %q: %s", form, secret, message)
			}
		}
	}
	if strings.Contains(message, "SENTINEL") {
		t.Errorf("error carries a fragment of a secret: %s", message)
	}
}

// TestCovesSignIn_SucceedsAgainstFakes pins that the fakes are passable: the
// helper reaches the issuer through its dial override, completes every hop
// and returns the sealed coves_session the callback set. Without this the
// failure cases below could pass for a helper that never gets past hop one.
func TestCovesSignIn_SucceedsAgainstFakes(t *testing.T) {
	pair := newFakeOAuthPair(t, fakeSignInSucceeds)
	token, err := pair.signIn(t)
	if err != nil {
		t.Fatalf("SignIn against fakes that accept every hop: %v", err)
	}
	if token != fakeSealedSession {
		t.Errorf("SignIn token = %q, want the coves_session the callback set %q", token, fakeSealedSession)
	}
}

// TestCovesSignIn_ErrorsCarryNoSecrets fails each hop of the flow in a way
// that puts a secret the flow handled (the password, the request_uri, the
// code, the state, the ephemeral token or a cookie value) into a body or a
// redirect Location, and checks that SignIn returns an error that holds none
// of them in raw, query-escaped or Go-quoted form.
func TestCovesSignIn_ErrorsCarryNoSecrets(t *testing.T) {
	cases := []struct {
		name    string
		failure fakeSignInFailure
		lastHop string
	}{
		{name: "login redirect with malformed Location carrying the password", failure: fakeLoginMalformedLocation, lastHop: "appview login"},
		{name: "login bounce whose error is the binding cookie", failure: fakeLoginBouncesWithCookie, lastHop: "appview login"},
		{name: "login bounce whose error is an error-shaped binding cookie", failure: fakeLoginBouncesWithErrorShapedCookie, lastHop: "appview login"},
		{name: "authorize refusal echoing its cookie and the request_uri", failure: fakeAuthorizeRefusesEchoing, lastHop: "pds authorize"},
		{name: "sign-in refusal echoing the password and session cookie", failure: fakeSignInRefusesEchoing, lastHop: "pds sign-in"},
		{name: "consent refusal echoing the ephemeral token and rotated cookie", failure: fakeConsentRefusesEchoing, lastHop: "pds consent"},
		{name: "consent url with code and state but no iss", failure: fakeConsentOmitsIss, lastHop: "pds consent"},
		{name: "callback bounce carrying the code and state", failure: fakeCallbackBouncesEchoing, lastHop: "appview callback"},
		{name: "callback bounce whose error is the error-shaped code", failure: fakeCallbackBouncesWithErrorShapedCode, lastHop: "appview callback"},
		{name: "callback bounce whose error is the error-shaped state", failure: fakeCallbackBouncesWithErrorShapedState, lastHop: "appview callback"},
		{name: "callback connection dropped", failure: fakeCallbackDropsConnection, lastHop: "appview callback"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pair := newFakeOAuthPair(t, c.failure)
			token, err := pair.signIn(t)
			if got := pair.lastHop(); got != c.lastHop {
				t.Errorf("the flow last reached %q, want the failing hop %q", got, c.lastHop)
			}
			if err == nil {
				t.Fatalf("SignIn returned token %q and no error; the %s hop failed", token, c.lastHop)
			}
			assertNoSecrets(t, err.Error())
		})
	}
}

// TestCovesSignIn_RefusesNonLoopbackDestinations pins the local-only rule: a
// redirect to a host that is not the AppView, a dial override or loopback is
// refused before any request is made to it, and a dial override that points
// off loopback is refused before the dial. 192.0.2.1 is TEST-NET-1, so even a
// helper that got this wrong would reach nothing.
func TestCovesSignIn_RefusesNonLoopbackDestinations(t *testing.T) {
	t.Run("login redirects to an off-loopback authorize host", func(t *testing.T) {
		pair := newFakeOAuthPair(t, fakeLoginRedirectsOffLoopback)
		_, err := pair.signIn(t)
		if err == nil {
			t.Fatal("SignIn followed a redirect to an off-loopback host without error")
		}
		want := `appview login: refusing redirect to non-local host "192.0.2.1"`
		if !strings.Contains(err.Error(), want) {
			t.Errorf("SignIn error = %q, want it to contain %q", err, want)
		}
		if got := pair.lastHop(); got != "appview login" {
			t.Errorf("the flow last reached %q, want %q", got, "appview login")
		}
		assertNoSecrets(t, err.Error())
	})
	t.Run("dial override points off loopback", func(t *testing.T) {
		pair := newFakeOAuthPair(t, fakeSignInSucceeds)
		_, err := pair.signInWith(t, map[string]string{fakeIssuerHost: fakeOffLoopbackHost})
		if err == nil {
			t.Fatal("SignIn dialed an off-loopback override without error")
		}
		want := `refusing to dial non-loopback host "192.0.2.1"`
		if !strings.Contains(err.Error(), want) {
			t.Errorf("SignIn error = %q, want it to contain %q", err, want)
		}
		if got := pair.pdsHops(); got != 0 {
			t.Errorf("the fake PDS received %d requests, want 0", got)
		}
		assertNoSecrets(t, err.Error())
	})
}

// TestCovesSignIn_ConsentRedirectChecked pins that the consent redirect is
// checked, not just mined for parameters: it must be the issuer's
// /oauth/authorize/redirect in query mode, its redirect_uri must be the
// AppView's origin and /oauth/callback, and it must carry the issuer as iss,
// or the flow stops before the callback.
func TestCovesSignIn_ConsentRedirectChecked(t *testing.T) {
	cases := []struct {
		name    string
		failure fakeSignInFailure
		want    string
	}{
		{name: "redirect on another origin", failure: fakeConsentWrongOrigin, want: "is not the AppView origin"},
		{name: "redirect on another path", failure: fakeConsentWrongPath, want: `redirect_uri path "/oauth/elsewhere" is not /oauth/callback`},
		{name: "redirect with another iss", failure: fakeConsentWrongIss, want: "redirect url iss is not the issuer"},
		{name: "url off the issuer's redirect endpoint", failure: fakeConsentWrongRedirectEndpoint, want: `url "http://localhost:3001/oauth/elsewhere" is not the issuer's /oauth/authorize/redirect`},
		{name: "redirect mode other than query", failure: fakeConsentWrongRedirectMode, want: `redirect_mode "fragment" is not query`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pair := newFakeOAuthPair(t, c.failure)
			token, err := pair.signIn(t)
			if err == nil {
				t.Fatalf("SignIn returned token %q and no error for a consent redirect that is not the AppView callback", token)
			}
			if !strings.Contains(err.Error(), "pds consent: ") || !strings.Contains(err.Error(), c.want) {
				t.Errorf("SignIn error = %q, want a pds consent error containing %q", err, c.want)
			}
			if got := pair.lastHop(); got != "pds consent" {
				t.Errorf("the flow last reached %q, want it to stop at %q", got, "pds consent")
			}
			assertNoSecrets(t, err.Error())
		})
	}
}
