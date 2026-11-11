package e2e

// No build tag: the helper is plain client logic, so the untagged unit tests
// in coves_signin_test.go can drive it against httptest fakes, and the
// e2e-tagged suite drives it against the stack.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Headless OAuth web sign-in against the Coves AppView, ported from Coves'
// tests/testkit/signin.go (a different module, so it cannot be imported).
//
// WHY THIS EXISTS
//
// The AppView accepts one credential: a sealed token naming a row in its OAuth
// session store, and the only thing that issues one is /oauth/callback at the
// end of a real authorization-code flow. The suite has no other way to get a
// session than to do what a browser does: start at /oauth/login, sign in and
// consent at the PDS's authorization server, and hand the resulting code to
// the callback.
//
// WHAT IT DEPENDS ON
//
// The PDS side is not an XRPC API. It is the JSON API behind the sign-in page
// of @atproto/oauth-provider (the ~api/sign-in and ~api/consent endpoints), an
// implementation detail of the provider's UI pinned by the PDS image in
// docker-compose.e2e.yml. Its checks shape the requests below: the authorize
// page must be loaded as a top-level navigation to bind a device, and the API
// calls must look same-origin (Origin, Referer and Sec-Fetch-* headers) and
// carry the double-submit CSRF token the page set. A PDS upgrade that changes
// that UI breaks this helper, and the fix belongs here.
//
// The stack's PDS calls itself http://localhost:3001, which is true only in the
// relay's network namespace; from the host it is published on another port.
// DialOverrides redirects the dial alone, so URL, Host and Origin keep the
// issuer host the provider checks them against.
//
// It is local-only: it uses no proxy, refuses to dial anything but a loopback
// IP literal or localhost (dialed as 127.0.0.1), and refuses a redirect to a
// host that is not the AppView's, a DialOverrides key or loopback before
// making any request to it.
//
// WHAT IT NEVER DOES
//
// It follows no redirect on its own: each hop is a step whose answer is checked,
// so a failure names the step. Its errors carry the HTTP status, the provider's
// error code and description, a redirect's path but never its query, and
// transport errors. Secrecy is enforced by value at one point, where SignIn
// returns: every secret the flow handled (the password, the ephemeral token,
// the request_uri, the authorization code and state, and every cookie value
// either side set, the sealed session included) is scrubbed from the message.

// covesSignIn signs accounts in through a Coves AppView's OAuth web flow.
type covesSignIn struct {
	// AppViewURL is the base URL the host dials, e.g. http://127.0.0.1:8093.
	AppViewURL string
	// DialOverrides maps a host:port in a URL to the host:port actually
	// dialed, e.g. "localhost:3001" -> "127.0.0.1:3081". A host:port with no
	// entry is dialed as is. Either way the dialed host must be loopback.
	DialOverrides map[string]string
}

// covesOAuthProviderAPIPrefix is where @atproto/oauth-provider mounts its UI's
// API.
const covesOAuthProviderAPIPrefix = "/@atproto/oauth-provider/~api"

// covesSignInRequestTimeout bounds each request of the flow; the caller's
// context bounds the whole.
const covesSignInRequestTimeout = 30 * time.Second

// covesSignInMaxBody bounds how much of any response body the flow reads.
const covesSignInMaxBody = 64 << 10

// covesSignInMaxDescription caps how much of a provider's error_description
// an error repeats.
const covesSignInMaxDescription = 200

// covesSignInRedacted stands in for a secret wherever an error would have
// quoted it.
const covesSignInRedacted = "[redacted]"

// covesOAuthErrorCode matches what an OAuth error code looks like, a first
// filter on what reaches a test log. It is a shape check only: secrecy comes
// from scrubbing.
var covesOAuthErrorCode = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// covesSignInFlow is the state of one sign-in: the two sides' cookies, the
// addresses each later step must reuse, and the secrets handled so far.
type covesSignInFlow struct {
	// appViewOrigin is where the flow's AppView requests go. It starts as
	// AppViewURL and moves if /oauth/login canonicalizes the host, because
	// the binding cookie it sets is host-only.
	appViewOrigin string
	// dialOverrides is covesSignIn.DialOverrides; its keys are hosts a
	// redirect may point at.
	dialOverrides map[string]string
	// appViewHTTP keeps a fresh cookie jar for the AppView side, so the
	// host-only, /oauth-scoped binding cookie reaches the callback exactly as
	// a browser would send it.
	appViewHTTP *http.Client

	pdsHTTP *http.Client
	// pdsCookies is a name→value map rather than a cookiejar: the provider
	// marks csrf-token Secure, and net/http/cookiejar will not send a Secure
	// cookie over the plain-http address the stack's PDS listens on.
	pdsCookies   map[string]string
	authorizeURL string
	issuer       string

	// secrets holds every secret value the flow has handled, in each form
	// remember records, for scrubbing out of errors.
	secrets []string
}

// SignIn runs the real OAuth web login for handle and returns the sealed
// coves_session value, for use as `Authorization: Bearer <token>` on AppView
// XRPC calls. Its error is scrubbed of every secret the flow handled;
// errors.Is still recognizes the context's deadline or cancellation through
// it.
func (s covesSignIn) SignIn(ctx context.Context, handle, password string) (string, error) {
	flow, err := s.newFlow()
	if err != nil {
		return "", err
	}
	flow.remember(password)
	token, err := flow.run(ctx, handle, password)
	if err != nil {
		return "", flow.scrub(err)
	}
	return token, nil
}

func (s covesSignIn) newFlow() (*covesSignInFlow, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("sign-in: creating cookie jar: %w", err)
	}
	// Test tooling against loopback services, so a plain transport: the
	// bridge's ap.NewGuardedHTTPClient refuses loopback addresses by design.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	dialer := &net.Dialer{Timeout: covesSignInRequestTimeout}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if override, ok := s.DialOverrides[address]; ok {
			address = override
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("sign-in: dial address is not host:port")
		}
		if !covesLoopbackHost(host) {
			return nil, fmt.Errorf("sign-in: refusing to dial non-loopback host %q", host)
		}
		if host == "localhost" {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return dialer.DialContext(ctx, network, address)
	}
	guarded := covesLocationGuard{next: transport}
	noRedirects := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &covesSignInFlow{
		appViewOrigin: strings.TrimRight(s.AppViewURL, "/"),
		dialOverrides: s.DialOverrides,
		appViewHTTP:   &http.Client{Transport: guarded, Timeout: covesSignInRequestTimeout, Jar: jar, CheckRedirect: noRedirects},
		pdsHTTP:       &http.Client{Transport: guarded, Timeout: covesSignInRequestTimeout, CheckRedirect: noRedirects},
		pdsCookies:    map[string]string{},
	}, nil
}

// run is the flow itself. Its errors are not yet scrubbed; SignIn does that.
func (f *covesSignInFlow) run(ctx context.Context, handle, password string) (string, error) {
	authorizeURL, err := f.startLogin(ctx, handle)
	if err != nil {
		return "", err
	}
	if err := f.loadAuthorizePage(ctx, authorizeURL); err != nil {
		return "", err
	}

	var signIn struct {
		Account struct {
			DID string `json:"did"`
		} `json:"account"`
		EphemeralToken string `json:"ephemeralToken"`
	}
	err = f.callProviderAPI(ctx, "sign-in", map[string]any{
		"locale":   "en",
		"username": handle,
		"password": password,
		"remember": false,
	}, "", &signIn)
	if err != nil {
		return "", err
	}
	f.remember(signIn.EphemeralToken)
	if signIn.Account.DID == "" || signIn.EphemeralToken == "" {
		return "", errors.New("pds sign-in: response carried no account.did or no ephemeralToken")
	}

	// Consent answers with the URL the provider would send the browser to:
	// the issuer's /oauth/authorize/redirect, which redirects to its
	// redirect_uri with code, state and iss in the query. It carries exactly
	// the parameters the AppView's callback receives, so it is read, never
	// fetched.
	var consent struct {
		URL string `json:"url"`
	}
	if err := f.callProviderAPI(ctx, "consent", map[string]any{"did": signIn.Account.DID}, signIn.EphemeralToken, &consent); err != nil {
		return "", err
	}
	if consent.URL == "" {
		return "", errors.New("pds consent: response carried no redirect url")
	}
	authorization, err := url.Parse(consent.URL)
	if err != nil {
		return "", errors.New("pds consent: redirect url did not parse")
	}
	query := authorization.Query()
	f.remember(query.Get("code"), query.Get("state"))
	// The browser would land on the AppView callback, with the parameters in
	// its query; the errors name origins, paths and the mode, never code or
	// state.
	if endpoint := authorization.Scheme + "://" + authorization.Host + authorization.Path; endpoint != f.issuer+"/oauth/authorize/redirect" {
		return "", fmt.Errorf("pds consent: redirect url %q is not the issuer's /oauth/authorize/redirect", endpoint)
	}
	if mode := query.Get("redirect_mode"); mode != "query" {
		return "", fmt.Errorf("pds consent: redirect_mode %q is not query", mode)
	}
	callbackURL, err := url.Parse(query.Get("redirect_uri"))
	if err != nil {
		return "", errors.New("pds consent: redirect_uri did not parse")
	}
	if origin := callbackURL.Scheme + "://" + callbackURL.Host; origin != f.appViewOrigin {
		return "", fmt.Errorf("pds consent: redirect_uri origin %q is not the AppView origin %q", origin, f.appViewOrigin)
	}
	if callbackURL.Path != "/oauth/callback" {
		return "", fmt.Errorf("pds consent: redirect_uri path %q is not /oauth/callback", callbackURL.Path)
	}
	if code := query.Get("error"); code != "" {
		return "", fmt.Errorf("pds consent: authorization refused with %s", covesSanitizeOAuthError(code))
	}
	callback := url.Values{}
	for _, name := range []string{"code", "state", "iss"} {
		if query.Get(name) == "" {
			return "", fmt.Errorf("pds consent: redirect url carried no %s", name)
		}
		callback.Set(name, query.Get(name))
	}
	if query.Get("iss") != f.issuer {
		return "", fmt.Errorf("pds consent: redirect url iss is not the issuer %q", f.issuer)
	}

	return f.completeCallback(ctx, callback)
}

// startLogin requests /oauth/login and returns the PDS authorize URL it
// redirects to. A dev-mode AppView reached under a host other than the one in
// its configured public URL first redirects to /oauth/login on the public
// URL's host; that one hop is taken.
func (f *covesSignInFlow) startLogin(ctx context.Context, handle string) (string, error) {
	target := f.appViewOrigin + "/oauth/login?" + url.Values{"handle": {handle}}.Encode()
	for range 2 {
		resp, err := f.appViewRequest(ctx, "appview login", target)
		if err != nil {
			return "", err
		}
		location, err := covesRedirectLocation("appview login", resp)
		if err != nil {
			return "", err
		}
		if (location.Path == "/oauth/authorize" || location.Path == "/oauth/login") && !f.localRedirectHost(location.Host) {
			return "", fmt.Errorf("appview login: refusing redirect to non-local host %q", location.Hostname())
		}
		switch location.Path {
		case "/oauth/authorize":
			return location.String(), nil
		case "/oauth/login":
			f.appViewOrigin = location.Scheme + "://" + location.Host
			target = location.String()
		case "/login":
			return "", fmt.Errorf("appview login: redirected to the login page with %s",
				covesSanitizeOAuthError(location.Query().Get("error")))
		default:
			return "", fmt.Errorf("appview login: redirected to unexpected path %q", location.Path)
		}
	}
	return "", errors.New("appview login: redirected to /oauth/login more than once")
}

// localRedirectHost reports whether a redirect may point at hostPort: the
// AppView's host, a DialOverrides key, or a loopback host.
func (f *covesSignInFlow) localRedirectHost(hostPort string) bool {
	if appView, err := url.Parse(f.appViewOrigin); err == nil && appView.Host == hostPort {
		return true
	}
	if _, ok := f.dialOverrides[hostPort]; ok {
		return true
	}
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
	}
	return covesLoopbackHost(host)
}

// covesLoopbackHost reports whether host, without a port, is exactly
// localhost or a loopback IP literal.
func covesLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// loadAuthorizePage loads the PDS authorize page as a top-level navigation,
// which binds the pending authorization request to a new device session.
func (f *covesSignInFlow) loadAuthorizePage(ctx context.Context, authorizeURL string) error {
	parsed, err := url.Parse(authorizeURL)
	if err != nil {
		return errors.New("pds authorize: redirect location is not a URL")
	}
	f.remember(parsed.Query().Get("request_uri"))
	f.authorizeURL = authorizeURL
	f.issuer = parsed.Scheme + "://" + parsed.Host

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authorizeURL, nil)
	if err != nil {
		return errors.New("pds authorize: building request failed")
	}
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	resp, err := covesSignInSend(f.pdsHTTP, req, "pds authorize")
	if err != nil {
		return err
	}
	defer covesDrainAndClose(resp)
	// Absorb before the status check: a refusal's cookies must be remembered
	// before its body can echo them into the error.
	f.absorbPDSCookies(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pds authorize: HTTP %d%s", resp.StatusCode, f.providerErrorSuffix(resp))
	}
	// The provider's CSRF check is double-submit: the header must equal the
	// cookie the page set, so a page that set none is a failure here.
	var missing []string
	for _, name := range []string{"dev-id", "ses-id", "csrf-token"} {
		if f.pdsCookies[name] == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("pds authorize: page set no %s cookie", strings.Join(missing, ", "))
	}
	return nil
}

// callProviderAPI POSTs in to the provider UI's endpoint, as the sign-in
// page's own script would, and decodes a 200 answer into out.
func (f *covesSignInFlow) callProviderAPI(ctx context.Context, endpoint string, in any, bearer string, out any) error {
	step := "pds " + endpoint
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("%s: encoding request failed", step)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.issuer+covesOAuthProviderAPIPrefix+"/"+endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: building request failed", step)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", f.issuer)
	req.Header.Set("Referer", f.authorizeURL)
	req.Header.Set("Sec-Fetch-Mode", "same-origin")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("X-CSRF-Token", f.pdsCookies["csrf-token"])
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for name, value := range f.pdsCookies {
		req.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	resp, err := covesSignInSend(f.pdsHTTP, req, step)
	if err != nil {
		return err
	}
	defer covesDrainAndClose(resp)
	// Sign-in rotates ses-id; consent must present the rotated one. Absorb
	// before the status check so a refusal's cookies are remembered before
	// its body can echo them into the error.
	f.absorbPDSCookies(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d%s", step, resp.StatusCode, f.providerErrorSuffix(resp))
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, covesSignInMaxBody)).Decode(out); err != nil {
		return fmt.Errorf("%s: response (Content-Type %q) is not the expected JSON: %w", step, resp.Header.Get("Content-Type"), err)
	}
	return nil
}

// completeCallback hands the authorization response to the AppView's callback
// and returns the sealed session it set. A callback that answers without
// setting coves_session failed, whatever its status says.
func (f *covesSignInFlow) completeCallback(ctx context.Context, params url.Values) (string, error) {
	resp, err := f.appViewRequest(ctx, "appview callback", f.appViewOrigin+"/oauth/callback?"+params.Encode())
	if err != nil {
		return "", err
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "coves_session" && cookie.Value != "" && cookie.MaxAge >= 0 {
			return cookie.Value, nil
		}
	}
	if location, err := resp.Location(); err == nil && location.Path == "/login" {
		return "", fmt.Errorf("appview callback: HTTP %d, redirected to the login page with %s",
			resp.StatusCode, covesSanitizeOAuthError(location.Query().Get("error")))
	}
	return "", fmt.Errorf("appview callback: HTTP %d without setting coves_session", resp.StatusCode)
}

// appViewRequest GETs target with the flow's AppView cookie jar. The body is
// drained and closed; callers read only the status, Location and cookies.
func (f *covesSignInFlow) appViewRequest(ctx context.Context, step, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: building request failed", step)
	}
	resp, err := covesSignInSend(f.appViewHTTP, req, step)
	if err != nil {
		return nil, err
	}
	covesDrainAndClose(resp)
	for _, cookie := range resp.Cookies() {
		f.remember(cookie.Value)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("%s: HTTP 429, rate limited by the AppView's OAuth login limiter "+
			"(10 per minute per client IP; each sign-in spends two or three)", step)
	}
	return resp, nil
}

func (f *covesSignInFlow) absorbPDSCookies(resp *http.Response) {
	for _, cookie := range resp.Cookies() {
		f.remember(cookie.Value)
		if cookie.MaxAge < 0 || cookie.Value == "" {
			delete(f.pdsCookies, cookie.Name)
			continue
		}
		f.pdsCookies[cookie.Name] = cookie.Value
	}
}

// providerErrorSuffix reads an OAuth-style {"error", "error_description"} body
// and returns both for an error message. The description is free text that
// can echo the request, so it is scrubbed before it is cut to
// covesSignInMaxDescription bytes, which keeps a cut from splitting a secret.
func (f *covesSignInFlow) providerErrorSuffix(resp *http.Response) string {
	var envelope struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, covesSignInMaxBody)).Decode(&envelope); err != nil || envelope.Error == "" {
		return ""
	}
	suffix := " " + covesSanitizeOAuthError(envelope.Error)
	if description := f.scrubText(envelope.Description); description != "" {
		if len(description) > covesSignInMaxDescription {
			description = strings.ToValidUTF8(description[:covesSignInMaxDescription], "") + "…"
		}
		suffix += ": " + description
	}
	return suffix
}

// remember records secret values the flow has handled, so scrub can remove
// them from errors: each raw, query-escaped, and as %q would write it (the
// strconv.Quote form without its outer quotes, kept only when quoting changed
// the value).
func (f *covesSignInFlow) remember(values ...string) {
	for _, value := range values {
		if value == "" {
			continue
		}
		f.secrets = append(f.secrets, value, url.QueryEscape(value))
		if quoted := strconv.Quote(value); quoted[1:len(quoted)-1] != value {
			f.secrets = append(f.secrets, quoted[1:len(quoted)-1])
		}
	}
}

// scrubText replaces every remembered form of every secret in text with
// covesSignInRedacted.
func (f *covesSignInFlow) scrubText(text string) string {
	secrets := slices.Clone(f.secrets)
	// Longest first, so a secret that contains another is replaced whole.
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	for _, secret := range secrets {
		text = strings.ReplaceAll(text, secret, covesSignInRedacted)
	}
	return text
}

// scrub is the flow's one secrecy choke point: it returns err's message with
// every remembered secret removed.
func (f *covesSignInFlow) scrub(err error) error {
	scrubbed := &covesSignInError{message: f.scrubText(err.Error())}
	for _, contextErr := range []error{context.DeadlineExceeded, context.Canceled} {
		if errors.Is(err, contextErr) {
			scrubbed.contextErr = contextErr
			break
		}
	}
	return scrubbed
}

// covesSignInError is a scrubbed sign-in failure. It unwraps only to the
// context's error, so errors.Is still sees a deadline or cancellation while no
// unscrubbed message stays reachable through it.
type covesSignInError struct {
	message    string
	contextErr error
}

func (e *covesSignInError) Error() string { return e.message }

func (e *covesSignInError) Unwrap() error { return e.contextErr }

// covesLocationGuard drops a Location header that does not parse as a URL
// reference. net/http parses Location before CheckRedirect can stop the
// redirect and, when that fails, returns an error quoting the raw header,
// query and all: text the flow has never seen and so cannot scrub. Without the
// header the client returns the response, and covesRedirectLocation reports a
// redirect without a usable Location.
type covesLocationGuard struct {
	next http.RoundTripper
}

func (g covesLocationGuard) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := g.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if location := resp.Header.Get("Location"); location != "" {
		if _, err := req.URL.Parse(location); err != nil {
			resp.Header.Del("Location")
		}
	}
	return resp, nil
}

// covesSignInSend performs req, unwrapping *url.Error: its message quotes the
// full request URL, and the callback's query string holds the authorization
// code.
func covesSignInSend(client *http.Client, req *http.Request, step string) (*http.Response, error) {
	resp, err := client.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("%s: %w", step, err)
	}
	return resp, nil
}

// covesRedirectLocation requires resp to be a redirect and returns where it
// points. The Location itself stays out of the error: its query can carry
// anything.
func covesRedirectLocation(step string, resp *http.Response) (*url.URL, error) {
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: HTTP %d, expected a redirect", step, resp.StatusCode)
	}
	location, err := resp.Location()
	if err != nil {
		return nil, fmt.Errorf("%s: HTTP %d redirect without a usable Location", step, resp.StatusCode)
	}
	return location, nil
}

// covesSanitizeOAuthError renders an OAuth error code for a message, or says
// there was none worth repeating.
func covesSanitizeOAuthError(code string) string {
	switch {
	case code == "":
		return "no error code"
	case covesOAuthErrorCode.MatchString(code):
		return "error " + code
	default:
		return "an unrecognized error code"
	}
}

// covesDrainAndClose discards what is left of a body, bounded, so the
// connection can be reused, then closes it.
func covesDrainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, covesSignInMaxBody))
	_ = resp.Body.Close()
}
