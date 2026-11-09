package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// The media gate's defaults.
const (
	// DefaultMediaParkDelay is how long a delivery whose images are not served
	// yet waits before it probes again.
	DefaultMediaParkDelay = 10 * time.Second
	// DefaultMediaWaitBudget is the wall-clock window, from the delivery's
	// creation, the gate holds a delivery for its images before it delivers
	// anyway.
	DefaultMediaWaitBudget = 2 * time.Minute
	// DefaultMediaProbeTimeout bounds one Decide's whole probe sequence, well
	// under the worker's 2-minute claim lease, so a slow proxy cannot hold a
	// claim until another worker re-claims the delivery.
	DefaultMediaProbeTimeout = 30 * time.Second
)

// deliveryMediaPending is the park class of a delivery held by the media gate.
const deliveryMediaPending = "media_pending"

// MediaProber answers whether one proxy image URL is served.
type MediaProber interface {
	Probe(ctx context.Context, imageURL string) (statusCode int, err error)
}

// MediaGate decides whether a Create/Update carrying proxy image URLs may be
// delivered yet. A peer fetches an image when it receives the activity, and
// the Coves proxy answers 404 until the AppView has indexed the blob, so the
// first delivery waits until every image URL is served or Budget runs out.
// One Decide's probes together run for at most ProbeTimeout, so a slow proxy
// cannot hold a delivery's claim past its lease.
type MediaGate struct {
	// BaseURL is COVES_MEDIA_BASE_URL. Only URLs under BaseURL+"/img/" are
	// probed; every other URL is ignored, never fetched.
	BaseURL   string
	Prober    MediaProber
	ParkDelay time.Duration
	Budget    time.Duration
	// ProbeTimeout bounds the whole probe sequence of one Decide; a probe cut
	// off by it is "not served". Zero means DefaultMediaProbeTimeout.
	ProbeTimeout time.Duration
}

// MediaGateInput is one delivery as the gate sees it.
type MediaGateInput struct {
	Kind       string
	Payload    []byte
	MediaReady bool
	CreatedAt  time.Time
	// Now is the clock. It is read after probing, so a slow probe sequence
	// is charged against the budget and NextAttempt is never in the past.
	// Nil means time.Now.
	Now func() time.Time
}

// MediaGateOutcome is the gate's verdict.
type MediaGateOutcome int

const (
	// MediaNotGated: deliver without marking (no image URLs under the base,
	// a kind other than Create/Update, or the activity is already ready).
	MediaNotGated MediaGateOutcome = iota
	// MediaAllServed: every image URL answered 2xx; deliver and mark ready.
	MediaAllServed
	// MediaParked: an image is not served yet; park until NextAttempt.
	MediaParked
	// MediaBudgetExhausted: an image is still not served past the budget;
	// deliver anyway, mark ready and count it.
	MediaBudgetExhausted
	// MediaAborted: the caller's context was cancelled while probing (a
	// shutdown); nothing was decided, so do not deliver, park, mark or count.
	MediaAborted
)

// String names the outcome for logs.
func (o MediaGateOutcome) String() string {
	switch o {
	case MediaNotGated:
		return "not_gated"
	case MediaAllServed:
		return "all_served"
	case MediaParked:
		return "parked"
	case MediaBudgetExhausted:
		return "budget_exhausted"
	case MediaAborted:
		return "aborted"
	default:
		return fmt.Sprintf("unknown(%d)", int(o))
	}
}

// MediaFailureClass names why the first unserved image was not served. It
// carries no URL and no error text, so it is safe to log.
type MediaFailureClass string

const (
	MediaFailureNone      MediaFailureClass = ""
	MediaFailureStatus404 MediaFailureClass = "status_404"
	MediaFailureStatus503 MediaFailureClass = "status_503"
	// MediaFailureStatusOther is any other non-2xx, non-3xx status.
	MediaFailureStatusOther MediaFailureClass = "status_other"
	// MediaFailureRedirect is a 3xx: the prober never follows one.
	MediaFailureRedirect MediaFailureClass = "redirect"
	// MediaFailureTimeout is a probe cut off by a client or probe deadline.
	MediaFailureTimeout MediaFailureClass = "timeout"
	// MediaFailureTransport is any other probe error: DNS, TLS, a refused
	// connection, or the egress guard refusing the proxy's address.
	MediaFailureTransport MediaFailureClass = "transport"
)

// MediaGateDecision is the verdict plus, when parked, when to try again and,
// when parked or exhausted, why the image was not served.
type MediaGateDecision struct {
	Outcome      MediaGateOutcome
	NextAttempt  time.Time
	FailureClass MediaFailureClass
}

// Decide probes the payload's image URLs under BaseURL, each distinct URL
// once, stopping at the first that is not served. A non-2xx answer (a 3xx
// included) or a probe error is "not served". The whole probe sequence runs
// under ProbeTimeout, and a probe cut off by it is "not served" too. The
// clock is read after probing, so the budget check and NextAttempt see the
// time the probes took. A ctx cancelled by the caller (not the probe
// timeout) yields MediaAborted.
func (g MediaGate) Decide(ctx context.Context, in MediaGateInput) MediaGateDecision {
	if g.BaseURL == "" || g.Prober == nil || in.MediaReady || (in.Kind != "Create" && in.Kind != "Update") {
		return MediaGateDecision{Outcome: MediaNotGated}
	}
	urls := payloadImageURLs(in.Payload, g.BaseURL+"/img/")
	if len(urls) == 0 {
		return MediaGateDecision{Outcome: MediaNotGated}
	}
	probeTimeout := g.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = DefaultMediaProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	failure := MediaFailureNone
	for _, imageURL := range urls {
		status, err := g.Prober.Probe(probeCtx, imageURL)
		if err == nil && status >= 200 && status < 300 {
			continue
		}
		failure = classifyMediaFailure(status, err)
		break
	}
	if failure == MediaFailureNone {
		return MediaGateDecision{Outcome: MediaAllServed}
	}
	if ctx.Err() != nil {
		return MediaGateDecision{Outcome: MediaAborted}
	}
	now := in.Now
	if now == nil {
		now = time.Now
	}
	probedAt := now()
	if probedAt.Sub(in.CreatedAt) >= g.Budget {
		return MediaGateDecision{Outcome: MediaBudgetExhausted, FailureClass: failure}
	}
	return MediaGateDecision{Outcome: MediaParked, NextAttempt: probedAt.Add(g.ParkDelay), FailureClass: failure}
}

// classifyMediaFailure names why a probe was not served: by its error when it
// has one, else by its status.
func classifyMediaFailure(status int, err error) MediaFailureClass {
	if err != nil {
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return MediaFailureTimeout
		}
		return MediaFailureTransport
	}
	switch {
	case status == http.StatusNotFound:
		return MediaFailureStatus404
	case status == http.StatusServiceUnavailable:
		return MediaFailureStatus503
	case status >= 300 && status < 400:
		return MediaFailureRedirect
	default:
		return MediaFailureStatusOther
	}
}

// payloadImageURLs returns the distinct image URLs under prefix in a stored
// activity's object, sorted: content <img src> and attachment[].url, the two
// places Tidepool itself writes images. source.content is not read: it holds
// author text verbatim, code facets included, so image syntax there need not
// be an image. A payload that does not decode carries none.
func payloadImageURLs(payload []byte, prefix string) []string {
	var activity struct {
		Object struct {
			Content    string `json:"content"`
			Attachment []struct {
				URL string `json:"url"`
			} `json:"attachment"`
		} `json:"object"`
	}
	if err := json.Unmarshal(payload, &activity); err != nil {
		return nil
	}
	found := make(map[string]struct{})
	add := func(candidate string) {
		if strings.HasPrefix(candidate, prefix) {
			found[candidate] = struct{}{}
		}
	}
	for _, src := range htmlImageSources(activity.Object.Content) {
		add(src)
	}
	for _, attachment := range activity.Object.Attachment {
		add(attachment.URL)
	}
	urls := make([]string, 0, len(found))
	for imageURL := range found {
		urls = append(urls, imageURL)
	}
	sort.Strings(urls)
	return urls
}

// htmlImageSources returns the src of every <img> in an HTML fragment.
func htmlImageSources(fragment string) []string {
	if fragment == "" {
		return nil
	}
	var sources []string
	tokenizer := html.NewTokenizer(strings.NewReader(fragment))
	for {
		switch tokenizer.Next() {
		case html.ErrorToken:
			return sources
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data != "img" {
				continue
			}
			for _, attribute := range token.Attr {
				if attribute.Key == "src" {
					sources = append(sources, attribute.Val)
				}
			}
		}
	}
}

// httpMediaProber probes with HEAD and never follows a redirect: a 3xx is
// returned as the answer, so the Location origin gets no request.
type httpMediaProber struct {
	client *http.Client
}

// newHTTPMediaProber clones client (an ap.NewGuardedHTTPClient) so redirects
// stop at the first response.
func newHTTPMediaProber(client *http.Client) *httpMediaProber {
	clone := *client
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &httpMediaProber{client: &clone}
}

func (p *httpMediaProber) Probe(ctx context.Context, imageURL string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, imageURL, nil)
	if err != nil {
		return 0, fmt.Errorf("media probe: build request: %w", err)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("media probe: %w", err)
	}
	if err := response.Body.Close(); err != nil {
		return 0, fmt.Errorf("media probe: close body: %w", err)
	}
	return response.StatusCode, nil
}
