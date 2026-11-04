// Command plc-fixture is the e2e stack's reverse proxy in front of the local
// did:plc directory. The local PLC has no rate limit, so the proxy answers
// POSTs 429 on request and lists every POST it saw. It never contacts anything
// but its configured upstream.
//
// The proxy listener passes everything through. The control listener serves:
//
//	POST /control/rate-limit {"count":1,"retry_after":"60"}
//	  answers the next count POSTs 429 with that Retry-After, not forwarded.
//	  Each call replaces the previous one, so count 0 disarms it.
//	GET /control/posts
//	  {"posts":[{"path":"/did:plc:...","status":429,"at":"<RFC3339Nano>"}]}
//	  in arrival order. status is the upstream's for a forwarded POST, and 0
//	  while that POST is still in flight.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"
)

// post is one POST the proxy saw.
type post struct {
	Path   string    `json:"path"`
	Status int       `json:"status"`
	At     time.Time `json:"at"`
}

// fixture is the state the proxy and control listeners share.
type fixture struct {
	mutex          sync.Mutex
	posts          []post
	limitRemaining int
	retryAfter     string
}

// recordArrival appends a POST and spends one armed 429 if any is left. When
// the POST is limited, the entry is complete and retryAfter is the header to
// answer with.
func (f *fixture) recordArrival(path string) (index int, limited bool, retryAfter string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	entry := post{Path: path, At: time.Now()}
	if f.limitRemaining > 0 {
		f.limitRemaining--
		entry.Status = http.StatusTooManyRequests
		limited, retryAfter = true, f.retryAfter
	}
	f.posts = append(f.posts, entry)
	return len(f.posts) - 1, limited, retryAfter
}

func (f *fixture) setStatus(index, status int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.posts[index].Status = status
}

func (f *fixture) armRateLimit(count int, retryAfter string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.limitRemaining, f.retryAfter = count, retryAfter
}

func (f *fixture) postsSnapshot() []post {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return append([]post{}, f.posts...)
}

// statusRecorder keeps the status the proxy writes. The last call wins, so a
// forwarded 1xx does not hide the final status.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (f *fixture) proxyHandler(upstream http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			upstream.ServeHTTP(w, r)
			return
		}
		index, limited, retryAfter := f.recordArrival(r.URL.Path)
		if limited {
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			http.Error(w, "rate limited by plc-fixture", http.StatusTooManyRequests)
			return
		}
		recorder := &statusRecorder{ResponseWriter: w}
		// Deferred because ReverseProxy panics when a response body copy fails.
		defer func() { f.setStatus(index, recorder.status) }()
		upstream.ServeHTTP(recorder, r)
	})
}

func (f *fixture) controlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /control/rate-limit", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Count      int    `json:"count"`
			RetryAfter string `json:"retry_after"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if request.Count < 0 {
			http.Error(w, "count must not be negative", http.StatusBadRequest)
			return
		}
		f.armRateLimit(request.Count, request.RetryAfter)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /control/posts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string][]post{"posts": f.postsSnapshot()}); err != nil {
			log.Printf("write /control/posts: %v", err)
		}
	})
	return mux
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	upstreamURL, err := url.Parse(envOr("PLC_UPSTREAM_URL", "http://plc:3000"))
	if err != nil {
		log.Fatal(fmt.Errorf("parse PLC_UPSTREAM_URL: %w", err))
	}
	upstream := &httputil.ReverseProxy{Rewrite: func(request *httputil.ProxyRequest) {
		request.SetURL(upstreamURL)
	}}

	state := &fixture{}
	failed := make(chan error, 2)
	go func() {
		failed <- http.ListenAndServe(envOr("PROXY_LISTEN", ":3000"), state.proxyHandler(upstream))
	}()
	go func() {
		failed <- http.ListenAndServe(envOr("CONTROL_LISTEN", ":3001"), state.controlHandler())
	}()
	log.Fatal(<-failed)
}
