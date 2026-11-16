package testutil

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingTB stands in for the testing.TB handed to a helper under test: it
// records Fatal*/FailNow/Error* and Skip* instead of acting on the real test,
// stopping the helper's goroutine (runtime.Goexit) where the real method
// would. Cleanups are collected and run by runCleanups.
type recordingTB struct {
	testing.TB
	mu       sync.Mutex
	fatal    []string
	errors   []string
	skipped  []string
	cleanups []func()
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) recordFatal(msg string) {
	r.mu.Lock()
	r.fatal = append(r.fatal, msg)
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) recordSkip(msg string) {
	r.mu.Lock()
	r.skipped = append(r.skipped, msg)
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Fatal(args ...any)                 { r.recordFatal(fmt.Sprint(args...)) }
func (r *recordingTB) Fatalf(format string, args ...any) { r.recordFatal(fmt.Sprintf(format, args...)) }
func (r *recordingTB) FailNow()                          { r.recordFatal("FailNow") }
func (r *recordingTB) Skip(args ...any)                  { r.recordSkip(fmt.Sprint(args...)) }
func (r *recordingTB) Skipf(format string, args ...any)  { r.recordSkip(fmt.Sprintf(format, args...)) }
func (r *recordingTB) SkipNow()                          { r.recordSkip("SkipNow") }

func (r *recordingTB) Error(args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprint(args...))
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Cleanup(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, f)
}

func (r *recordingTB) runCleanups() {
	r.mu.Lock()
	cleanups := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

// closedLoopbackEndpoint returns an http URL on a loopback port nothing
// listens on.
func closedLoopbackEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return "http://" + address
}

// countingLoopbackListener listens on 127.0.0.1, closes every connection it
// accepts, and returns its port and a count of the connections accepted. The
// count is taken before the connection is closed, so a client that has seen
// the close is already counted.
func countingLoopbackListener(t *testing.T) (string, func() int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var mu sync.Mutex
	accepted := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepted++
			mu.Unlock()
			_ = conn.Close()
		}
	}()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	return port, func() int {
		mu.Lock()
		defer mu.Unlock()
		return accepted
	}
}

// TestNewS3BucketFailsInsteadOfSkipping (B3): NewS3Bucket fails the test —
// never skips it — when the TIDEPOOL_TEST_S3_* rail names a non-local
// endpoint, is unset (even with the production BLOB_S3_* variables set), or
// names a MinIO that does not answer.
func TestNewS3BucketFailsInsteadOfSkipping(t *testing.T) {
	const (
		accessKey = "tidepool-test-access-key"
		secretKey = "tidepool-test-secret-key"
		region    = "us-east-1"
	)
	type s3Env struct {
		endpoint, accessKey, secretKey, region *string
		// accepted, when set, reports how many connections the row's
		// listener took; a refused host must cause none.
		accepted func() int
	}
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name string
		env  func(t *testing.T) s3Env
		// productionEnv, when set, populates the BLOB_S3_* variables.
		productionEnv bool
	}{
		{
			name: "non-loopback IP endpoint",
			env: func(*testing.T) s3Env {
				return s3Env{str("http://192.0.2.1:9000"), str(accessKey), str(secretKey), str(region), nil}
			},
		},
		{
			// 0.0.0.0 is not a loopback address, but a dial to it reaches a
			// listener on this machine, so a request made before the host
			// check would show up as an accepted connection.
			name: "non-loopback IP endpoint reaching a local listener",
			env: func(t *testing.T) s3Env {
				port, accepted := countingLoopbackListener(t)
				return s3Env{str("http://0.0.0.0:" + port), str(accessKey), str(secretKey), str(region), accepted}
			},
		},
		{
			name: "non-allowed host endpoint",
			env: func(*testing.T) s3Env {
				return s3Env{str("http://s3.invalid:9000"), str(accessKey), str(secretKey), str(region), nil}
			},
		},
		{
			name:          "test variables unset with production variables set",
			env:           func(*testing.T) s3Env { return s3Env{} },
			productionEnv: true,
		},
		{
			name: "unreachable loopback endpoint",
			env: func(t *testing.T) s3Env {
				return s3Env{str(closedLoopbackEndpoint(t)), str(accessKey), str(secretKey), str(region), nil}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := tc.env(t)
			for name, value := range map[string]*string{
				"TIDEPOOL_TEST_S3_ENDPOINT":          env.endpoint,
				"TIDEPOOL_TEST_S3_ACCESS_KEY_ID":     env.accessKey,
				"TIDEPOOL_TEST_S3_SECRET_ACCESS_KEY": env.secretKey,
				"TIDEPOOL_TEST_S3_REGION":            env.region,
			} {
				t.Setenv(name, "")
				if value == nil {
					if err := os.Unsetenv(name); err != nil {
						t.Fatalf("unset %s: %v", name, err)
					}
				} else {
					t.Setenv(name, *value)
				}
			}
			if tc.productionEnv {
				t.Setenv("BLOB_S3_ENDPOINT", "http://127.0.0.1:9010")
				t.Setenv("BLOB_S3_ACCESS_KEY_ID", accessKey)
				t.Setenv("BLOB_S3_SECRET_ACCESS_KEY", secretKey)
				t.Setenv("BLOB_S3_REGION", region)
				t.Setenv("BLOB_S3_BUCKET", "tidepool-blobs")
			}

			recorder := &recordingTB{TB: t}
			done := make(chan struct{})
			go func() {
				defer close(done)
				NewS3Bucket(recorder)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("NewS3Bucket did not return within 5s")
			}
			recorder.runCleanups()

			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			if len(recorder.fatal) == 0 {
				t.Errorf("NewS3Bucket must call Fatal; errors=%q", recorder.errors)
			}
			if len(recorder.skipped) != 0 {
				t.Errorf("NewS3Bucket must never skip; skipped=%q", recorder.skipped)
			}
			if env.accepted != nil {
				if n := env.accepted(); n != 0 {
					t.Errorf("NewS3Bucket connected to the refused endpoint %d time(s); want 0", n)
				}
			}
		})
	}
}

// proxyChildEnv marks the subprocess TestNewS3BucketIgnoresProxyEnvironment
// starts. net/http reads the proxy variables once per process, so the
// proxy-set case runs in a fresh test binary.
const proxyChildEnv = "TIDEPOOL_TESTUTIL_S3_PROXY_CHILD"

// TestNewS3BucketIgnoresProxyEnvironment: with HTTP_PROXY set, the helper's
// MinIO client must still dial the endpoint itself, never the proxy, so a
// proxy variable cannot route test traffic off the machine. The endpoint
// host is "minio", which net/http would proxy (it only exempts localhost and
// loopback IPs).
func TestNewS3BucketIgnoresProxyEnvironment(t *testing.T) {
	if os.Getenv(proxyChildEnv) == "1" {
		recorder := &recordingTB{TB: t}
		done := make(chan struct{})
		go func() {
			defer close(done)
			NewS3Bucket(recorder)
		}()
		<-done
		recorder.runCleanups()
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		fmt.Printf("child fatal count: %d\n", len(recorder.fatal))
		return
	}

	port, accepted := countingLoopbackListener(t)
	command := exec.Command(os.Args[0], "-test.run=^TestNewS3BucketIgnoresProxyEnvironment$", "-test.count=1")
	var environment []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "REQUEST_METHOD":
			continue
		}
		if strings.HasPrefix(name, "TIDEPOOL_TEST_S3_") {
			continue
		}
		environment = append(environment, entry)
	}
	command.Env = append(environment,
		proxyChildEnv+"=1",
		"HTTP_PROXY=http://127.0.0.1:"+port,
		"HTTPS_PROXY=http://127.0.0.1:"+port,
		"TIDEPOOL_TEST_S3_ENDPOINT=http://minio:9010",
		"TIDEPOOL_TEST_S3_ACCESS_KEY_ID=tidepool-test-access-key",
		"TIDEPOOL_TEST_S3_SECRET_ACCESS_KEY=tidepool-test-secret-key",
		"TIDEPOOL_TEST_S3_REGION=us-east-1",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("proxy child test failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "child fatal count: 1") {
		t.Fatalf("proxy child did not run NewS3Bucket to a single Fatal:\n%s", output)
	}
	if n := accepted(); n != 0 {
		t.Errorf("NewS3Bucket sent %d connection(s) to the HTTP_PROXY listener; want 0", n)
	}
}

// TestLocalS3TransportRefusesNonLocalAddresses: the test MinIO transport
// dials only loopback or private addresses. 0.0.0.0 is neither, but a dial to
// it reaches a listener on this machine, so a missing guard shows up as an
// accepted connection.
func TestLocalS3TransportRefusesNonLocalAddresses(t *testing.T) {
	port, accepted := countingLoopbackListener(t)
	request, err := http.NewRequest(http.MethodGet, "http://0.0.0.0:"+port+"/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	response, err := LocalS3Transport().RoundTrip(request)
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("LocalS3Transport dialed 0.0.0.0; want it refused")
	}
	if !strings.Contains(err.Error(), "non-local address 0.0.0.0") {
		t.Errorf("err = %v; want the non-local address refusal", err)
	}
	if n := accepted(); n != 0 {
		t.Errorf("LocalS3Transport connected %d time(s); want 0", n)
	}
}
