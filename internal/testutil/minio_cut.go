package testutil

import (
	"io"
	"net"
	"sync"
	"testing"
)

// MinIOCut is a loopback TCP proxy in front of the test MinIO that a test
// can cut (stop accepting and drop every live connection) and restore
// (accept again on the same address), so an s3 store pointed at it sees
// MinIO go away and come back without the shared container being touched.
type MinIOCut struct {
	t       testing.TB
	target  string // MinIO host:port
	address string // the proxy's loopback host:port

	mu       sync.Mutex
	listener net.Listener // nil while cut
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

// NewMinIOCut starts a proxy to target (host:port) on a fresh loopback port.
// It is cut at cleanup.
func NewMinIOCut(t testing.TB, target string) *MinIOCut {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the MinIO proxy: %v", err)
	}
	c := &MinIOCut{t: t, target: target, address: listener.Addr().String(), conns: map[net.Conn]struct{}{}}
	c.serve(listener)
	t.Cleanup(func() {
		c.Cut()
		c.wg.Wait()
	})
	return c
}

// Endpoint is the proxy as an S3 endpoint URL.
func (c *MinIOCut) Endpoint() string { return "http://" + c.address }

// Cut stops accepting and closes every live connection, both sides.
func (c *MinIOCut) Cut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener != nil {
		_ = c.listener.Close()
		c.listener = nil
	}
	for conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, conn)
	}
}

// Restore accepts again on the proxy's original address.
func (c *MinIOCut) Restore() {
	c.t.Helper()
	listener, err := net.Listen("tcp", c.address)
	if err != nil {
		c.t.Fatalf("reopen the MinIO proxy on %s: %v", c.address, err)
	}
	c.serve(listener)
}

func (c *MinIOCut) serve(listener net.Listener) {
	c.mu.Lock()
	c.listener = listener
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			downstream, err := listener.Accept()
			if err != nil {
				return
			}
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.relay(listener, downstream)
			}()
		}
	}()
}

// relay pipes downstream to a fresh MinIO connection until either side ends
// or the proxy is cut.
func (c *MinIOCut) relay(listener net.Listener, downstream net.Conn) {
	upstream, err := net.Dial("tcp", c.target)
	if err != nil {
		_ = downstream.Close()
		return
	}
	c.mu.Lock()
	if c.listener != listener {
		// Cut between Accept and here.
		c.mu.Unlock()
		_ = downstream.Close()
		_ = upstream.Close()
		return
	}
	c.conns[downstream] = struct{}{}
	c.conns[upstream] = struct{}{}
	c.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, downstream); done <- struct{}{} }()
	go func() { _, _ = io.Copy(downstream, upstream); done <- struct{}{} }()
	<-done
	c.mu.Lock()
	delete(c.conns, downstream)
	delete(c.conns, upstream)
	c.mu.Unlock()
	_ = downstream.Close()
	_ = upstream.Close()
	<-done
}
