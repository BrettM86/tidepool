package dns

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func freeDNSAddress(t *testing.T) string {
	t.Helper()
	for attempt := 0; attempt < 10; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		packet, err := net.ListenPacket("udp", address)
		if err == nil {
			require.NoError(t, packet.Close())
			require.NoError(t, listener.Close())
			return address
		}
		require.NoError(t, listener.Close())
		if !errors.Is(err, syscall.EADDRINUSE) {
			require.NoError(t, err)
		}
	}
	t.Fatal("could not find a loopback port free for both UDP and TCP")
	return ""
}

func TestServe(t *testing.T) {
	t.Run("answers handle TXT over UDP and TCP and releases both sockets on cancellation", func(t *testing.T) {
		resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
		handler, err := NewHandler(handlerOptions(resolver))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		address, done := serveOnFreeDNSAddress(t, ctx, handler)

		request := new(miekgdns.Msg)
		request.SetQuestion("_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT)
		for _, network := range []string{"udp", "tcp"} {
			t.Run(network, func(t *testing.T) {
				response, _, err := (&miekgdns.Client{Net: network, Timeout: time.Second}).Exchange(request, address)
				require.NoError(t, err, "Serve must receive the query over %s", network)
				require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
				require.True(t, response.Authoritative)
				require.Len(t, response.Answer, 1)
				txt, ok := response.Answer[0].(*miekgdns.TXT)
				require.True(t, ok)
				require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)
			})
		}

		cancel()
		requireClosedWithoutError(t, done)
		requireBothSocketsAvailable(t, address)
	})

	t.Run("occupied UDP address fails synchronously", func(t *testing.T) {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		defer packet.Close()
		handler, err := NewHandler(handlerOptions(&recordingResolver{}))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done, err := Serve(ctx, packet.LocalAddr().String(), handler, slog.New(slog.DiscardHandler))
		require.Error(t, err)
		require.Nil(t, done)
	})

	t.Run("immediate cancellation closes bound socket", func(t *testing.T) {
		handler, err := NewHandler(handlerOptions(&recordingResolver{}))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		address, done := serveOnFreeDNSAddress(t, ctx, handler)
		probe, err := net.ListenPacket("udp", address)
		if err == nil {
			_ = probe.Close()
		}
		require.Error(t, err, "Serve must bind before returning")
		cancel()
		requireClosedWithoutError(t, done)
		require.Eventually(t, func() bool {
			probe, err := net.ListenPacket("udp", address)
			if err != nil {
				return false
			}
			return probe.Close() == nil
		}, 2*time.Second, 10*time.Millisecond)
	})
}

func requireBothSocketsAvailable(t *testing.T, address string) {
	t.Helper()
	require.Eventually(t, func() bool {
		packet, err := net.ListenPacket("udp", address)
		if err != nil {
			return false
		}
		defer packet.Close()
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return false
		}
		return listener.Close() == nil
	}, 2*time.Second, 10*time.Millisecond, "cancellation must release both UDP and TCP sockets")
}

func serveOnFreeDNSAddress(t *testing.T, ctx context.Context, handler miekgdns.Handler) (string, <-chan error) {
	t.Helper()
	var (
		address string
		done    <-chan error
		err     error
	)
	for attempt := 0; attempt < 5; attempt++ {
		address = freeDNSAddress(t)
		done, err = Serve(ctx, address, handler, slog.New(slog.DiscardHandler))
		if !errors.Is(err, syscall.EADDRINUSE) {
			break
		}
	}
	require.NoError(t, err)
	require.NotNil(t, done)
	return address, done
}

func requireClosedWithoutError(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err, open := <-done:
		require.False(t, open, "cancellation must close the channel without sending an error, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("server channel was not closed after cancellation")
	}
}

func largeNSFixture(t *testing.T) string {
	t.Helper()
	resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
	options := handlerOptions(resolver)
	options.Nameservers = make([]string, 40)
	for index := range options.Nameservers {
		options.Nameservers[index] = fmt.Sprintf("ns.distinct-nameserver-domain-label-%02d.example", index)
	}
	handler, err := NewHandler(options)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	address, done := serveOnFreeDNSAddress(t, ctx, handler)
	t.Cleanup(func() {
		cancel()
		requireClosedWithoutError(t, done)
	})

	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeNS)
	response, _, err := (&miekgdns.Client{Net: "tcp", Timeout: time.Second}).Exchange(request, address)
	require.NoError(t, err, "fixture precondition: full NS reply must be available over TCP")
	require.Len(t, response.Answer, 40, "fixture precondition: full NS reply")
	response.Compress = true
	packed, err := response.Pack()
	require.NoError(t, err)
	require.Greater(t, len(packed), 1300, "fixture precondition: packed full reply must exceed 1300 bytes")
	require.Less(t, len(packed), 4000, "fixture precondition: packed full reply must be under 4000 bytes")
	return address
}

func rawUDPQuery(t *testing.T, address string, request *miekgdns.Msg) (*miekgdns.Msg, int) {
	t.Helper()
	packed, err := request.Pack()
	require.NoError(t, err)
	conn, err := net.Dial("udp", address)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	_, err = conn.Write(packed)
	require.NoError(t, err)
	buffer := make([]byte, 65535)
	count, err := conn.Read(buffer)
	require.NoError(t, err)
	response := new(miekgdns.Msg)
	require.NoError(t, response.Unpack(buffer[:count]))
	return response, count
}

func TestServeTCPFullNSAnswers(t *testing.T) {
	address := largeNSFixture(t)
	for _, tc := range []struct {
		name     string
		ednsSize uint16
	}{
		{"without OPT", 0},
		{"with OPT advertising 1232", 1232},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := new(miekgdns.Msg)
			request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeNS)
			if tc.ednsSize != 0 {
				request.SetEdns0(tc.ednsSize, false)
			}
			response, _, err := (&miekgdns.Client{Net: "tcp", Timeout: time.Second}).Exchange(request, address)
			require.NoError(t, err)
			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.False(t, response.Truncated, "TCP must return the full reply")
			require.Len(t, response.Answer, 40)
			if tc.ednsSize != 0 {
				opt := response.IsEdns0()
				require.NotNil(t, opt, "TCP replies to EDNS queries must carry an OPT record")
				require.Equal(t, uint16(advertisedUDPPayloadSize), opt.UDPSize())
			} else {
				require.Nil(t, response.IsEdns0(), "replies to non-EDNS queries must not carry an OPT record")
			}
			for _, record := range response.Answer {
				_, ok := record.(*miekgdns.NS)
				require.True(t, ok, "expected NS, got %T", record)
			}
		})
	}
}

func TestServeUDPWithoutEDNSTruncatesNS(t *testing.T) {
	address := largeNSFixture(t)
	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeNS)
	response, count := rawUDPQuery(t, address, request)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.True(t, response.Truncated)
	require.LessOrEqual(t, count, 512, "received datagram must fit legacy UDP")
	require.Nil(t, response.IsEdns0(), "replies to non-EDNS queries must not carry an OPT record")

	request.SetQuestion("_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT)
	response, count = rawUDPQuery(t, address, request)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.False(t, response.Truncated, "small UDP replies must not be truncated")
	require.LessOrEqual(t, count, 512)
	require.Nil(t, response.IsEdns0(), "replies to non-EDNS queries must not carry an OPT record")
	require.Len(t, response.Answer, 1)
	txt, ok := response.Answer[0].(*miekgdns.TXT)
	require.True(t, ok)
	require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)
}

func TestServeUDPWithLargeEDNSReturnsFullNS(t *testing.T) {
	address := largeNSFixture(t)
	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeNS)
	request.SetEdns0(4096, false)
	response, count := rawUDPQuery(t, address, request)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.False(t, response.Truncated)
	require.Len(t, response.Answer, 40)
	require.LessOrEqual(t, count, 4096)
	opt := response.IsEdns0()
	require.NotNil(t, opt, "EDNS0 replies must carry an OPT record")
	require.Equal(t, uint16(advertisedUDPPayloadSize), opt.UDPSize(), "the OPT must advertise the server's own receive size")
}

func TestServeUDPWithSmallEDNSTruncatesNS(t *testing.T) {
	address := largeNSFixture(t)
	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeNS)
	request.SetEdns0(1232, false)
	response, count := rawUDPQuery(t, address, request)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.True(t, response.Truncated)
	require.Greater(t, count, 512, "EDNS0 must allow more than legacy UDP")
	require.LessOrEqual(t, count, 1232)
	opt := response.IsEdns0()
	require.NotNil(t, opt, "EDNS0 replies must carry an OPT record")
	require.Equal(t, uint16(advertisedUDPPayloadSize), opt.UDPSize(), "the OPT must advertise the server's own receive size")
}

func TestServeUDPAnswersQueriesLargerThanLegacySize(t *testing.T) {
	resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
	handler, err := NewHandler(handlerOptions(resolver))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	address, _ := serveOnFreeDNSAddress(t, ctx, handler)

	request := new(miekgdns.Msg)
	request.SetQuestion("_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT)
	request.SetEdns0(4096, false)
	opt := request.IsEdns0()
	opt.Option = append(opt.Option, &miekgdns.EDNS0_PADDING{Padding: make([]byte, 800)})
	packed, err := request.Pack()
	require.NoError(t, err)
	require.Greater(t, len(packed), 512, "fixture precondition: query must exceed legacy UDP size")
	require.LessOrEqual(t, len(packed), advertisedUDPPayloadSize, "fixture precondition: query must fit the advertised size")

	response, _ := rawUDPQuery(t, address, request)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.Len(t, response.Answer, 1)
	txt, ok := response.Answer[0].(*miekgdns.TXT)
	require.True(t, ok)
	require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)
}

var errInjectedRead = errors.New("injected read failure")

// failingPacketConn reads nothing until fail is closed, then returns a
// non-temporary read error, which stops the miekg serve loop.
type failingPacketConn struct {
	net.PacketConn
	fail chan struct{}
}

func (c *failingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	<-c.fail
	return 0, nil, errInjectedRead
}

func TestServePacketConnReportsServeFailure(t *testing.T) {
	handler, err := NewHandler(handlerOptions(&recordingResolver{}))
	require.NoError(t, err)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	failing := &failingPacketConn{PacketConn: packet, fail: make(chan struct{})}

	done, err := servePacketConn(t.Context(), failing, handler)
	require.NoError(t, err)
	require.NotNil(t, done)
	select {
	case err := <-done:
		t.Fatalf("server stopped before the read failure: %v", err)
	default:
	}

	close(failing.fail)
	select {
	case err, open := <-done:
		require.True(t, open, "a serve failure must be sent before the channel closes")
		require.ErrorIs(t, err, errInjectedRead)
	case <-time.After(2 * time.Second):
		t.Fatal("serve failure was not reported")
	}
	select {
	case _, open := <-done:
		require.False(t, open, "the channel must close after the serve failure")
	case <-time.After(2 * time.Second):
		t.Fatal("server channel was not closed after the serve failure")
	}
}

var errInjectedAccept = errors.New("injected accept failure")

type failingListener struct {
	net.Listener
	fail chan struct{}
}

func (l *failingListener) Accept() (net.Conn, error) {
	<-l.fail
	return nil, errInjectedAccept
}

func TestServeConnsReportsEitherTransportFailure(t *testing.T) {
	for _, network := range []string{"udp", "tcp"} {
		t.Run(network, func(t *testing.T) {
			packet, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)
			defer packet.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var fail chan struct{}
			if network == "udp" {
				failing := &failingPacketConn{PacketConn: packet, fail: make(chan struct{})}
				fail = failing.fail
				packet = failing
			} else {
				failing := &failingListener{Listener: listener, fail: make(chan struct{})}
				fail = failing.fail
				listener = failing
			}
			handler, err := NewHandler(handlerOptions(&recordingResolver{}))
			require.NoError(t, err)
			done, err := serveConns(ctx, packet, listener, handler, slog.New(slog.DiscardHandler))
			require.NoError(t, err)
			select {
			case reported, open := <-done:
				t.Fatalf("server stopped before %s failed: error=%v open=%v", network, reported, open)
			default:
			}
			close(fail)
			select {
			case reported, open := <-done:
				require.True(t, open, "failure must be sent before the channel closes")
				require.Error(t, reported)
			case <-time.After(2 * time.Second):
				t.Fatal("transport failure was not reported")
			}
			select {
			case reported, open := <-done:
				require.False(t, open, "the channel must close after one error, got %v", reported)
			case <-time.After(2 * time.Second):
				t.Fatal("server channel was not closed after the failure")
			}
		})
	}
}

// dialTCPAndQuery opens a DNS TCP connection and checks it is served.
func dialTCPAndQuery(t *testing.T, address string, request *miekgdns.Msg) *miekgdns.Conn {
	t.Helper()
	conn, err := miekgdns.Dial("tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	require.NoError(t, conn.WriteMsg(request))
	response, err := conn.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	return conn
}

func TestServeTCPLimitsConcurrentConnections(t *testing.T) {
	handler, err := NewHandler(handlerOptions(&recordingResolver{}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	address, _ := serveOnFreeDNSAddress(t, ctx, handler)
	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeSOA)

	held := make([]*miekgdns.Conn, 0, maxTCPConnections)
	for range maxTCPConnections {
		held = append(held, dialTCPAndQuery(t, address, request))
	}

	extra, err := miekgdns.Dial("tcp", address)
	require.NoError(t, err)
	defer func() { _ = extra.Close() }()
	require.NoError(t, extra.SetDeadline(time.Now().Add(300*time.Millisecond)))
	require.NoError(t, extra.WriteMsg(request))
	_, err = extra.ReadMsg()
	var netError net.Error
	require.ErrorAs(t, err, &netError, "a connection over the limit must not be served")
	require.True(t, netError.Timeout(), "a connection over the limit must wait, got %v", err)

	require.NoError(t, held[0].Close())
	require.NoError(t, extra.SetDeadline(time.Now().Add(time.Second)))
	response, err := extra.ReadMsg()
	require.NoError(t, err, "a waiting connection must be served once a slot frees")
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
}

func TestServeTCPClosesConnectionAfterQueryLimit(t *testing.T) {
	handler, err := NewHandler(handlerOptions(&recordingResolver{}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	address, _ := serveOnFreeDNSAddress(t, ctx, handler)
	request := new(miekgdns.Msg)
	request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeSOA)

	conn, err := miekgdns.Dial("tcp", address)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	for query := range maxTCPQueriesPerConnection {
		require.NoError(t, conn.WriteMsg(request), "query %d", query)
		response, err := conn.ReadMsg()
		require.NoError(t, err, "query %d must be answered", query)
		require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	}
	_ = conn.WriteMsg(request)
	_, err = conn.ReadMsg()
	require.Error(t, err, "the connection must be closed after the query limit")
	var netError net.Error
	if errors.As(err, &netError) {
		require.False(t, netError.Timeout(), "the server must close the connection, not leave it idle")
	}
}

type temporaryAcceptError struct{}

func (temporaryAcceptError) Error() string   { return "too many open files" }
func (temporaryAcceptError) Timeout() bool   { return false }
func (temporaryAcceptError) Temporary() bool { return true }

// scriptedListener returns the scripted Accept errors, then a connection or,
// when the script is exhausted and repeat is set, the last error forever.
type scriptedListener struct {
	mutex    sync.Mutex
	errors   []error
	repeat   bool
	accepted int
	conn     net.Conn
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.accepted++
	if len(l.errors) == 0 {
		return l.conn, nil
	}
	err := l.errors[0]
	if !l.repeat || len(l.errors) > 1 {
		l.errors = l.errors[1:]
	}
	return nil, err
}

func (l *scriptedListener) acceptCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.accepted
}

func (l *scriptedListener) Close() error   { return nil }
func (l *scriptedListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestAcceptBackoffListener(t *testing.T) {
	t.Run("retries temporary errors with a warning", func(t *testing.T) {
		var logs bytes.Buffer
		client, server := net.Pipe()
		defer func() { _ = client.Close() }()
		defer func() { _ = server.Close() }()
		inner := &scriptedListener{errors: []error{temporaryAcceptError{}, temporaryAcceptError{}}, conn: server}
		listener := newAcceptBackoffListener(inner, slog.New(slog.NewTextHandler(&logs, nil)))

		conn, err := listener.Accept()
		require.NoError(t, err)
		require.Same(t, server, conn)
		require.Equal(t, 3, inner.acceptCount())
		require.Equal(t, 2, strings.Count(logs.String(), "level=WARN"), logs.String())
		require.Contains(t, logs.String(), "too many open files")
	})

	t.Run("passes other errors through without a warning", func(t *testing.T) {
		var logs bytes.Buffer
		inner := &scriptedListener{errors: []error{errInjectedAccept}}
		listener := newAcceptBackoffListener(inner, slog.New(slog.NewTextHandler(&logs, nil)))

		_, err := listener.Accept()
		require.ErrorIs(t, err, errInjectedAccept)
		require.Equal(t, 1, inner.acceptCount())
		require.Empty(t, logs.String())
	})

	t.Run("close interrupts the backoff", func(t *testing.T) {
		inner := &scriptedListener{errors: []error{temporaryAcceptError{}}, repeat: true}
		listener := newAcceptBackoffListener(inner, slog.New(slog.DiscardHandler))
		returned := make(chan error, 1)
		go func() {
			_, err := listener.Accept()
			returned <- err
		}()
		// Eight failures put the next backoff at 640ms.
		require.Eventually(t, func() bool { return inner.acceptCount() >= 8 }, 2*time.Second, time.Millisecond)
		require.NoError(t, listener.Close())
		select {
		case err := <-returned:
			require.ErrorIs(t, err, net.ErrClosed)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Close must interrupt the accept backoff")
		}
	})
}
