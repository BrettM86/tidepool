package dns

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func freeUDPAddress(t *testing.T) string {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	address := packet.LocalAddr().String()
	require.NoError(t, packet.Close())
	return address
}

func TestServeUDP(t *testing.T) {
	t.Run("answers handle TXT and releases socket on cancellation", func(t *testing.T) {
		resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
		handler, err := NewHandler(handlerOptions(resolver))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		address, done := serveOnFreeUDPAddress(t, ctx, handler)

		request := new(miekgdns.Msg)
		request.SetQuestion("_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT)
		response, _, err := (&miekgdns.Client{Net: "udp", Timeout: time.Second}).Exchange(request, address)
		require.NoError(t, err, "ServeUDP must receive the query on its bound loopback socket")
		require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
		require.True(t, response.Authoritative)
		require.Len(t, response.Answer, 1)
		txt, ok := response.Answer[0].(*miekgdns.TXT)
		require.True(t, ok)
		require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)

		cancel()
		requireClosedWithoutError(t, done)
		require.Eventually(t, func() bool {
			probe, err := net.ListenPacket("udp", address)
			if err != nil {
				return false
			}
			return probe.Close() == nil
		}, 2*time.Second, 10*time.Millisecond, "cancellation must release the UDP socket")
	})

	t.Run("occupied UDP address fails synchronously", func(t *testing.T) {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		defer packet.Close()
		handler, err := NewHandler(handlerOptions(&recordingResolver{}))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done, err := ServeUDP(ctx, packet.LocalAddr().String(), handler)
		require.Error(t, err)
		require.Nil(t, done)
	})

	t.Run("immediate cancellation closes bound socket", func(t *testing.T) {
		handler, err := NewHandler(handlerOptions(&recordingResolver{}))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		address, done := serveOnFreeUDPAddress(t, ctx, handler)
		probe, err := net.ListenPacket("udp", address)
		if err == nil {
			_ = probe.Close()
		}
		require.Error(t, err, "ServeUDP must bind before returning")
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

func serveOnFreeUDPAddress(t *testing.T, ctx context.Context, handler miekgdns.Handler) (string, <-chan error) {
	t.Helper()
	var (
		address string
		done    <-chan error
		err     error
	)
	for attempt := 0; attempt < 5; attempt++ {
		address = freeUDPAddress(t)
		done, err = ServeUDP(ctx, address, handler)
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
