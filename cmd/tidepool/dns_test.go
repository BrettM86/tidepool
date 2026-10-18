package main

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"tidepool/internal/config"
)

type dnsStartupResolver struct {
	handles map[string]string
}

func (r *dnsStartupResolver) ResolveHandle(_ context.Context, handle string) (string, error) {
	return r.handles[handle], nil
}

func freeStartupDNSAddress(t *testing.T) string {
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

func TestStartDNSServerUsesConfiguredZoneAndResolver(t *testing.T) {
	address := freeStartupDNSAddress(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{
		DNSListen:          address,
		DNSUDPResponseRate: 20,
		BridgeHostname:     "Handles.Bridge.Example",
		DNSPublicIPv4:      netip.MustParseAddr("127.0.0.1"),
		DNSNameservers:     []string{"ns-a.bridge.example", "ns-b.bridge.example"},
	}
	resolver := &dnsStartupResolver{handles: map[string]string{
		"bob.lemmy-world.handles.bridge.example": "did:plc:bob",
	}}
	before := uint32(time.Now().Unix())
	dnsErrors, err := startDNSServer(ctx, cfg, resolver, testLogger())
	require.NoError(t, err)
	require.NotNil(t, dnsErrors, "an enabled DNS server must report unexpected stops to run")

	client := &miekgdns.Client{Net: "udp", Timeout: time.Second}
	request := new(miekgdns.Msg)
	request.SetQuestion("_atproto.bob.lemmy-world.handles.bridge.example.", miekgdns.TypeTXT)
	response, _, err := client.Exchange(request, address)
	require.NoError(t, err, "startDNSServer must serve UDP on the configured address")
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.True(t, response.Authoritative)
	require.Len(t, response.Answer, 1)
	txt, ok := response.Answer[0].(*miekgdns.TXT)
	require.True(t, ok)
	require.Equal(t, []string{"did=did:plc:bob"}, txt.Txt)

	request.SetQuestion("bob.lemmy-world.handles.bridge.example.", miekgdns.TypeTXT)
	response, _, err = client.Exchange(request, address)
	after := uint32(time.Now().Unix())
	require.NoError(t, err)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.Empty(t, response.Answer)
	require.Len(t, response.Ns, 1)
	soa, ok := response.Ns[0].(*miekgdns.SOA)
	require.True(t, ok)
	require.Equal(t, "lemmy-world.handles.bridge.example.", soa.Hdr.Name)
	require.Equal(t, "ns-a.bridge.example.", soa.Ns)
	require.True(t, before <= soa.Serial && soa.Serial <= after,
		"SOA serial must be the process start time in Unix seconds: got %d, want [%d, %d]", soa.Serial, before, after)

	cancel()
	require.Eventually(t, func() bool {
		probe, err := net.ListenPacket("udp", address)
		if err != nil {
			return false
		}
		return probe.Close() == nil
	}, 2*time.Second, 10*time.Millisecond, "shutdown must release the configured socket")
	select {
	case err, open := <-dnsErrors:
		require.False(t, open, "cancellation is not a serve failure, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("the DNS error channel must close once the server stops")
	}
}

func TestStartDNSServerServesBothHandleFormatsOverUDPAndTCP(t *testing.T) {
	address := freeStartupDNSAddress(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{
		DNSListen:          address,
		DNSUDPResponseRate: 20,
		BridgeHostname:     "bridge.example",
		HandleZoneRoot:     "names.bridge.example",
		DNSPublicIPv4:      netip.MustParseAddr("127.0.0.1"),
		DNSNameservers:     []string{"ns-a.bridge.example", "ns-b.bridge.example"},
	}
	resolver := &dnsStartupResolver{handles: map[string]string{
		"bob.lemmy-world.bridge.example":         "did:plc:bob",
		"alice.lemmy.world.names.bridge.example": "did:plc:alice",
	}}
	dnsErrors, err := startDNSServer(ctx, cfg, resolver, testLogger())
	require.NoError(t, err)
	require.NotNil(t, dnsErrors)

	for _, network := range []string{"udp", "tcp"} {
		t.Run(network, func(t *testing.T) {
			client := &miekgdns.Client{Net: network, Timeout: time.Second}
			for _, tc := range []struct {
				name     string
				question string
				wantTXT  string
			}{
				{"old-format handle", "_atproto.bob.lemmy-world.bridge.example.", "did=did:plc:bob"},
				{"new-format handle", "_atproto.alice.lemmy.world.names.bridge.example.", "did=did:plc:alice"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := new(miekgdns.Msg)
					request.SetQuestion(tc.question, miekgdns.TypeTXT)
					response, _, err := client.Exchange(request, address)
					require.NoError(t, err)
					require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
					require.True(t, response.Authoritative)
					require.Len(t, response.Answer, 1)
					txt, ok := response.Answer[0].(*miekgdns.TXT)
					require.True(t, ok)
					require.Equal(t, []string{tc.wantTXT}, txt.Txt)
				})
			}
		})
	}

	cancel()
	select {
	case err, open := <-dnsErrors:
		require.False(t, open, "cancellation is not a serve failure, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("DNS server did not stop after cancellation")
	}
}

func TestStartDNSServerReleasesUDPWhenTCPBindFails(t *testing.T) {
	var listener net.Listener
	for attempt := 0; attempt < 10; attempt++ {
		candidate, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		packet, err := net.ListenPacket("udp", candidate.Addr().String())
		if err == nil {
			require.NoError(t, packet.Close())
			listener = candidate
			break
		}
		require.NoError(t, candidate.Close())
		if !errors.Is(err, syscall.EADDRINUSE) {
			require.NoError(t, err)
		}
	}
	require.NotNil(t, listener)
	defer func() { _ = listener.Close() }()
	address := listener.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{
		DNSListen:          address,
		DNSUDPResponseRate: 20,
		BridgeHostname:     "tdpl.example",
		DNSPublicIPv4:      netip.MustParseAddr("127.0.0.1"),
		DNSNameservers:     []string{"ns1.tdpl.example", "ns2.tdpl.example"},
	}
	done, err := startDNSServer(ctx, cfg, &dnsStartupResolver{}, testLogger())
	defer func() {
		cancel()
		if done != nil {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("DNS server did not stop after cancellation")
			}
		}
	}()
	require.Error(t, err, "occupied TCP port must fail startup synchronously")
	require.Nil(t, done)
	packet, err := net.ListenPacket("udp", address)
	require.NoError(t, err, "TCP bind failure must release UDP before context cancellation")
	require.NoError(t, packet.Close())
}

func TestStartDNSServerDisabledReturnsNilChannel(t *testing.T) {
	dnsErrors, err := startDNSServer(t.Context(), &config.Config{}, &dnsStartupResolver{}, testLogger())
	require.NoError(t, err)
	require.Nil(t, dnsErrors, "a nil channel keeps run's select from ever choosing the DNS case")
}

func TestStartDNSServerAppliesUDPResponseRate(t *testing.T) {
	address := freeStartupDNSAddress(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{
		DNSListen:          address,
		DNSUDPResponseRate: 1,
		BridgeHostname:     "tdpl.example",
		DNSPublicIPv4:      netip.MustParseAddr("127.0.0.1"),
		DNSNameservers:     []string{"ns1.tdpl.example", "ns2.tdpl.example"},
	}
	dnsErrors, err := startDNSServer(ctx, cfg, &dnsStartupResolver{}, testLogger())
	require.NoError(t, err)
	require.NotNil(t, dnsErrors)
	defer func() {
		cancel()
		select {
		case err, open := <-dnsErrors:
			require.False(t, open, "DNS server stopped with error: %v", err)
		case <-time.After(2 * time.Second):
			t.Error("DNS server did not stop after cancellation")
		}
	}()

	conn, err := net.Dial("udp", address)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	send := func(id uint16) {
		t.Helper()
		request := new(miekgdns.Msg)
		request.SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeSOA)
		request.Id = id
		packed, err := request.Pack()
		require.NoError(t, err)
		_, err = conn.Write(packed)
		require.NoError(t, err)
	}
	read := func() (*miekgdns.Msg, error) {
		buffer := make([]byte, 512)
		count, err := conn.Read(buffer)
		if err != nil {
			return nil, err
		}
		response := new(miekgdns.Msg)
		if err := response.Unpack(buffer[:count]); err != nil {
			return nil, err
		}
		return response, nil
	}
	send(100)
	first, err := read()
	require.NoError(t, err)
	require.Equal(t, uint16(100), first.Id)
	require.Equal(t, miekgdns.RcodeSuccess, first.Rcode)
	require.False(t, first.Truncated)
	require.Len(t, first.Answer, 1)
	require.Equal(t, miekgdns.TypeSOA, first.Answer[0].Header().Rrtype)

	for id := uint16(101); id <= 110; id++ {
		send(id)
	}
	for replies := 0; replies < 10; replies++ {
		response, err := read()
		require.NoError(t, err, "no TC reply arrived before the deadline (%d normal replies)", replies)
		require.GreaterOrEqual(t, response.Id, uint16(101))
		require.LessOrEqual(t, response.Id, uint16(110))
		if response.Truncated {
			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.Equal(t, (new(miekgdns.Msg)).SetQuestion("lemmy-world.tdpl.example.", miekgdns.TypeSOA).Question, response.Question)
			require.Empty(t, response.Answer)
			require.Empty(t, response.Ns)
			require.Empty(t, response.Extra)
			return
		}
		require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
		require.Len(t, response.Answer, 1)
		require.Equal(t, miekgdns.TypeSOA, response.Answer[0].Header().Rrtype)
	}
	t.Fatal("all 10 over-budget queries received normal answers; configured rate was not applied")
}
