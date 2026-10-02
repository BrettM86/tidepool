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
		DNSListen:      address,
		BridgeHostname: "Handles.Bridge.Example",
		DNSPublicIPv4:  netip.MustParseAddr("127.0.0.1"),
		DNSNameservers: []string{"ns-a.bridge.example", "ns-b.bridge.example"},
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
	defer listener.Close()
	address := listener.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{
		DNSListen:      address,
		BridgeHostname: "tdpl.example",
		DNSPublicIPv4:  netip.MustParseAddr("127.0.0.1"),
		DNSNameservers: []string{"ns1.tdpl.example", "ns2.tdpl.example"},
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
