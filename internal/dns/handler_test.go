package dns

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	internalerrors "tidepool/internal/errors"
)

type recordingDNSWriter struct {
	response *miekgdns.Msg
}

func (w *recordingDNSWriter) LocalAddr() net.Addr  { return nil }
func (w *recordingDNSWriter) RemoteAddr() net.Addr { return nil }
func (w *recordingDNSWriter) WriteMsg(msg *miekgdns.Msg) error {
	w.response = msg
	return nil
}
func (w *recordingDNSWriter) Write(data []byte) (int, error) {
	w.response = new(miekgdns.Msg)
	if err := w.response.Unpack(data); err != nil {
		return 0, err
	}
	return len(data), nil
}
func (w *recordingDNSWriter) Close() error        { return nil }
func (w *recordingDNSWriter) TsigStatus() error   { return nil }
func (w *recordingDNSWriter) TsigTimersOnly(bool) {}
func (w *recordingDNSWriter) Hijack()             {}

type recordingResolver struct {
	handles         map[string]string
	err             error
	asked           []string
	missingNotFound bool
}

func (r *recordingResolver) ResolveHandle(_ context.Context, handle string) (string, error) {
	r.asked = append(r.asked, handle)
	if r.err != nil {
		return "", r.err
	}
	did, found := r.handles[handle]
	if !found && r.missingNotFound {
		return "", internalerrors.NewNotFoundError("handle", handle)
	}
	return did, nil
}

func handlerOptions(resolver *recordingResolver) Options {
	return Options{
		ZoneRoot:        "tdpl.example",
		Nameservers:     []string{"ns1.tdpl.example", "ns2.tdpl.example"},
		PublicIPv4:      netip.MustParseAddr("192.0.2.1"),
		Serial:          2024100101,
		Resolver:        resolver,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		UDPResponseRate: 20,
	}
}

func TestNewHandlerOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Options)
	}{
		{"missing zone root", func(o *Options) { o.ZoneRoot = "" }},
		{"missing public IPv4", func(o *Options) { o.PublicIPv4 = netip.Addr{} }},
		{"IPv6 as public IPv4", func(o *Options) { o.PublicIPv4 = netip.MustParseAddr("2001:db8::1") }},
		{"missing nameservers", func(o *Options) { o.Nameservers = nil }},
		{"missing resolver", func(o *Options) { o.Resolver = nil }},
		{"zone root with port", func(o *Options) { o.ZoneRoot = "tdpl.io:443" }},
		{"zone root with empty label", func(o *Options) { o.ZoneRoot = "tdpl..example" }},
		{"zone root is the DNS root", func(o *Options) { o.ZoneRoot = "." }},
		{"nameserver with empty label", func(o *Options) { o.Nameservers = []string{"ns1..example"} }},
		{"nameserver label over 63 bytes", func(o *Options) { o.Nameservers = []string{strings.Repeat("a", 64) + ".example"} }},
		{"nameserver with port", func(o *Options) { o.Nameservers = []string{"ns1.tdpl.example:53"} }},
		{"second nameserver invalid", func(o *Options) { o.Nameservers = []string{"ns1.tdpl.example", "ns2..example"} }},
		{"zero UDP response rate", func(o *Options) { o.UDPResponseRate = 0 }},
		{"negative UDP response rate", func(o *Options) { o.UDPResponseRate = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := handlerOptions(&recordingResolver{})
			tc.change(&options)
			_, err := NewHandler(options)
			require.Error(t, err)
		})
	}

	handler, err := NewHandler(handlerOptions(&recordingResolver{}))
	require.NoError(t, err)
	require.NotNil(t, handler)

	options := handlerOptions(&recordingResolver{})
	options.ZoneRoot = "  TDPL.Example \t"
	options.Nameservers = []string{" NS1.Tdpl.Example. ", "ns2.tdpl.example"}
	handler, err = NewHandler(options)
	require.NoError(t, err)
	require.Equal(t, "tdpl.example.", handler.zoneRoot, "zone root must be stored trimmed, lowercased and fully qualified")
	require.Equal(t, []string{"ns1.tdpl.example.", "ns2.tdpl.example."}, handler.nameservers, "nameservers must be stored trimmed, lowercased and fully qualified")
}

func TestHandlerNameClassification(t *testing.T) {
	for _, tc := range []struct {
		name              string
		zoneRoot          string
		nameservers       []string
		question          string
		questionType      uint16
		handles           map[string]string
		resolverError     error
		wantCode          int
		wantAuthoritative bool
		wantTXT           string
		wantSOAOwner      string
		wantSOAMailbox    string
		wantLookup        []string
	}{
		{
			name: "B2 mixed-case handle TXT", question: "_ATPROTO.Alice.Lemmy-World.tdpl.example.", questionType: miekgdns.TypeTXT,
			handles:  map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"},
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true, wantTXT: "did=did:plc:abc",
			wantLookup: []string{"alice.lemmy-world.tdpl.example"},
		},
		{
			name: "B2 mixed-case three-label root", zoneRoot: "Handles.Bridge.Example", nameservers: []string{"ns-a.bridge.example", "ns-b.bridge.example"},
			question: "_atproto.bob.lemmy-world.handles.bridge.example.", questionType: miekgdns.TypeTXT,
			handles:  map[string]string{"bob.lemmy-world.handles.bridge.example": "did:plc:bob"},
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true, wantTXT: "did=did:plc:bob",
			wantLookup: []string{"bob.lemmy-world.handles.bridge.example"},
		},
		{
			name: "B3 unknown handle", question: "_atproto.unknown.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			resolverError: internalerrors.NewNotFoundError("handle", "unknown.lemmy-world.tdpl.example"),
			wantCode:      miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
			wantLookup: []string{"unknown.lemmy-world.tdpl.example"},
		},
		{
			name: "B3 tombstoned handle", question: "_atproto.deleted.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			resolverError: internalerrors.NewNotFoundError("handle", "deleted.lemmy-world.tdpl.example"),
			wantCode:      miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
			wantLookup: []string{"deleted.lemmy-world.tdpl.example"},
		},
		{
			name: "B3 invalid handle", question: "_atproto.invalid.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			resolverError: internalerrors.NewValidationError("handle", "invalid handle"),
			wantCode:      miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
			wantLookup: []string{"invalid.lemmy-world.tdpl.example"},
		},
		{
			name: "B4 two-label TXT", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 two-label _atproto TXT", question: "_atproto.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 _atproto label apex", question: "_atproto.tdpl.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "_atproto.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 four-label TXT", question: "x._atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 non-handle three-label TXT", question: "other.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 MX at handle name", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeMX,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 CNAME at handle name", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeCNAME,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 ANY at handle name", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeANY,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.tdpl.example.", wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B4 NODATA under three-label root", zoneRoot: "Handles.Bridge.Example", nameservers: []string{"ns-a.bridge.example", "ns-b.bridge.example"},
			question: "alice.lemmy-world.handles.bridge.example.", questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: "lemmy-world.handles.bridge.example.", wantSOAMailbox: "hostmaster.handles.bridge.example.",
		},
		{
			name: "B4 escaped dot makes two labels", question: `_atproto.a\.b.tdpl.example.`, questionType: miekgdns.TypeTXT,
			wantCode: miekgdns.RcodeSuccess, wantAuthoritative: true,
			wantSOAOwner: `a\.b.tdpl.example.`, wantSOAMailbox: "hostmaster.tdpl.example.",
		},
		{
			name: "B5 store unreachable", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeTXT,
			resolverError: errors.New("store unreachable"), wantCode: miekgdns.RcodeServerFailure, wantAuthoritative: true,
			wantLookup: []string{"alice.lemmy-world.tdpl.example"},
		},
		{
			name: "B6 zone root TXT", question: "tdpl.example.", questionType: miekgdns.TypeTXT, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 zone root A", question: "tdpl.example.", questionType: miekgdns.TypeA, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 unrelated TXT", question: "example.org.", questionType: miekgdns.TypeTXT, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 unrelated A", question: "example.org.", questionType: miekgdns.TypeA, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 suffix spoof TXT", question: "tdpl.example.evil.org.", questionType: miekgdns.TypeTXT, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 suffix spoof A", question: "tdpl.example.evil.org.", questionType: miekgdns.TypeA, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 escaped dot before zone root TXT", question: `_atproto.alice.b\.tdpl.example.`, questionType: miekgdns.TypeTXT, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 partial-label suffix TXT", question: "nottdpl.example.", questionType: miekgdns.TypeTXT, wantCode: miekgdns.RcodeRefused,
		},
		{
			name: "B6 partial-label suffix A", question: "nottdpl.example.", questionType: miekgdns.TypeA, wantCode: miekgdns.RcodeRefused,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{handles: tc.handles, err: tc.resolverError}
			options := handlerOptions(resolver)
			if tc.zoneRoot != "" {
				options.ZoneRoot = tc.zoneRoot
				options.Nameservers = tc.nameservers
			}
			handler, err := NewHandler(options)
			require.NoError(t, err)
			require.NotNil(t, handler)

			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			request.RecursionDesired = true
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response, "no response written")
			response := writer.response
			require.Equal(t, tc.wantCode, response.Rcode)
			require.False(t, response.RecursionAvailable, "RA must be clear even when RD is set")
			if tc.wantAuthoritative {
				require.True(t, response.Authoritative, "in-zone responses must set AA")
			}
			require.Equal(t, tc.wantLookup, resolver.asked)

			if tc.wantTXT != "" {
				require.Len(t, response.Answer, 1)
				txt, ok := response.Answer[0].(*miekgdns.TXT)
				require.True(t, ok, "answer must be TXT, got %T", response.Answer[0])
				require.True(t, strings.EqualFold(tc.question, txt.Hdr.Name), "TXT owner = %s", txt.Hdr.Name)
				require.Equal(t, uint16(miekgdns.ClassINET), txt.Hdr.Class)
				require.Equal(t, uint32(300), txt.Hdr.Ttl)
				require.Equal(t, []string{tc.wantTXT}, txt.Txt)
			} else {
				require.Empty(t, response.Answer)
			}

			if tc.wantSOAOwner != "" {
				require.Len(t, response.Ns, 1)
				soa, ok := response.Ns[0].(*miekgdns.SOA)
				require.True(t, ok, "authority must be SOA, got %T", response.Ns[0])
				require.Equal(t, tc.wantSOAOwner, soa.Hdr.Name)
				require.Equal(t, uint16(miekgdns.ClassINET), soa.Hdr.Class)
				require.Equal(t, uint32(3600), soa.Hdr.Ttl)
				if tc.zoneRoot == "" {
					require.Equal(t, "ns1.tdpl.example.", soa.Ns)
				} else {
					require.Equal(t, "ns-a.bridge.example.", soa.Ns)
				}
				require.Equal(t, tc.wantSOAMailbox, soa.Mbox)
				require.Equal(t, uint32(2024100101), soa.Serial)
				require.Equal(t, uint32(300), soa.Minttl)
			}
			if tc.wantCode == miekgdns.RcodeRefused {
				require.Empty(t, response.Ns)
				require.Empty(t, response.Extra)
			}
		})
	}
}

func TestHandlerApexSOAAndNS(t *testing.T) {
	for _, tc := range []struct {
		name          string
		question      string
		questionType  uint16
		wantAnswer    int
		wantAuthority int
	}{
		{"B1 apex SOA", "lemmy-world.tdpl.example.", miekgdns.TypeSOA, 1, 0},
		{"B1 nested SOA NODATA", "alice.lemmy-world.tdpl.example.", miekgdns.TypeSOA, 0, 1},
		{"B2 apex NS", "lemmy-world.tdpl.example.", miekgdns.TypeNS, 2, 0},
		{"B2 nested NS NODATA", "alice.lemmy-world.tdpl.example.", miekgdns.TypeNS, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{}
			handler, err := NewHandler(handlerOptions(resolver))
			require.NoError(t, err)
			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			response := writer.response
			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.True(t, response.Authoritative)
			require.Len(t, response.Answer, tc.wantAnswer)
			require.Len(t, response.Ns, tc.wantAuthority)
			require.Empty(t, response.Extra)
			require.Empty(t, resolver.asked)

			if tc.questionType == miekgdns.TypeNS && tc.wantAnswer != 0 {
				for index, wantTarget := range []string{"ns1.tdpl.example.", "ns2.tdpl.example."} {
					ns, ok := response.Answer[index].(*miekgdns.NS)
					require.True(t, ok, "answer %d must be NS, got %T", index, response.Answer[index])
					require.Equal(t, "lemmy-world.tdpl.example.", ns.Hdr.Name)
					require.Equal(t, uint16(miekgdns.ClassINET), ns.Hdr.Class)
					require.Equal(t, uint32(3600), ns.Hdr.Ttl)
					require.Equal(t, wantTarget, ns.Ns)
				}
				return
			}

			var record miekgdns.RR
			if tc.wantAnswer != 0 {
				record = response.Answer[0]
			} else {
				record = response.Ns[0]
			}
			soa, ok := record.(*miekgdns.SOA)
			require.True(t, ok, "expected SOA, got %T", record)
			require.Equal(t, "lemmy-world.tdpl.example.", soa.Hdr.Name)
			require.Equal(t, uint16(miekgdns.ClassINET), soa.Hdr.Class)
			require.Equal(t, uint32(3600), soa.Hdr.Ttl)
			require.Equal(t, "ns1.tdpl.example.", soa.Ns)
			require.Equal(t, "hostmaster.tdpl.example.", soa.Mbox)
			require.Equal(t, uint32(2024100101), soa.Serial)
			require.Equal(t, uint32(300), soa.Minttl)
		})
	}
}

func TestHandlerAddressAnswersAndTransferRefusal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		question     string
		questionType uint16
		publicIPv6   bool
		wantAddress  string
		wantSOA      bool
		wantRefused  bool
	}{
		{name: "B1 A label apex", question: "lemmy-world.tdpl.example.", questionType: miekgdns.TypeA, wantAddress: "192.0.2.10"},
		{name: "B1 A known handle", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeA, wantAddress: "192.0.2.10"},
		{name: "B1 A unknown handle", question: "nobody.lemmy-world.tdpl.example.", questionType: miekgdns.TypeA, wantAddress: "192.0.2.10"},
		{name: "B1 A atproto name is NODATA", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeA, wantSOA: true},
		{name: "B1 A four labels is NODATA", question: "x.y.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeA, wantSOA: true},
		{name: "B2 AAAA label apex", question: "lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, publicIPv6: true, wantAddress: "2001:db8::10"},
		{name: "B2 AAAA handle", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, publicIPv6: true, wantAddress: "2001:db8::10"},
		{name: "B2 AAAA atproto name is NODATA", question: "_atproto.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, publicIPv6: true, wantSOA: true},
		{name: "B2 AAAA four labels is NODATA", question: "x.y.alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, publicIPv6: true, wantSOA: true},
		{name: "B2 AAAA label apex without IPv6 is NODATA", question: "lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, wantSOA: true},
		{name: "B2 AAAA handle without IPv6 is NODATA", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeAAAA, wantSOA: true},
		{name: "B3 AXFR label apex refused", question: "lemmy-world.tdpl.example.", questionType: miekgdns.TypeAXFR, wantRefused: true},
		{name: "B3 IXFR label apex refused", question: "lemmy-world.tdpl.example.", questionType: miekgdns.TypeIXFR, wantRefused: true},
		{name: "B3 AXFR handle refused", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeAXFR, wantRefused: true},
		{name: "B3 IXFR handle refused", question: "alice.lemmy-world.tdpl.example.", questionType: miekgdns.TypeIXFR, wantRefused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:alice"}}
			options := handlerOptions(resolver)
			options.PublicIPv4 = netip.MustParseAddr("192.0.2.10")
			if tc.publicIPv6 {
				options.PublicIPv6 = netip.MustParseAddr("2001:db8::10")
			}
			handler, err := NewHandler(options)
			require.NoError(t, err)

			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response, "no response written")
			response := writer.response
			require.Empty(t, resolver.asked, "address and transfer queries must not resolve handles")
			require.Empty(t, response.Extra)
			if tc.wantRefused {
				require.Equal(t, miekgdns.RcodeRefused, response.Rcode)
				require.Empty(t, response.Answer)
				require.Empty(t, response.Ns)
				return
			}

			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.True(t, response.Authoritative)
			if tc.wantSOA {
				require.Empty(t, response.Answer)
				require.Len(t, response.Ns, 1)
				soa, ok := response.Ns[0].(*miekgdns.SOA)
				require.True(t, ok, "authority must be SOA, got %T", response.Ns[0])
				require.Equal(t, "lemmy-world.tdpl.example.", soa.Hdr.Name)
				require.Equal(t, miekgdns.TypeSOA, soa.Hdr.Rrtype)
				return
			}

			require.Empty(t, response.Ns)
			require.Len(t, response.Answer, 1)
			switch tc.questionType {
			case miekgdns.TypeA:
				record, ok := response.Answer[0].(*miekgdns.A)
				require.True(t, ok, "answer must be A, got %T", response.Answer[0])
				require.Equal(t, tc.question, record.Hdr.Name)
				require.Equal(t, uint16(miekgdns.ClassINET), record.Hdr.Class)
				require.Equal(t, uint32(300), record.Hdr.Ttl)
				require.Equal(t, tc.wantAddress, record.A.String())
			case miekgdns.TypeAAAA:
				record, ok := response.Answer[0].(*miekgdns.AAAA)
				require.True(t, ok, "answer must be AAAA, got %T", response.Answer[0])
				require.Equal(t, tc.question, record.Hdr.Name)
				require.Equal(t, uint16(miekgdns.ClassINET), record.Hdr.Class)
				require.Equal(t, uint32(300), record.Hdr.Ttl)
				require.Equal(t, tc.wantAddress, record.AAAA.String())
			}
		})
	}
}

func TestHandlerHandleZoneTXTAtAnyDepth(t *testing.T) {
	for _, tc := range []struct {
		name     string
		question string
		handle   string
		did      string
		wantTXT  string
	}{
		{"two-label instance host", "_atproto.alice.lemmy.world.ap.tdpl.example.", "alice.lemmy.world.ap.tdpl.example", "did:plc:alice", "did=did:plc:alice"},
		{"single-label instance host", "_atproto.bob.example.ap.tdpl.example.", "bob.example.ap.tdpl.example", "did:plc:bob", "did=did:plc:bob"},
		{"deep instance host", "_atproto.carol.a.b.c.ap.tdpl.example.", "carol.a.b.c.ap.tdpl.example", "did:plc:carol", "did=did:plc:carol"},
		{"mixed-case query", "_ATPROTO.Alice.Lemmy.World.AP.tdpl.example.", "alice.lemmy.world.ap.tdpl.example", "did:plc:alice", "did=did:plc:alice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{handles: map[string]string{tc.handle: tc.did}}
			options := handlerOptions(resolver)
			options.HandleZoneRoot = "ap.tdpl.example"
			handler, err := NewHandler(options)
			require.NoError(t, err)

			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, miekgdns.TypeTXT)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			require.Equal(t, miekgdns.RcodeSuccess, writer.response.Rcode)
			require.True(t, writer.response.Authoritative)
			require.Equal(t, []string{tc.handle}, resolver.asked)
			require.Len(t, writer.response.Answer, 1)
			txt, ok := writer.response.Answer[0].(*miekgdns.TXT)
			require.True(t, ok)
			require.Equal(t, tc.question, txt.Hdr.Name)
			require.Equal(t, uint32(300), txt.Hdr.Ttl)
			require.Equal(t, []string{tc.wantTXT}, txt.Txt)
		})
	}
}

func TestHandlerHandleZoneNegativeTXT(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lookupErr error
		wantCode  int
		wantSOA   bool
	}{
		{"unknown handle", internalerrors.NewNotFoundError("handle", "unknown.lemmy.world.ap.tdpl.example"), miekgdns.RcodeSuccess, true},
		{"invalid handle", internalerrors.NewValidationError("handle", "invalid handle"), miekgdns.RcodeSuccess, true},
		{"store down", errors.New("store down"), miekgdns.RcodeServerFailure, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{err: tc.lookupErr}
			options := handlerOptions(resolver)
			options.HandleZoneRoot = "ap.tdpl.example"
			handler, err := NewHandler(options)
			require.NoError(t, err)

			request := new(miekgdns.Msg)
			request.SetQuestion("_atproto.unknown.lemmy.world.ap.tdpl.example.", miekgdns.TypeTXT)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			require.Equal(t, tc.wantCode, writer.response.Rcode)
			require.Equal(t, []string{"unknown.lemmy.world.ap.tdpl.example"}, resolver.asked)
			require.Empty(t, writer.response.Answer)
			if !tc.wantSOA {
				require.Empty(t, writer.response.Ns)
				return
			}
			require.Len(t, writer.response.Ns, 1)
			soa, ok := writer.response.Ns[0].(*miekgdns.SOA)
			require.True(t, ok)
			require.Equal(t, "ap.tdpl.example.", soa.Hdr.Name)
			require.Equal(t, "ns1.tdpl.example.", soa.Ns)
			require.Equal(t, uint32(3600), soa.Hdr.Ttl)
			require.Equal(t, uint32(300), soa.Minttl)
		})
	}
}

func TestHandlerHandleZoneAddressesAtAnyDepth(t *testing.T) {
	for _, tc := range []struct {
		name         string
		question     string
		questionType uint16
		publicIPv6   bool
		wantAddress  string
	}{
		{"root A", "ap.tdpl.example.", miekgdns.TypeA, true, "192.0.2.1"},
		{"one level A", "x.ap.tdpl.example.", miekgdns.TypeA, true, "192.0.2.1"},
		{"handle A", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeA, true, "192.0.2.1"},
		{"deep handle A", "carol.a.b.c.ap.tdpl.example.", miekgdns.TypeA, true, "192.0.2.1"},
		{"root AAAA", "ap.tdpl.example.", miekgdns.TypeAAAA, true, "2001:db8::1"},
		{"one level AAAA", "x.ap.tdpl.example.", miekgdns.TypeAAAA, true, "2001:db8::1"},
		{"handle AAAA", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeAAAA, true, "2001:db8::1"},
		{"deep handle AAAA", "carol.a.b.c.ap.tdpl.example.", miekgdns.TypeAAAA, true, "2001:db8::1"},
		{"handle AAAA without IPv6", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeAAAA, false, ""},
		{"root AAAA without IPv6", "ap.tdpl.example.", miekgdns.TypeAAAA, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{}
			options := handlerOptions(resolver)
			options.HandleZoneRoot = "ap.tdpl.example"
			if tc.publicIPv6 {
				options.PublicIPv6 = netip.MustParseAddr("2001:db8::1")
			}
			handler, err := NewHandler(options)
			require.NoError(t, err)

			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			require.Equal(t, miekgdns.RcodeSuccess, writer.response.Rcode)
			require.True(t, writer.response.Authoritative)
			require.Empty(t, resolver.asked)
			if tc.wantAddress == "" {
				require.Empty(t, writer.response.Answer)
				require.Len(t, writer.response.Ns, 1)
				soa, ok := writer.response.Ns[0].(*miekgdns.SOA)
				require.True(t, ok, "authority must be SOA, got %T", writer.response.Ns[0])
				require.Equal(t, "ap.tdpl.example.", soa.Hdr.Name)
				require.Equal(t, uint32(3600), soa.Hdr.Ttl)
				require.Equal(t, uint32(300), soa.Minttl)
				return
			}
			require.Len(t, writer.response.Answer, 1)
			switch tc.questionType {
			case miekgdns.TypeA:
				record, ok := writer.response.Answer[0].(*miekgdns.A)
				require.True(t, ok)
				require.Equal(t, tc.question, record.Hdr.Name)
				require.Equal(t, uint32(300), record.Hdr.Ttl)
				require.Equal(t, tc.wantAddress, record.A.String())
			case miekgdns.TypeAAAA:
				record, ok := writer.response.Answer[0].(*miekgdns.AAAA)
				require.True(t, ok)
				require.Equal(t, tc.question, record.Hdr.Name)
				require.Equal(t, uint32(300), record.Hdr.Ttl)
				require.Equal(t, tc.wantAddress, record.AAAA.String())
			}
		})
	}
}

func TestHandlerHandleZoneNonLookupAnswers(t *testing.T) {
	for _, tc := range []struct {
		name, question  string
		questionType    uint16
		wantCode        int
		wantSOA, wantNS bool
	}{
		{"root SOA", "ap.tdpl.example.", miekgdns.TypeSOA, miekgdns.RcodeSuccess, true, false},
		{"mixed-case root SOA", "AP.Tdpl.Example.", miekgdns.TypeSOA, miekgdns.RcodeSuccess, true, false},
		{"root NS", "ap.tdpl.example.", miekgdns.TypeNS, miekgdns.RcodeSuccess, false, true},
		{"one label SOA", "x.ap.tdpl.example.", miekgdns.TypeSOA, miekgdns.RcodeSuccess, false, false},
		{"instance SOA", "lemmy.world.ap.tdpl.example.", miekgdns.TypeSOA, miekgdns.RcodeSuccess, false, false},
		{"handle SOA", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeSOA, miekgdns.RcodeSuccess, false, false},
		{"one label NS", "x.ap.tdpl.example.", miekgdns.TypeNS, miekgdns.RcodeSuccess, false, false},
		{"instance NS", "lemmy.world.ap.tdpl.example.", miekgdns.TypeNS, miekgdns.RcodeSuccess, false, false},
		{"handle NS", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeNS, miekgdns.RcodeSuccess, false, false},
		{"handle MX", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeMX, miekgdns.RcodeSuccess, false, false},
		{"root MX", "ap.tdpl.example.", miekgdns.TypeMX, miekgdns.RcodeSuccess, false, false},
		{"handle TXT without atproto", "alice.lemmy.world.ap.tdpl.example.", miekgdns.TypeTXT, miekgdns.RcodeSuccess, false, false},
		{"root atproto TXT", "_atproto.ap.tdpl.example.", miekgdns.TypeTXT, miekgdns.RcodeSuccess, false, false},
		{"root AXFR", "ap.tdpl.example.", miekgdns.TypeAXFR, miekgdns.RcodeRefused, false, false},
		{"outside A", "example.com.", miekgdns.TypeA, miekgdns.RcodeRefused, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &recordingResolver{}
			options := handlerOptions(resolver)
			options.HandleZoneRoot = "ap.tdpl.example"
			handler, err := NewHandler(options)
			require.NoError(t, err)
			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			response := writer.response
			require.Equal(t, tc.wantCode, response.Rcode)
			require.Empty(t, resolver.asked)
			if tc.wantCode == miekgdns.RcodeRefused {
				require.Empty(t, response.Answer)
				require.Empty(t, response.Ns)
				return
			}
			require.True(t, response.Authoritative)
			switch {
			case tc.wantSOA:
				require.Len(t, response.Answer, 1)
				soa, ok := response.Answer[0].(*miekgdns.SOA)
				require.True(t, ok, "expected SOA, got %T", response.Answer[0])
				require.Equal(t, "ap.tdpl.example.", soa.Hdr.Name)
				require.Equal(t, uint32(3600), soa.Hdr.Ttl)
				require.Equal(t, "ns1.tdpl.example.", soa.Ns)
				require.Equal(t, uint32(300), soa.Minttl)
				require.Empty(t, response.Ns)
			case tc.wantNS:
				require.Len(t, response.Answer, 2)
				for i, target := range []string{"ns1.tdpl.example.", "ns2.tdpl.example."} {
					ns, ok := response.Answer[i].(*miekgdns.NS)
					require.True(t, ok, "expected NS, got %T", response.Answer[i])
					require.Equal(t, "ap.tdpl.example.", ns.Hdr.Name)
					require.Equal(t, uint32(3600), ns.Hdr.Ttl)
					require.Equal(t, target, ns.Ns)
				}
				require.Empty(t, response.Ns)
			default:
				require.Empty(t, response.Answer)
				require.Len(t, response.Ns, 1, "NODATA must carry the handle zone SOA in authority")
				soa, ok := response.Ns[0].(*miekgdns.SOA)
				require.True(t, ok, "authority must be SOA, got %T", response.Ns[0])
				require.Equal(t, "ap.tdpl.example.", soa.Hdr.Name)
				require.Equal(t, uint32(3600), soa.Hdr.Ttl)
				require.Equal(t, uint32(300), soa.Minttl)
			}
		})
	}
}

func TestHandlerMultiLabelHandleZoneRoot(t *testing.T) {
	resolver := &recordingResolver{handles: map[string]string{"dave.host.handles.ap.tdpl.example": "did:plc:dave"}}
	options := handlerOptions(resolver)
	options.HandleZoneRoot = "  Handles.AP.tdpl.example "
	handler, err := NewHandler(options)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, question string
		questionType   uint16
	}{
		{"SOA", "handles.ap.tdpl.example.", miekgdns.TypeSOA},
		{"TXT", "_atproto.dave.host.handles.ap.tdpl.example.", miekgdns.TypeTXT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := new(miekgdns.Msg)
			request.SetQuestion(tc.question, tc.questionType)
			writer := &recordingDNSWriter{}
			handler.ServeDNS(writer, request)
			require.NotNil(t, writer.response)
			require.Equal(t, miekgdns.RcodeSuccess, writer.response.Rcode)
			require.True(t, writer.response.Authoritative)
			require.Len(t, writer.response.Answer, 1)
			switch tc.questionType {
			case miekgdns.TypeSOA:
				soa, ok := writer.response.Answer[0].(*miekgdns.SOA)
				require.True(t, ok, "expected SOA, got %T", writer.response.Answer[0])
				require.Equal(t, "handles.ap.tdpl.example.", soa.Hdr.Name)
				require.Equal(t, "ns1.tdpl.example.", soa.Ns)
				require.Equal(t, uint32(3600), soa.Hdr.Ttl)
				require.Equal(t, uint32(300), soa.Minttl)
				require.Empty(t, resolver.asked)
			case miekgdns.TypeTXT:
				txt, ok := writer.response.Answer[0].(*miekgdns.TXT)
				require.True(t, ok, "expected TXT, got %T", writer.response.Answer[0])
				require.Equal(t, "_atproto.dave.host.handles.ap.tdpl.example.", txt.Hdr.Name)
				require.Equal(t, uint32(300), txt.Hdr.Ttl)
				require.Equal(t, []string{"did=did:plc:dave"}, txt.Txt)
				require.Equal(t, []string{"dave.host.handles.ap.tdpl.example"}, resolver.asked)
			}
		})
	}
}

func TestHandlerPerInstanceAnswersUnchangedByHandleZone(t *testing.T) {
	handles := map[string]string{
		"alice.lemmy-world.tdpl.example": "did:plc:alice",
		"x.notap.tdpl.example":           "did:plc:x",
	}
	baselineOptions := handlerOptions(&recordingResolver{handles: handles, missingNotFound: true})
	baselineOptions.PublicIPv6 = netip.MustParseAddr("2001:db8::1")
	baseline, err := NewHandler(baselineOptions)
	require.NoError(t, err)
	withZoneOptions := handlerOptions(&recordingResolver{handles: handles, missingNotFound: true})
	withZoneOptions.PublicIPv6 = netip.MustParseAddr("2001:db8::1")
	withZoneOptions.HandleZoneRoot = "ap.tdpl.example"
	withZone, err := NewHandler(withZoneOptions)
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		questionType  uint16
		checkSOAOwner bool
	}{
		{"_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT, false},
		{"_atproto.unknown.lemmy-world.tdpl.example.", miekgdns.TypeTXT, false},
		{"lemmy-world.tdpl.example.", miekgdns.TypeSOA, false},
		{"lemmy-world.tdpl.example.", miekgdns.TypeNS, false},
		{"lemmy-world.tdpl.example.", miekgdns.TypeA, false},
		{"lemmy-world.tdpl.example.", miekgdns.TypeAAAA, false},
		{"alice.lemmy-world.tdpl.example.", miekgdns.TypeA, false},
		{"alice.lemmy-world.tdpl.example.", miekgdns.TypeAAAA, false},
		{"alice.lemmy-world.tdpl.example.", miekgdns.TypeMX, false},
		{"notap.tdpl.example.", miekgdns.TypeSOA, true},
		{"_atproto.x.notap.tdpl.example.", miekgdns.TypeTXT, false},
	} {
		t.Run(miekgdns.TypeToString[tc.questionType]+" "+tc.name, func(t *testing.T) {
			request := new(miekgdns.Msg)
			request.SetQuestion(tc.name, tc.questionType)
			request.Id = 1234
			first, second := &recordingDNSWriter{}, &recordingDNSWriter{}
			baseline.ServeDNS(first, request.Copy())
			withZone.ServeDNS(second, request.Copy())
			require.NotNil(t, first.response)
			require.NotNil(t, second.response)
			require.Equal(t, first.response.String(), second.response.String())
			if tc.checkSOAOwner {
				require.Len(t, second.response.Answer, 1)
				soa, ok := second.response.Answer[0].(*miekgdns.SOA)
				require.True(t, ok, "expected SOA, got %T", second.response.Answer[0])
				require.Equal(t, "notap.tdpl.example.", soa.Hdr.Name)
			}
		})
	}
}

type failingDNSWriter struct {
	recordingDNSWriter
	writeErrors []error
	written     []*miekgdns.Msg
}

func (w *failingDNSWriter) WriteMsg(msg *miekgdns.Msg) error {
	w.written = append(w.written, msg)
	if len(w.writeErrors) == 0 {
		return nil
	}
	err := w.writeErrors[0]
	w.writeErrors = w.writeErrors[1:]
	return err
}

func TestHandlerLogsWriteFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		writeErrors  []error
		wantLevel    string
		wantMessages int
		wantRetry    bool
		requestEDNS  bool
	}{
		{
			name:        "pack failure logs error and answers SERVFAIL",
			writeErrors: []error{miekgdns.ErrRdata},
			wantLevel:   "level=ERROR",
			wantRetry:   true,
		},
		{
			name:        "pack failure of an EDNS query answers SERVFAIL with an OPT record",
			writeErrors: []error{miekgdns.ErrRdata},
			wantLevel:   "level=ERROR",
			wantRetry:   true,
			requestEDNS: true,
		},
		{
			name:        "socket failure logs warning",
			writeErrors: []error{&net.OpError{Op: "write", Net: "udp", Err: errors.New("network unreachable")}},
			wantLevel:   "level=WARN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			options := handlerOptions(&recordingResolver{})
			options.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			handler, err := NewHandler(options)
			require.NoError(t, err)

			request := new(miekgdns.Msg)
			request.SetQuestion("alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT)
			if tc.requestEDNS {
				request.SetEdns0(4096, false)
			}
			writer := &failingDNSWriter{writeErrors: tc.writeErrors}
			handler.ServeDNS(writer, request)

			require.Contains(t, logs.String(), tc.wantLevel, "write failure must be logged")
			require.Contains(t, logs.String(), tc.writeErrors[0].Error())
			if tc.wantRetry {
				require.Len(t, writer.written, 2, "an unpackable response must be replaced by SERVFAIL")
				failure := writer.written[1]
				require.Equal(t, miekgdns.RcodeServerFailure, failure.Rcode)
				require.True(t, failure.Response)
				require.Equal(t, request.Id, failure.Id)
				require.Equal(t, request.Question, failure.Question)
				require.Empty(t, failure.Answer)
				require.Empty(t, failure.Ns)
				if tc.requestEDNS {
					require.Len(t, failure.Extra, 1, "the SERVFAIL must carry only the OPT record")
					opt := failure.IsEdns0()
					require.NotNil(t, opt, "an EDNS query's SERVFAIL must carry an OPT record")
					require.Equal(t, uint16(advertisedUDPPayloadSize), opt.UDPSize())
				} else {
					require.Empty(t, failure.Extra)
				}
			} else {
				require.Len(t, writer.written, 1, "a socket failure must not be retried")
			}
		})
	}
}

type blockingResolver struct {
	mutex   sync.Mutex
	asked   int
	entered chan struct{}
	release chan struct{}
}

func (r *blockingResolver) ResolveHandle(ctx context.Context, _ string) (string, error) {
	r.mutex.Lock()
	r.asked++
	r.mutex.Unlock()
	r.entered <- struct{}{}
	select {
	case <-r.release:
		return "did:plc:abc", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (r *blockingResolver) askedCount() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.asked
}

func TestHandlerLimitsConcurrentHandleLookups(t *testing.T) {
	for _, tc := range []struct {
		name     string
		question string
	}{
		{"old-format overflow", "_atproto.alice.lemmy-world.tdpl.example."},
		{"new-format overflow", "_atproto.alice.lemmy.world.ap.tdpl.example."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &blockingResolver{
				entered: make(chan struct{}, maxConcurrentHandleLookups+1),
				release: make(chan struct{}),
			}
			options := handlerOptions(nil)
			options.Resolver = resolver
			options.HandleZoneRoot = "ap.tdpl.example"
			handler, err := NewHandler(options)
			require.NoError(t, err)

			query := func(question string) *miekgdns.Msg {
				request := new(miekgdns.Msg)
				request.SetQuestion(question, miekgdns.TypeTXT)
				writer := &recordingDNSWriter{}
				handler.ServeDNS(writer, request)
				return writer.response
			}

			var waitGroup sync.WaitGroup
			responses := make(chan *miekgdns.Msg, maxConcurrentHandleLookups)
			for range maxConcurrentHandleLookups {
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					responses <- query("_atproto.alice.lemmy-world.tdpl.example.")
				}()
			}
			for range maxConcurrentHandleLookups {
				<-resolver.entered
			}

			overflow := query(tc.question)
			require.NotNil(t, overflow)
			require.Equal(t, miekgdns.RcodeServerFailure, overflow.Rcode, "a query over the lookup limit must get SERVFAIL")
			require.Equal(t, maxConcurrentHandleLookups, resolver.askedCount(), "a query over the lookup limit must not reach the resolver")

			close(resolver.release)
			waitGroup.Wait()
			close(responses)
			for response := range responses {
				require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
				require.Len(t, response.Answer, 1)
			}

			afterRelease := query(tc.question)
			require.NotNil(t, afterRelease)
			require.Equal(t, miekgdns.RcodeSuccess, afterRelease.Rcode, "lookup slots must be released after each lookup")
			require.Len(t, afterRelease.Answer, 1)
			require.Equal(t, maxConcurrentHandleLookups+1, resolver.askedCount())
		})
	}
}
