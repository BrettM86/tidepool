package dns

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miekgdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

type udpRateLimitClock struct {
	mutex sync.Mutex
	time  time.Time
}

func newUDPRateLimitClock() *udpRateLimitClock {
	return &udpRateLimitClock{time: time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)}
}

func (clock *udpRateLimitClock) now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.time
}

func (clock *udpRateLimitClock) advance(duration time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.time = clock.time.Add(duration)
}

type recordingUDPWriter struct {
	recordingDNSWriter
	mutex   sync.Mutex
	remote  *net.UDPAddr
	written []*miekgdns.Msg
}

func newRecordingUDPWriter(ip string, port int) *recordingUDPWriter {
	// netip keeps 192.0.2.1 as 4 bytes and ::ffff:192.0.2.1 as 16, as sockets deliver them.
	return &recordingUDPWriter{remote: net.UDPAddrFromAddrPort(netip.AddrPortFrom(netip.MustParseAddr(ip), uint16(port)))}
}

func (writer *recordingUDPWriter) RemoteAddr() net.Addr { return writer.remote }

func (writer *recordingUDPWriter) WriteMsg(message *miekgdns.Msg) error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	writer.written = append(writer.written, message.Copy())
	return nil
}

func (writer *recordingUDPWriter) Write(data []byte) (int, error) {
	message := new(miekgdns.Msg)
	if err := message.Unpack(data); err != nil {
		return 0, err
	}
	return len(data), writer.WriteMsg(message)
}

func (writer *recordingUDPWriter) messages() []*miekgdns.Msg {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return append([]*miekgdns.Msg(nil), writer.written...)
}

func newRateLimitedTestHandler(t *testing.T, rate int, clock *udpRateLimitClock, resolver *recordingResolver) *Handler {
	t.Helper()
	options := handlerOptions(resolver)
	options.UDPResponseRate = rate
	options.Now = clock.now
	handler, err := NewHandler(options)
	require.NoError(t, err)
	return handler
}

func udpRateLimitRequest(id uint16, name string, questionType uint16) *miekgdns.Msg {
	request := new(miekgdns.Msg)
	request.SetQuestion(name, questionType)
	request.Id = id
	return request
}

func requireNormalUDPAnswer(t *testing.T, writer *recordingUDPWriter, id uint16, recordType uint16) {
	t.Helper()
	messages := writer.messages()
	require.Len(t, messages, 1)
	response := messages[0]
	require.Equal(t, id, response.Id)
	require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
	require.False(t, response.Truncated)
	require.Len(t, response.Answer, 1)
	require.Equal(t, recordType, response.Answer[0].Header().Rrtype)
}

func TestUDPRateLimitSuppressesBeforeHandleResolutionAndRefills(t *testing.T) {
	clock := newUDPRateLimitClock()
	resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
	handler := newRateLimitedTestHandler(t, 20, clock, resolver)
	query := func(id uint16) *recordingUDPWriter {
		writer := newRecordingUDPWriter("192.0.2.1", 5353)
		handler.serveUDP(writer, udpRateLimitRequest(id, "_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT))
		return writer
	}
	for id := uint16(1); id <= 30; id++ {
		writer := query(id)
		if id <= 20 {
			requireNormalUDPAnswer(t, writer, id, miekgdns.TypeTXT)
			txt, ok := writer.messages()[0].Answer[0].(*miekgdns.TXT)
			require.True(t, ok)
			require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)
			continue
		}
		for _, response := range writer.messages() {
			require.True(t, response.Truncated, "suppressed query %d must not get a normal answer", id)
			require.Empty(t, response.Answer)
		}
	}
	require.Len(t, resolver.asked, 20, "suppression must happen before the resolver is called")

	clock.advance(time.Second)
	for id := uint16(31); id <= 50; id++ {
		writer := query(id)
		requireNormalUDPAnswer(t, writer, id, miekgdns.TypeTXT)
		txt, ok := writer.messages()[0].Answer[0].(*miekgdns.TXT)
		require.True(t, ok)
		require.Equal(t, []string{"did=did:plc:abc"}, txt.Txt)
	}
	for _, response := range query(51).messages() {
		require.True(t, response.Truncated, "the 21st query after refill must be suppressed")
		require.Empty(t, response.Answer)
	}
	require.Len(t, resolver.asked, 40)
}

func TestUDPRateLimitDropsThenSlipsEverySecondSuppressedQuery(t *testing.T) {
	handler := newRateLimitedTestHandler(t, 20, newUDPRateLimitClock(), &recordingResolver{})
	for id := uint16(1); id <= 20; id++ {
		writer := newRecordingUDPWriter("192.0.2.1", 5353)
		handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
	}

	for suppressed := uint16(1); suppressed <= 10; suppressed++ {
		id := 200 + suppressed
		request := udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA)
		writer := newRecordingUDPWriter("192.0.2.1", 5353)
		handler.serveUDP(writer, request)
		messages := writer.messages()
		if suppressed%2 == 1 {
			require.Empty(t, messages, "suppressed query #%d must be dropped", suppressed)
			continue
		}
		require.Len(t, messages, 1, "suppressed query #%d must slip exactly once", suppressed)
		response := messages[0]
		require.True(t, response.Response, "a slip reply without the QR bit is discarded by resolvers")
		require.Equal(t, request.Opcode, response.Opcode)
		require.True(t, response.Truncated)
		require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
		require.Equal(t, id, response.Id)
		require.Equal(t, request.Question, response.Question)
		require.Empty(t, response.Answer)
		require.Empty(t, response.Ns)
		require.Empty(t, response.Extra)
	}
}

func TestUDPRateLimitGroupsClientPrefixes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first      string
		second     string
		shareLimit bool
	}{
		{"IPv4 same /24", "192.0.2.1", "192.0.2.200", true},
		{"IPv6 same /56", "2001:db8:0:1::1", "2001:db8:0:ff::1", true},
		{"mapped IPv6 shares IPv4 /24", "::ffff:192.0.2.1", "192.0.2.200", true},
		{"IPv4 separate /24", "192.0.2.1", "192.0.3.1", false},
		{"IPv6 separate /56", "2001:db8:0:100::1", "2001:db8:0:1::1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := newRateLimitedTestHandler(t, 2, newUDPRateLimitClock(), &recordingResolver{})
			for id := uint16(1); id <= 2; id++ {
				writer := newRecordingUDPWriter(tc.first, 5353)
				handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
				requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
			}
			writer := newRecordingUDPWriter(tc.second, 5353)
			handler.serveUDP(writer, udpRateLimitRequest(3, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
			if tc.shareLimit {
				require.Empty(t, writer.messages(), "the first suppressed response must be dropped")
			} else {
				requireNormalUDPAnswer(t, writer, 3, miekgdns.TypeSOA)
			}
		})
	}

	t.Run("port, name and question type do not bypass a client's budget", func(t *testing.T) {
		resolver := &recordingResolver{handles: map[string]string{"alice.lemmy-world.tdpl.example": "did:plc:abc"}}
		handler := newRateLimitedTestHandler(t, 2, newUDPRateLimitClock(), resolver)
		for id := uint16(1); id <= 2; id++ {
			writer := newRecordingUDPWriter("192.0.2.1", 5353)
			handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
			requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
		}
		writer := newRecordingUDPWriter("192.0.2.1", 40000)
		handler.serveUDP(writer, udpRateLimitRequest(3, "_atproto.alice.lemmy-world.tdpl.example.", miekgdns.TypeTXT))
		require.Empty(t, writer.messages(), "the first suppressed response must be dropped")
	})
}

func TestUDPRateLimitBoundsPrefixTableAndTracksNewClients(t *testing.T) {
	handler := newRateLimitedTestHandler(t, 2, newUDPRateLimitClock(), &recordingResolver{})
	for index := range 16484 {
		writer := newRecordingUDPWriter(fmt.Sprintf("10.%d.%d.1", index/256, index%256), 5353)
		handler.serveUDP(writer, udpRateLimitRequest(1, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
	}
	require.Positive(t, handler.udpRateLimitEntries(), "the limiter must track prefixes")
	require.LessOrEqual(t, handler.udpRateLimitEntries(), 16384, "the prefix table must stay bounded")

	for id := uint16(1); id <= 3; id++ {
		writer := newRecordingUDPWriter("11.0.0.1", 5353)
		handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		if id <= 2 {
			requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
		} else {
			require.Empty(t, writer.messages(), "a new prefix must be limited even when the table is full")
		}
	}

	// A full table must evict the least recently active prefix, so a new prefix sent after each
	// victim query cannot evict the victim and reset its budget.
	for step := range 5 {
		id := uint16(step + 1)
		writer := newRecordingUDPWriter("12.0.0.1", 5353)
		handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		switch {
		case step < 2:
			requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
		case step == 2:
			require.Empty(t, writer.messages(), "the victim must be suppressed after its budget although new prefixes keep arriving")
		default:
			for _, response := range writer.messages() {
				require.True(t, response.Truncated, "victim query %d must not get a normal answer", step+1)
				require.Empty(t, response.Answer)
			}
		}
		newPrefix := newRecordingUDPWriter(fmt.Sprintf("13.0.%d.1", step), 5353)
		handler.serveUDP(newPrefix, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		requireNormalUDPAnswer(t, newPrefix, id, miekgdns.TypeSOA)
	}
	require.Equal(t, 16384, handler.udpRateLimitEntries(), "the prefix table must stay at its cap")
}

func TestUDPRateLimitKeepsSlipPhasePerPrefix(t *testing.T) {
	handler := newRateLimitedTestHandler(t, 2, newUDPRateLimitClock(), &recordingResolver{})
	query := func(ip string, id uint16) (*miekgdns.Msg, []*miekgdns.Msg) {
		request := udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA)
		writer := newRecordingUDPWriter(ip, 5353)
		handler.serveUDP(writer, request)
		return request, writer.messages()
	}
	clients := []string{"192.0.2.1", "198.51.100.1"}
	for _, ip := range clients {
		for id := uint16(1); id <= 2; id++ {
			writer := newRecordingUDPWriter(ip, 5353)
			handler.serveUDP(writer, udpRateLimitRequest(id, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
			requireNormalUDPAnswer(t, writer, id, miekgdns.TypeSOA)
		}
	}

	for round, slips := range []bool{false, true, false} {
		for index, ip := range clients {
			id := uint16(100 + round*10 + index)
			request, messages := query(ip, id)
			if !slips {
				require.Empty(t, messages, "%s suppressed query #%d must be dropped", ip, round+1)
				continue
			}
			require.Len(t, messages, 1, "%s suppressed query #%d must slip exactly once", ip, round+1)
			response := messages[0]
			require.True(t, response.Truncated)
			require.Equal(t, request.Id, response.Id)
			require.Empty(t, response.Answer)
		}
	}
}

func TestUDPRateLimitExpiresByLastActivity(t *testing.T) {
	clock := newUDPRateLimitClock()
	handler := newRateLimitedTestHandler(t, 20, clock, &recordingResolver{})
	query := func(ip string) {
		writer := newRecordingUDPWriter(ip, 5353)
		handler.serveUDP(writer, udpRateLimitRequest(1, "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		requireNormalUDPAnswer(t, writer, 1, miekgdns.TypeSOA)
	}
	query("192.0.2.1")
	query("198.51.100.1")
	require.Equal(t, 2, handler.udpRateLimitEntries(), "both initial prefixes must be tracked")
	clock.advance(9 * time.Second)
	query("192.0.2.1")
	clock.advance(2 * time.Second)
	query("203.0.113.1")
	require.Equal(t, 2, handler.udpRateLimitEntries(), "active A and new C remain; idle B expires")
}

type countingUDPLogHandler struct {
	records  atomic.Int64
	mutex    sync.Mutex
	recorded []slog.Record
}

func (handler *countingUDPLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *countingUDPLogHandler) Handle(_ context.Context, record slog.Record) error {
	handler.records.Add(1)
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	handler.recorded = append(handler.recorded, record.Clone())
	return nil
}

func (handler *countingUDPLogHandler) recordedAttributes(t *testing.T) (string, map[string]string) {
	t.Helper()
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	require.NotEmpty(t, handler.recorded, "a suppression log record must exist")
	record := handler.recorded[0]
	attributes := map[string]string{}
	record.Attrs(func(attribute slog.Attr) bool {
		attributes[attribute.Key] = attribute.Value.String()
		return true
	})
	return record.Message, attributes
}
func (handler *countingUDPLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *countingUDPLogHandler) WithGroup(string) slog.Handler      { return handler }

func TestUDPRateLimitCountsAndBoundsSuppressionLogs(t *testing.T) {
	clock := newUDPRateLimitClock()
	options := handlerOptions(&recordingResolver{})
	options.UDPResponseRate = 20
	options.Now = clock.now
	logs := &countingUDPLogHandler{}
	options.Logger = slog.New(logs)
	handler, err := NewHandler(options)
	require.NoError(t, err)
	counter, ok := expvar.Get("tidepool_dns_udp_responses_suppressed").(*expvar.Int)
	require.True(t, ok && counter != nil, "suppressed responses must publish an expvar.Int")
	initial := counter.Value()
	query := func(ip string, count int) {
		for index := range count {
			writer := newRecordingUDPWriter(ip, 5353)
			handler.serveUDP(writer, udpRateLimitRequest(uint16(index+1), "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		}
	}
	query("192.0.2.1", 30)
	query("198.51.100.1", 30)
	require.Equal(t, int64(20), counter.Value()-initial)
	require.Equal(t, int64(1), logs.records.Load())
	message, attributes := logs.recordedAttributes(t)
	require.NotContains(t, attributes, "prefix", "a client prefix identifies a household or a spoofed victim")
	require.Equal(t, "ipv4", attributes["family"])
	require.Equal(t, "1", attributes["suppressed"])
	for _, client := range []string{"192.0.2", "198.51.100"} {
		require.NotContains(t, message, client)
		for key, value := range attributes {
			require.NotContains(t, value, client, "log attribute %q must not carry the client address", key)
		}
	}
	clock.advance(30 * time.Second)
	query("192.0.2.1", 30)
	require.Equal(t, int64(30), counter.Value()-initial)
	require.Equal(t, int64(1), logs.records.Load())
	// 61s after the first record, so either a >= or a > interval check logs again.
	clock.advance(31 * time.Second)
	query("192.0.2.1", 30)
	require.Equal(t, int64(40), counter.Value()-initial)
	require.Equal(t, int64(2), logs.records.Load())
}

func TestUDPRateLimitConcurrentBurst(t *testing.T) {
	clock := newUDPRateLimitClock()
	options := handlerOptions(&recordingResolver{})
	options.UDPResponseRate = 20
	options.Now = clock.now
	logs := &countingUDPLogHandler{}
	options.Logger = slog.New(logs)
	handler, err := NewHandler(options)
	require.NoError(t, err)
	counter, ok := expvar.Get("tidepool_dns_udp_responses_suppressed").(*expvar.Int)
	require.True(t, ok && counter != nil, "suppressed responses must publish an expvar.Int")
	initial := counter.Value()

	writers := make([]*recordingUDPWriter, 50)
	start := make(chan struct{})
	var done sync.WaitGroup
	for index := range writers {
		writers[index] = newRecordingUDPWriter("192.0.2.1", 5353)
		done.Add(1)
		go func(index int) {
			defer done.Done()
			<-start
			handler.serveUDP(writers[index], udpRateLimitRequest(uint16(index+1), "lemmy-world.tdpl.example.", miekgdns.TypeSOA))
		}(index)
	}
	close(start)
	done.Wait()

	var normal, truncated, dropped int
	for index, writer := range writers {
		messages := writer.messages()
		require.LessOrEqual(t, len(messages), 1, "query %d must get at most one reply", index)
		if len(messages) == 0 {
			dropped++
			continue
		}
		response := messages[0]
		require.Equal(t, uint16(index+1), response.Id)
		if response.Truncated {
			truncated++
			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.Empty(t, response.Answer)
			require.Empty(t, response.Ns)
			require.Empty(t, response.Extra)
		} else {
			normal++
			require.Equal(t, miekgdns.RcodeSuccess, response.Rcode)
			require.Len(t, response.Answer, 1)
			require.Equal(t, miekgdns.TypeSOA, response.Answer[0].Header().Rrtype)
		}
	}
	require.Equal(t, 20, normal)
	require.Equal(t, 15, truncated)
	require.Equal(t, 15, dropped)
	require.Equal(t, int64(30), counter.Value()-initial)
	require.Equal(t, int64(1), logs.records.Load())
}
