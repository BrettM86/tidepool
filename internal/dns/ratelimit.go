package dns

import (
	"container/list"
	"expvar"
	"net"
	"net/netip"
	"sync"
	"time"

	miekgdns "github.com/miekg/dns"
)

// Prefix grouping shares a budget across a client's source ports and questions.
const (
	ipv4ClientPrefixLength = 24
	ipv6ClientPrefixLength = 56
	udpResponseSlip        = 2
)

// The idle window and hard cap bound the per-handler prefix table without a sweeper goroutine.
const (
	maxUDPRateLimitPrefixes   = 16384
	udpRateLimitIdleExpiry    = 10 * time.Second
	udpSuppressionLogInterval = time.Minute
)

var udpResponsesSuppressed = expvar.NewInt("tidepool_dns_udp_responses_suppressed")

// udpPrefixEntry holds a fixed one-second response budget and the slip phase.
type udpPrefixEntry struct {
	prefix      netip.Prefix
	lastActive  time.Time
	windowStart time.Time
	responses   int
	suppressed  uint64
	position    *list.Element
}

// udpRateLimiter orders entries by last activity for O(1) cap eviction and idle expiry.
type udpRateLimiter struct {
	mutex     sync.Mutex
	rate      int
	now       func() time.Time
	prefixes  map[netip.Prefix]*udpPrefixEntry
	activity  list.List
	lastLog   time.Time
	hasLogged bool
}

func udpClientPrefix(address net.Addr) netip.Prefix {
	var ip netip.Addr
	if remote, ok := address.(*net.UDPAddr); ok {
		ip, _ = netip.AddrFromSlice(remote.IP)
	}
	ip = ip.Unmap()
	if ip.Is4() {
		return netip.PrefixFrom(ip, ipv4ClientPrefixLength).Masked()
	}
	if ip.Is6() {
		return netip.PrefixFrom(ip, ipv6ClientPrefixLength).Masked()
	}
	// An unrecognized remote address shares one budget rather than bypassing it.
	return netip.Prefix{}
}

// admitUDP serializes each prefix's budget, slip phase, expiry and log interval.
// It returns whether to answer normally and, when suppressed, whether to slip.
func (h *Handler) admitUDP(address net.Addr) (bool, bool) {
	limiter := &h.udpRateLimit
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()

	now := limiter.now()
	for front := limiter.activity.Front(); front != nil; front = limiter.activity.Front() {
		entry := front.Value.(*udpPrefixEntry)
		if now.Sub(entry.lastActive) < udpRateLimitIdleExpiry {
			break
		}
		delete(limiter.prefixes, entry.prefix)
		limiter.activity.Remove(front)
	}

	prefix := udpClientPrefix(address)
	entry := limiter.prefixes[prefix]
	if entry == nil {
		if len(limiter.prefixes) == maxUDPRateLimitPrefixes {
			oldest := limiter.activity.Front()
			delete(limiter.prefixes, oldest.Value.(*udpPrefixEntry).prefix)
			limiter.activity.Remove(oldest)
		}
		entry = &udpPrefixEntry{prefix: prefix, windowStart: now}
		entry.position = limiter.activity.PushBack(entry)
		limiter.prefixes[prefix] = entry
	} else {
		limiter.activity.MoveToBack(entry.position)
		if now.Sub(entry.windowStart) >= time.Second {
			entry.windowStart = now
			entry.responses = 0
		}
	}
	entry.lastActive = now
	if entry.responses < limiter.rate {
		entry.responses++
		return true, false
	}
	entry.suppressed++
	udpResponsesSuppressed.Add(1)
	if !limiter.hasLogged || now.Sub(limiter.lastLog) >= udpSuppressionLogInterval {
		limiter.hasLogged = true
		limiter.lastLog = now
		// The prefix is omitted: an IPv6 /56 identifies a household, and a spoofed flood names its victim.
		family := "unknown"
		if prefix.Addr().Is4() {
			family = "ipv4"
		} else if prefix.Addr().Is6() {
			family = "ipv6"
		}
		h.logger.Warn("DNS UDP responses suppressed", "family", family, "suppressed", entry.suppressed)
	}
	return false, entry.suppressed%udpResponseSlip == 0
}

func (h *Handler) serveUDP(writer miekgdns.ResponseWriter, request *miekgdns.Msg) {
	allowed, slip := h.admitUDP(writer.RemoteAddr())
	if allowed {
		udpHandler{h}.ServeDNS(writer, request)
		return
	}
	if slip {
		response := new(miekgdns.Msg)
		response.SetReply(request)
		response.Truncated = true
		h.writeResponse(writer, response)
	}
}

func (h *Handler) udpRateLimitEntries() int {
	h.udpRateLimit.mutex.Lock()
	defer h.udpRateLimit.mutex.Unlock()
	return len(h.udpRateLimit.prefixes)
}
