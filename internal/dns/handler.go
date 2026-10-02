package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"time"

	miekgdns "github.com/miekg/dns"

	internalerrors "tidepool/internal/errors"
	"tidepool/internal/identity"
)

const handleLookupTimeout = 2 * time.Second

// maxConcurrentHandleLookups bounds how many DNS queries may run a handle
// lookup at once, so spoofed UDP floods cannot exhaust the shared database
// connection pool. Queries over the limit are answered SERVFAIL.
const maxConcurrentHandleLookups = 8

// Options configures an authoritative DNS handler for bridged handles.
type Options struct {
	ZoneRoot    string
	Nameservers []string
	PublicIPv4  netip.Addr
	PublicIPv6  netip.Addr
	Serial      uint32
	Resolver    identity.Resolver
	Logger      *slog.Logger
}

// Handler answers DNS queries in the configured handle subzones.
type Handler struct {
	zoneRoot   string
	nameserver string
	serial     uint32
	resolver   identity.Resolver
	logger     *slog.Logger
	// lookupSlots holds one token per in-flight handle lookup.
	lookupSlots chan struct{}
}

// NewHandler validates options and constructs a handle DNS handler.
func NewHandler(options Options) (*Handler, error) {
	if strings.TrimSpace(options.ZoneRoot) == "" {
		return nil, fmt.Errorf("zone root is required")
	}
	zoneRoot, err := normalizeDomainName(options.ZoneRoot)
	if err != nil {
		return nil, fmt.Errorf("zone root: %w", err)
	}
	if !options.PublicIPv4.Is4() {
		return nil, fmt.Errorf("public IPv4 address is required")
	}
	if options.PublicIPv6.IsValid() && !options.PublicIPv6.Is6() {
		return nil, fmt.Errorf("public IPv6 address must be IPv6")
	}
	if len(options.Nameservers) == 0 || strings.TrimSpace(options.Nameservers[0]) == "" {
		return nil, fmt.Errorf("at least one nameserver is required")
	}
	nameservers := make([]string, 0, len(options.Nameservers))
	for _, nameserver := range options.Nameservers {
		normalized, err := normalizeDomainName(nameserver)
		if err != nil {
			return nil, fmt.Errorf("nameserver: %w", err)
		}
		nameservers = append(nameservers, normalized)
	}
	if options.Resolver == nil {
		return nil, fmt.Errorf("resolver is required")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		zoneRoot:    zoneRoot,
		nameserver:  nameservers[0],
		serial:      options.Serial,
		resolver:    options.Resolver,
		logger:      logger,
		lookupSlots: make(chan struct{}, maxConcurrentHandleLookups),
	}, nil
}

// normalizeDomainName trims, lowercases and fully qualifies name, then checks
// that it is a non-root domain name that packs on the wire.
func normalizeDomainName(name string) (string, error) {
	normalized := miekgdns.Fqdn(strings.ToLower(strings.TrimSpace(name)))
	if normalized == "." {
		return "", fmt.Errorf("%q must not be the DNS root", name)
	}
	if strings.Contains(normalized, ":") {
		return "", fmt.Errorf("%q must be a domain name without a port", name)
	}
	// IsDomainName rejects empty labels, labels over 63 bytes and names over 255 bytes.
	if _, ok := miekgdns.IsDomainName(normalized); !ok {
		return "", fmt.Errorf("%q is not a valid domain name", name)
	}
	return normalized, nil
}

// ServeDNS answers handle TXT queries and returns NODATA for other in-zone queries.
func (h *Handler) ServeDNS(writer miekgdns.ResponseWriter, request *miekgdns.Msg) {
	response := new(miekgdns.Msg)
	response.SetReply(request)
	response.RecursionAvailable = false
	if len(request.Question) != 1 {
		response.Rcode = miekgdns.RcodeFormatError
		h.writeResponse(writer, response)
		return
	}

	question := request.Question[0]
	name := miekgdns.Fqdn(strings.ToLower(question.Name))
	// IsSubDomain and SplitDomainName treat an escaped dot (\.) as part of a label.
	if name == h.zoneRoot || !miekgdns.IsSubDomain(h.zoneRoot, name) {
		response.Rcode = miekgdns.RcodeRefused
		h.writeResponse(writer, response)
		return
	}

	nameLabels := miekgdns.SplitDomainName(name)
	labels := nameLabels[:len(nameLabels)-miekgdns.CountLabel(h.zoneRoot)]
	apex := labels[len(labels)-1] + "." + h.zoneRoot
	response.Authoritative = true

	// Task 03: refuse in-zone AXFR and IXFR here.
	if question.Qtype == miekgdns.TypeTXT && len(labels) == 3 && labels[0] == "_atproto" {
		handle := labels[1] + "." + labels[2] + "." + strings.TrimSuffix(h.zoneRoot, ".")
		select {
		case h.lookupSlots <- struct{}{}:
		default:
			h.logger.Debug("DNS handle lookup limit reached", "limit", maxConcurrentHandleLookups)
			response.Rcode = miekgdns.RcodeServerFailure
			h.writeResponse(writer, response)
			return
		}
		lookupContext, cancel := context.WithTimeout(context.Background(), handleLookupTimeout)
		did, err := h.resolver.ResolveHandle(lookupContext, handle)
		cancel()
		<-h.lookupSlots
		switch {
		case err == nil:
			response.Answer = []miekgdns.RR{&miekgdns.TXT{
				Hdr: miekgdns.RR_Header{Name: question.Name, Rrtype: miekgdns.TypeTXT, Class: miekgdns.ClassINET, Ttl: 300},
				Txt: []string{"did=" + did},
			}}
		case internalerrors.IsNotFound(err) || internalerrors.IsValidation(err):
			h.addSOA(response, apex)
		default:
			h.logger.Error("DNS handle lookup failed", "handle", handle, "error", err)
			response.Rcode = miekgdns.RcodeServerFailure
		}
		h.writeResponse(writer, response)
		return
	}

	// Task 02: answer SOA and NS queries at the label apex here.
	// Task 03: answer A and AAAA queries here.
	h.addSOA(response, apex)
	h.writeResponse(writer, response)
}

// writeResponse sends response and logs a failure. A response that cannot be
// packed is replaced by a record-free SERVFAIL so the client is not left to time out.
func (h *Handler) writeResponse(writer miekgdns.ResponseWriter, response *miekgdns.Msg) {
	err := writer.WriteMsg(response)
	if err == nil {
		return
	}
	var packError *miekgdns.Error
	if !errors.As(err, &packError) {
		h.logger.Warn("DNS response write failed", "error", err)
		return
	}
	h.logger.Error("DNS response could not be packed", "error", err)
	failure := new(miekgdns.Msg)
	failure.MsgHdr = response.MsgHdr
	failure.Question = response.Question
	failure.Rcode = miekgdns.RcodeServerFailure
	if err := writer.WriteMsg(failure); err != nil {
		h.logger.Warn("DNS SERVFAIL write failed", "error", err)
	}
}

func (h *Handler) addSOA(response *miekgdns.Msg, apex string) {
	response.Ns = []miekgdns.RR{&miekgdns.SOA{
		Hdr:     miekgdns.RR_Header{Name: apex, Rrtype: miekgdns.TypeSOA, Class: miekgdns.ClassINET, Ttl: 3600},
		Ns:      h.nameserver,
		Mbox:    "hostmaster." + h.zoneRoot,
		Serial:  h.serial,
		Refresh: 3600,
		Retry:   600,
		Expire:  1209600,
		Minttl:  300,
	}}
}

// ServeUDP binds address synchronously and returns the bind or startup error.
// Once started it serves in the background until ctx is cancelled. The returned
// channel receives one non-nil error if serving stops for any reason other than
// ctx cancellation, and is closed once the server has stopped.
func ServeUDP(ctx context.Context, address string, handler miekgdns.Handler) (<-chan error, error) {
	packet, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, fmt.Errorf("bind DNS UDP %s: %w", address, err)
	}
	done, err := servePacketConn(ctx, packet, handler)
	if err != nil {
		return nil, fmt.Errorf("start DNS UDP %s: %w", address, err)
	}
	return done, nil
}

// servePacketConn serves DNS on packet with the contract of ServeUDP. It takes
// ownership of packet and closes it when serving stops. When a serve failure
// races with ctx cancellation, the failure is reported only if ctx.Err() is
// still nil at that point: a stop during shutdown is treated as the shutdown.
func servePacketConn(ctx context.Context, packet net.PacketConn, handler miekgdns.Handler) (<-chan error, error) {
	started := make(chan struct{})
	stopped := make(chan error, 1)
	server := &miekgdns.Server{
		PacketConn:        packet,
		Handler:           handler,
		NotifyStartedFunc: func() { close(started) },
	}
	go func() {
		err := server.ActivateAndServe()
		_ = packet.Close()
		stopped <- err
	}()
	select {
	case <-started:
	case err := <-stopped:
		if err == nil {
			err = errors.New("DNS server stopped before starting")
		}
		return nil, err
	}

	done := make(chan error, 1)
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			// Shutdown only fails when the server was never started.
			_ = server.Shutdown()
			<-stopped
		case err := <-stopped:
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				err = errors.New("DNS server stopped unexpectedly")
			}
			done <- fmt.Errorf("serve DNS on %s: %w", packet.LocalAddr(), err)
		}
	}()
	return done, nil
}
