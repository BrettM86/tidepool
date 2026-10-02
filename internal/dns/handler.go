package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	miekgdns "github.com/miekg/dns"
	"golang.org/x/net/netutil"

	internalerrors "tidepool/internal/errors"
	"tidepool/internal/identity"
)

const handleLookupTimeout = 2 * time.Second

const (
	zoneRecordTTL = 3600
	soaMinimumTTL = 300
	soaRefresh    = 3600
	soaRetry      = 600
	soaExpire     = 1209600
	handleTXTTTL  = 300
)

// maxConcurrentHandleLookups bounds how many DNS queries may run a handle
// lookup at once, so spoofed UDP floods cannot exhaust the shared database
// connection pool. Queries over the limit are answered SERVFAIL.
const maxConcurrentHandleLookups = 8

// advertisedUDPPayloadSize is the UDP payload size the server reads and
// advertises in its OPT record (the DNS Flag Day 2020 recommendation).
const advertisedUDPPayloadSize = 1232

// TCP limits keep a connection flood from exhausting the file descriptors,
// memory and CPU that the DNS server shares with the rest of the process.
const (
	maxTCPConnections          = 256
	tcpReadTimeout             = 2 * time.Second
	tcpIdleTimeout             = 3 * time.Second
	maxTCPQueriesPerConnection = 16
	acceptRetryInitialDelay    = 5 * time.Millisecond
	acceptRetryMaximumDelay    = time.Second
)

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
	zoneRoot    string
	nameservers []string
	serial      uint32
	resolver    identity.Resolver
	logger      *slog.Logger
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
		nameservers: nameservers,
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

// ServeDNS answers handle TXT and label-apex SOA/NS queries, returning NODATA
// for other in-zone questions.
func (h *Handler) ServeDNS(writer miekgdns.ResponseWriter, request *miekgdns.Msg) {
	response := new(miekgdns.Msg)
	response.SetReply(request)
	response.RecursionAvailable = false
	// SetReply does not copy the request's OPT record (RFC 6891 section 7).
	if request.IsEdns0() != nil {
		response.SetEdns0(advertisedUDPPayloadSize, false)
	}
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
				Hdr: miekgdns.RR_Header{Name: question.Name, Rrtype: miekgdns.TypeTXT, Class: miekgdns.ClassINET, Ttl: handleTXTTTL},
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

	if len(labels) == 1 {
		switch question.Qtype {
		case miekgdns.TypeSOA:
			response.Answer = []miekgdns.RR{h.soa(apex)}
			h.writeResponse(writer, response)
			return
		case miekgdns.TypeNS:
			for _, nameserver := range h.nameservers {
				response.Answer = append(response.Answer, &miekgdns.NS{
					Hdr: miekgdns.RR_Header{Name: apex, Rrtype: miekgdns.TypeNS, Class: miekgdns.ClassINET, Ttl: zoneRecordTTL},
					Ns:  nameserver,
				})
			}
			h.writeResponse(writer, response)
			return
		}
	}
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
	if opt := response.IsEdns0(); opt != nil {
		failure.Extra = []miekgdns.RR{opt}
	}
	if err := writer.WriteMsg(failure); err != nil {
		h.logger.Warn("DNS SERVFAIL write failed", "error", err)
	}
}

func (h *Handler) addSOA(response *miekgdns.Msg, apex string) {
	response.Ns = []miekgdns.RR{h.soa(apex)}
}

func (h *Handler) soa(apex string) *miekgdns.SOA {
	return &miekgdns.SOA{
		Hdr:     miekgdns.RR_Header{Name: apex, Rrtype: miekgdns.TypeSOA, Class: miekgdns.ClassINET, Ttl: zoneRecordTTL},
		Ns:      h.nameservers[0],
		Mbox:    "hostmaster." + h.zoneRoot,
		Serial:  h.serial,
		Refresh: soaRefresh,
		Retry:   soaRetry,
		Expire:  soaExpire,
		Minttl:  soaMinimumTTL,
	}
}

// servePacketConn serves DNS on packet and takes ownership of it.
func servePacketConn(ctx context.Context, packet net.PacketConn, handler miekgdns.Handler) (<-chan error, error) {
	server := &miekgdns.Server{PacketConn: packet, Handler: handler, UDPSize: advertisedUDPPayloadSize}
	return serveServer(ctx, server, packet.Close, packet.LocalAddr())
}

// serveListener serves DNS on listener with bounded connections, timeouts and
// queries per connection, and takes ownership of it.
func serveListener(ctx context.Context, listener net.Listener, handler miekgdns.Handler, logger *slog.Logger) (<-chan error, error) {
	// The backoff wraps the limit so a retrying Accept does not hold a slot.
	limited := newAcceptBackoffListener(netutil.LimitListener(listener, maxTCPConnections), logger)
	server := &miekgdns.Server{
		Listener:      limited,
		Handler:       handler,
		ReadTimeout:   tcpReadTimeout,
		IdleTimeout:   func() time.Duration { return tcpIdleTimeout },
		MaxTCPQueries: maxTCPQueriesPerConnection,
	}
	return serveServer(ctx, server, limited.Close, listener.Addr())
}

// acceptBackoffListener retries temporary Accept errors such as EMFILE with
// exponential backoff and a warning, where miekg would retry at once and
// silently. Other errors, including the one Accept returns after Close, pass
// through.
type acceptBackoffListener struct {
	net.Listener
	logger    *slog.Logger
	closeOnce sync.Once
	closed    chan struct{}
}

func newAcceptBackoffListener(listener net.Listener, logger *slog.Logger) *acceptBackoffListener {
	return &acceptBackoffListener{Listener: listener, logger: logger, closed: make(chan struct{})}
}

func (l *acceptBackoffListener) Accept() (net.Conn, error) {
	var delay time.Duration
	for {
		conn, err := l.Listener.Accept()
		var netError net.Error
		if err == nil || !errors.As(err, &netError) || !netError.Temporary() { //nolint:staticcheck // Temporary is the test miekg applies to Accept errors.
			return conn, err
		}
		delay = min(max(delay*2, acceptRetryInitialDelay), acceptRetryMaximumDelay)
		l.logger.Warn("DNS TCP accept failed; retrying", "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-l.closed:
			timer.Stop()
			return nil, net.ErrClosed
		}
	}
}

// Close interrupts a backoff in progress and closes the wrapped listener.
func (l *acceptBackoffListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return l.Listener.Close()
}

// serveServer waits for startup and reports an unexpected stop unless the
// context is cancelled. It closes the socket once serving stops.
func serveServer(ctx context.Context, server *miekgdns.Server, closeSocket func() error, address net.Addr) (<-chan error, error) {
	started := make(chan struct{})
	stopped := make(chan error, 1)
	server.NotifyStartedFunc = func() { close(started) }
	go func() {
		err := server.ActivateAndServe()
		_ = closeSocket()
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
			done <- fmt.Errorf("serve DNS on %s: %w", address, err)
		}
	}()
	return done, nil
}

// Serve binds UDP and TCP on the same address before starting either server.
// Its channel reports one unexpected transport failure and closes after both
// transports stop; cancellation shuts both down without reporting an error.
// TCP accept failures that are retried are logged to logger.
func Serve(ctx context.Context, address string, handler miekgdns.Handler, logger *slog.Logger) (<-chan error, error) {
	if logger == nil {
		logger = slog.Default()
	}
	packet, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, fmt.Errorf("bind DNS UDP %s: %w", address, err)
	}
	tcpAddress := address
	if _, port, splitErr := net.SplitHostPort(address); splitErr == nil && port == "0" {
		tcpAddress = packet.LocalAddr().String()
	}
	listener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		_ = packet.Close()
		return nil, fmt.Errorf("bind DNS TCP %s: %w", tcpAddress, err)
	}
	done, err := serveConns(ctx, packet, listener, handler, logger)
	if err != nil {
		return nil, fmt.Errorf("start DNS on %s: %w", address, err)
	}
	return done, nil
}

// udpResponseWriter applies the request's UDP payload limit before sending.
type udpResponseWriter struct {
	miekgdns.ResponseWriter
	size int
}

func (writer udpResponseWriter) WriteMsg(response *miekgdns.Msg) error {
	response.Truncate(writer.size)
	return writer.ResponseWriter.WriteMsg(response)
}

type udpHandler struct{ miekgdns.Handler }

func (handler udpHandler) ServeDNS(writer miekgdns.ResponseWriter, request *miekgdns.Msg) {
	size := miekgdns.MinMsgSize
	requestOPT := request.IsEdns0()
	if requestOPT != nil {
		size = int(requestOPT.UDPSize())
		if size < miekgdns.MinMsgSize {
			size = miekgdns.MinMsgSize
		}
	}
	handler.Handler.ServeDNS(udpResponseWriter{ResponseWriter: writer, size: size}, request)
}

// serveConns takes ownership of both sockets and reports the first unexpected
// stop, closing its channel after both transports have stopped.
func serveConns(ctx context.Context, packet net.PacketConn, listener net.Listener, handler miekgdns.Handler, logger *slog.Logger) (<-chan error, error) {
	serveContext, cancel := context.WithCancel(ctx)
	udpDone, err := servePacketConn(serveContext, packet, udpHandler{handler})
	if err != nil {
		cancel()
		_ = listener.Close()
		return nil, fmt.Errorf("start DNS UDP: %w", err)
	}
	tcpDone, err := serveListener(serveContext, listener, handler, logger)
	if err != nil {
		cancel()
		for range udpDone {
		}
		return nil, fmt.Errorf("start DNS TCP: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer cancel()
		for udpDone != nil || tcpDone != nil {
			select {
			case err, open := <-udpDone:
				if !open {
					udpDone = nil
				} else if serveContext.Err() == nil {
					done <- err
					cancel()
				}
			case err, open := <-tcpDone:
				if !open {
					tcpDone = nil
				} else if serveContext.Err() == nil {
					done <- err
					cancel()
				}
			}
		}
	}()
	return done, nil
}
