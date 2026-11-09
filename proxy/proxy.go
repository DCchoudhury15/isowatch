// Package proxy is a PostgreSQL wire protocol proxy that records
// transactions for IsoWatch. Apps in any language connect to the proxy
// instead of PostgreSQL. The proxy rewrites statements on watched tables the
// same way the capture package does, removes the hidden columns before the
// app sees the results, and sends each committed transaction to the
// Monitor's sink.
//
// The rule is the same as in the rest of IsoWatch: when the proxy is not
// sure, it forwards a message unchanged and does not track it. If it ever
// loses track of the protocol, it closes the connection rather than send
// the app wrong data.
//
// Limitations: clients must connect without TLS (the proxy answers "no" to
// SSLRequest and GSSENCRequest), and each client connection gets its own
// server connection.
package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DCchoudhury15/isowatch/capture"
)

// Config configures a Proxy.
type Config struct {
	// Upstream is the PostgreSQL server, as host:port. Required.
	Upstream string
	// Monitor plans the statements, records the transactions and sends
	// them to its sink. Required.
	Monitor *capture.Monitor
	// SearchPath is the search_path the Monitor read its tables with, as
	// PostgreSQL reports it (SHOW search_path), for example
	// `"$user", public`. On a connection whose search_path is different,
	// table names without a schema may mean other tables, so only
	// statements that name the schema are tracked there. Empty means the
	// PostgreSQL default.
	SearchPath string
	// Logger receives connection problems. Default: none.
	Logger *slog.Logger
	// DialTimeout limits connecting to Upstream. Default: 10s.
	DialTimeout time.Duration
}

// Stats are running counters for a Proxy.
type Stats struct {
	Connections int64 // client connections accepted
	Active      int64 // client connections open now
	// Desyncs counts connections the proxy closed because a reply from
	// PostgreSQL did not match what it expected. It should stay 0.
	Desyncs int64
}

// Proxy accepts client connections and relays them to Upstream.
type Proxy struct {
	cfg Config
	log *slog.Logger

	mu        sync.Mutex
	listeners map[net.Listener]struct{}
	conns     map[*conn]struct{}
	closed    bool
	wg        sync.WaitGroup

	ctlMu sync.RWMutex
	ctls  map[string]control

	connections, active, desyncs atomic.Int64
}

// New returns a Proxy.
func New(cfg Config) (*Proxy, error) {
	if cfg.Upstream == "" {
		return nil, errors.New("isowatch proxy: Config.Upstream is required")
	}
	if cfg.Monitor == nil {
		return nil, errors.New("isowatch proxy: Config.Monitor is required")
	}
	if cfg.SearchPath == "" {
		cfg.SearchPath = `"$user", public`
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Proxy{
		cfg:       cfg,
		log:       log,
		listeners: map[net.Listener]struct{}{},
		conns:     map[*conn]struct{}{},
		ctls:      map[string]control{},
	}, nil
}

// ErrClosed is returned by Serve after Close.
var ErrClosed = errors.New("isowatch proxy: closed")

// Serve accepts connections on ln until ln fails or Close is called.
func (p *Proxy) Serve(ln net.Listener) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	p.listeners[ln] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.listeners, ln)
		p.mu.Unlock()
	}()
	for {
		nc, err := ln.Accept()
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return ErrClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}
		c := &conn{p: p, client: nc}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			nc.Close()
			return ErrClosed
		}
		p.conns[c] = struct{}{}
		p.wg.Add(1)
		p.mu.Unlock()
		p.connections.Add(1)
		p.active.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.active.Add(-1)
			c.serve()
			p.mu.Lock()
			delete(p.conns, c)
			p.mu.Unlock()
		}()
	}
}

// Close stops all listeners, closes every client connection and waits for
// them to end. Transactions that were still open are not reported.
func (p *Proxy) Close() error {
	p.mu.Lock()
	p.closed = true
	for ln := range p.listeners {
		ln.Close()
	}
	for c := range p.conns {
		c.close()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

// Shutdown stops accepting connections and waits until the open ones end
// by themselves or ctx is done. Then it closes what is left.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	for ln := range p.listeners {
		ln.Close()
	}
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		p.Close()
		return ctx.Err()
	}
}

// Stats returns a snapshot of the counters.
func (p *Proxy) Stats() Stats {
	return Stats{
		Connections: p.connections.Load(),
		Active:      p.active.Load(),
		Desyncs:     p.desyncs.Load(),
	}
}

const ctlCacheSize = 2048

// classifyCached classifies a statement once per SQL text.
func (p *Proxy) classifyCached(sql string, multi bool) control {
	p.ctlMu.RLock()
	c, ok := p.ctls[sql]
	p.ctlMu.RUnlock()
	if ok {
		return c
	}
	c = classify(sql, multi)
	p.ctlMu.Lock()
	if len(p.ctls) >= ctlCacheSize {
		p.ctls = map[string]control{}
	}
	p.ctls[sql] = c
	p.ctlMu.Unlock()
	return c
}
