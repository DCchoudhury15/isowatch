package proxy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// stmtInfo is what the proxy knows about one SQL string: its plan, its
// effect on the transaction, and its sqlcommenter tags.
type stmtInfo struct {
	plan     *rewrite.Plan
	ctl      control
	meta     *meta
	original string   // the SQL without comments, as recorded in the history
	comments []string // put back after the rewritten SQL
	sql      string   // the SQL as the client sent it
	params   []byte   // Parse: the parameter types part of the message
}

func (i *stmtInfo) tracked() bool {
	if i == nil {
		return false
	}
	k := i.plan.Kind
	return k == rewrite.Read || k == rewrite.AggRead || k == rewrite.Write
}

// portal is one run of a statement: a Bind (extended protocol) or a Query
// (simple protocol). The client side sets info. The other fields belong to
// the server side.
type portal struct {
	info *stmtInfo

	started bool
	rec     *capture.Recorder
	stmt    capture.Stmt
}

// entry is one client message that PostgreSQL will answer. PostgreSQL
// answers in order, so the server side matches each reply to the entry at
// the head of the queue.
type entry struct {
	typ    byte      // 'P' Parse, 'B' Bind, 'D' Describe, 'E' Execute, 'C' Close, 'S' Sync, 'Q' Query, 'F' FunctionCall
	target byte      // Describe and Close: 'S' statement or 'P' portal
	name   string    // Parse and Close: the statement name
	info   *stmtInfo // Parse: the new statement; Describe: what is described
	prev   *stmtInfo // Parse and Close: what the name meant before
	portal *portal   // Execute and Query

	pathChange bool                 // may change search_path
	restore    map[string]*stmtInfo // statements a DEALLOCATE or DISCARD removed
	internal   bool                 // sent by the proxy itself: the reply is not forwarded
}

// conn is one client connection and its server connection. Two goroutines
// work on it: clientLoop (client to server) and serverLoop (server to
// client).
type conn struct {
	p      *Proxy
	client net.Conn

	mu        sync.Mutex
	server    net.Conn
	closeOnce sync.Once

	cr, sr *reader
	cw, sw *bufio.Writer

	q queue

	// Prepared statements by name. The client side changes them as it
	// sends Parse, Close and DEALLOCATE; the server side undoes a change
	// that PostgreSQL rejected or skipped.
	stmtMu sync.Mutex
	stmts  map[string]*stmtInfo

	// Client side only.
	portals map[string]*portal
	qplans  map[string]*rewrite.Plan // plans for a connection with another search_path

	// search_path: statements sent that may change it and are not answered
	// yet, and whether the server reported a different one.
	pathPending atomic.Int32
	pathOther   atomic.Bool

	// Server side only.
	started    bool // the first ReadyForQuery has been seen
	tx         txState
	sessionIso string // default isolation of new transactions on this connection
	startIso   string // the same, as the connection started
	rowBuf     []byte
}

// errDesync ends a connection whose replies no longer match the requests.
var errDesync = errors.New("isowatch proxy: lost track of the protocol")

func (c *conn) close() {
	c.closeOnce.Do(func() {
		c.client.Close()
		c.mu.Lock()
		if c.server != nil {
			c.server.Close()
		}
		c.mu.Unlock()
	})
}

func (c *conn) serve() {
	defer c.close()
	c.cr = newReader(c.client)
	pkt, err := c.readStartup()
	if err != nil {
		return
	}
	code := binary.BigEndian.Uint32(pkt[4:8])
	server, err := net.DialTimeout("tcp", c.p.cfg.Upstream, c.p.cfg.DialTimeout)
	if err != nil {
		c.p.log.Warn("isowatch proxy: cannot reach the database", "upstream", c.p.cfg.Upstream, "err", err)
		if code != codeCancelRequest {
			w := bufio.NewWriter(c.client)
			_ = writeMsg(w, 'E', errorResponse("FATAL", "08006", "isowatch proxy: cannot reach the database"))
			_ = w.Flush()
		}
		return
	}
	c.mu.Lock()
	c.server = server
	c.mu.Unlock()
	if code == codeCancelRequest {
		_, _ = server.Write(pkt)
		// PostgreSQL closes the connection after reading the request.
		_, _ = io.Copy(io.Discard, server)
		return
	}

	params := startupParams(pkt)
	c.sessionIso = c.p.cfg.Monitor.DefaultIsolation()
	if iso, ok := startupIso(params); ok {
		c.sessionIso = iso
	}
	c.startIso = c.sessionIso
	c.stmts = map[string]*stmtInfo{}
	c.portals = map[string]*portal{}
	c.sr = newReader(server)
	c.cw = bufio.NewWriterSize(c.client, 64<<10)
	c.sw = bufio.NewWriterSize(server, 64<<10)
	if _, err := c.sw.Write(pkt); err != nil {
		return
	}

	done := make(chan error, 1)
	go func() { done <- c.clientLoop() }()
	err = c.serverLoop()
	c.close()
	<-done
	if errors.Is(err, errDesync) {
		c.p.desyncs.Add(1)
	}
}

// readStartup reads the first packet. It answers SSL and GSS encryption
// requests with "N" (not supported), so the client continues without
// encryption or gives up, depending on its settings.
func (c *conn) readStartup() ([]byte, error) {
	for {
		pkt, err := c.cr.startup()
		if err != nil {
			return nil, err
		}
		switch binary.BigEndian.Uint32(pkt[4:8]) {
		case codeSSLRequest, codeGSSENCRequest:
			if _, err := c.client.Write([]byte{'N'}); err != nil {
				return nil, err
			}
			continue
		}
		return pkt, nil
	}
}

// clientLoop forwards client messages to the server, rewriting statements
// on watched tables.
func (c *conn) clientLoop() error {
	for {
		if !c.cr.buffered() {
			if err := c.sw.Flush(); err != nil {
				return err
			}
		}
		typ, body, err := c.cr.next()
		if err != nil {
			return err
		}
		if err := c.fromClient(typ, body); err != nil {
			return err
		}
		if typ == 'X' {
			return c.sw.Flush()
		}
	}
}

// serverLoop forwards server messages to the client, removing hidden
// columns and recording what the transactions read and wrote.
func (c *conn) serverLoop() error {
	for {
		if !c.sr.buffered() {
			if err := c.cw.Flush(); err != nil {
				return err
			}
		}
		typ, body, err := c.sr.next()
		if err != nil {
			_ = c.cw.Flush()
			return err
		}
		if err := c.fromServer(typ, body); err != nil {
			return err
		}
	}
}

func (c *conn) desync(format string, args ...any) error {
	c.p.log.Error("isowatch proxy: closing a connection, a reply from PostgreSQL did not match the request it answers",
		"detail", fmt.Sprintf(format, args...), "client", c.client.RemoteAddr().String())
	return errDesync
}
