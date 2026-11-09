package proxy

import (
	"context"
	"encoding/binary"
	"strings"

	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// txState follows the transaction PostgreSQL is running for the
// connection. A transaction is reported only when PostgreSQL confirmed its
// commit.
type txState struct {
	rec *capture.Recorder
	// poisoned: something happened in this transaction that the proxy does
	// not follow (for example a multi statement string), so nothing of it
	// is reported.
	poisoned bool
	// failed: PostgreSQL reported an error, so a transaction without BEGIN
	// is rolled back at the next ReadyForQuery.
	failed  bool
	inBlock bool // inside BEGIN ... COMMIT

	iso    string // the level from BEGIN or SET TRANSACTION, if isoSet
	isoSet bool

	sc       trace.SpanContext // from a traceparent tag
	name     string
	nameRank int
}

// recorder returns the recorder of the current transaction, starting one
// if needed. It is nil when the transaction is not followed.
func (c *conn) recorder() *capture.Recorder {
	if c.tx.poisoned {
		return nil
	}
	if c.tx.rec == nil {
		c.tx.rec = c.p.cfg.Monitor.NewRecorder()
	}
	return c.tx.rec
}

// applyMeta takes the trace and the name from a statement's tags. The first
// trace wins; a name from a more specific tag replaces a less specific one.
func (c *conn) applyMeta(m *meta) {
	if m == nil {
		return
	}
	if !c.tx.sc.IsValid() && m.sc.IsValid() {
		c.tx.sc = m.sc
	}
	if m.name != "" && m.nameRank > c.tx.nameRank {
		c.tx.name, c.tx.nameRank = m.name, m.nameRank
	}
}

// endTxn ends the current transaction. It is reported if it committed and
// read or wrote a watched table.
func (c *conn) endTxn(committed bool) {
	t := &c.tx
	if committed && t.rec != nil && !t.poisoned {
		txn := t.rec.Txn()
		iso := c.sessionIso
		if t.isoSet {
			iso = t.iso
		}
		if iso == isoUnknown {
			iso = "" // not sure, so never call an anomaly unexpected
		}
		txn.Isolation = iso
		txn.Name = t.name
		ctx := context.Background()
		if t.sc.IsValid() {
			txn.TraceID = t.sc.TraceID().String()
			txn.SpanID = t.sc.SpanID().String()
			ctx = trace.ContextWithRemoteSpanContext(ctx, t.sc)
		}
		t.rec.Commit(ctx)
	}
	c.tx = txState{}
}

func (c *conn) send(typ byte, body []byte) error { return writeMsg(c.cw, typ, body) }

// pop removes the head entry once it is fully answered.
func (c *conn) pop() *entry {
	e := c.q.pop()
	if e != nil && e.pathChange {
		c.pathPending.Add(-1)
	}
	return e
}

// fromServer handles one server message.
func (c *conn) fromServer(typ byte, body []byte) error {
	switch typ {
	case 'N', 'A': // NoticeResponse, NotificationResponse: any time
		return c.send(typ, body)
	case 'S': // ParameterStatus: any time
		if name, rest, err := cstring(body); err == nil && name == "search_path" {
			if v, _, err := cstring(rest); err == nil {
				c.pathOther.Store(v != c.p.cfg.SearchPath)
			}
		}
		return c.send(typ, body)
	case 'Z':
		return c.ready(body)
	case 'E':
		return c.serverError(body)
	}
	if !c.started {
		// Authentication, BackendKeyData, NegotiateProtocolVersion.
		return c.send(typ, body)
	}
	e := c.q.peek()
	if e == nil {
		return c.desync("reply %q with no request waiting", typ)
	}
	switch e.typ {
	case 'P':
		if typ == '1' { // ParseComplete
			c.pop()
			if e.internal {
				return nil
			}
			return c.send(typ, body)
		}
	case 'B':
		if typ == '2' { // BindComplete
			c.pop()
			return c.send(typ, body)
		}
	case 'C':
		if typ == '3' { // CloseComplete
			c.pop()
			if e.internal {
				return nil
			}
			return c.send(typ, body)
		}
	case 'D':
		return c.describeReply(e, typ, body)
	case 'E':
		return c.executeReply(e, typ, body)
	case 'Q':
		return c.queryReply(e, typ, body)
	case 'F':
		if typ == 'V' { // FunctionCallResponse, then ReadyForQuery
			return c.send(typ, body)
		}
	}
	return c.desync("reply %q to request %q", typ, e.typ)
}

func (c *conn) describeReply(e *entry, typ byte, body []byte) error {
	switch typ {
	case 't': // ParameterDescription, then RowDescription or NoData
		if e.target == 'S' {
			return c.send(typ, body)
		}
	case 'n': // NoData
		c.pop()
		return c.send(typ, body)
	case 'T':
		c.pop()
		return c.rowDescription(e.info, body, true)
	}
	return c.desync("reply %q to Describe", typ)
}

// rowDescription removes the hidden columns from a RowDescription. For a
// write without its own RETURNING, every column is hidden, and the client
// gets what it would get without IsoWatch: NoData for a Describe, nothing
// in the simple query protocol.
func (c *conn) rowDescription(info *stmtInfo, body []byte, describe bool) error {
	if !info.tracked() {
		return c.send('T', body)
	}
	out, keep, ok := stripRowDescription(body, info.plan.Hidden)
	if !ok {
		return c.desync("RowDescription does not end with the hidden columns")
	}
	if keep == 0 && info.plan.Kind == rewrite.Write {
		if describe {
			return c.send('n', nil)
		}
		return nil
	}
	return c.send('T', out)
}

func (c *conn) executeReply(e *entry, typ byte, body []byte) error {
	switch typ {
	case 'D':
		return c.dataRow(e.portal, body)
	case 'C': // CommandComplete
		c.pop()
		c.finish(e.portal, body, true)
		return c.send(typ, body)
	case 'I', 's': // EmptyQueryResponse, PortalSuspended
		c.pop()
		return c.send(typ, body)
	case 'G', 'H', 'W', 'd', 'c': // COPY
		return c.send(typ, body)
	}
	return c.desync("reply %q to Execute", typ)
}

func (c *conn) queryReply(e *entry, typ byte, body []byte) error {
	switch typ {
	case 'T':
		return c.rowDescription(e.info, body, false)
	case 'D':
		return c.dataRow(e.portal, body)
	case 'C':
		if e.info == nil || e.info.ctl.kind != ctlMulti {
			c.finish(e.portal, body, false) // a multi statement string ends in queryDone
		}
		return c.send(typ, body)
	case 'I', 'G', 'H', 'W', 'd', 'c':
		return c.send(typ, body)
	}
	return c.desync("reply %q to Query", typ)
}

// dataRow records the hidden values of a row and removes them.
func (c *conn) dataRow(pt *portal, body []byte) error {
	if !pt.info.tracked() {
		return c.send('D', body)
	}
	plan := pt.info.plan
	if len(body) < 2 {
		return c.desync("short DataRow")
	}
	n := int(binary.BigEndian.Uint16(body))
	if n < plan.Hidden {
		return c.desync("DataRow has %d columns, %d are hidden", n, plan.Hidden)
	}
	keep := n - plan.Hidden
	off, hidden, err := splitRow(body, keep)
	if err != nil {
		return c.desync("%v", err)
	}
	c.start(pt)
	if pt.rec != nil && pt.rec == c.tx.rec {
		pt.rec.Row(plan, pt.stmt, hidden)
	}
	if keep == 0 && plan.Kind == rewrite.Write {
		return nil // the row exists only because of the hidden RETURNING
	}
	c.rowBuf = append(c.rowBuf[:0], body[:off]...)
	binary.BigEndian.PutUint16(c.rowBuf, uint16(keep))
	return c.send('D', c.rowBuf)
}

// start begins recording a tracked statement at its first reply.
func (c *conn) start(pt *portal) {
	if pt.started {
		return
	}
	pt.started = true
	if rec := c.recorder(); rec != nil {
		pt.rec = rec
		pt.stmt = rec.Statement(pt.info.original, pt.info.plan)
	}
}

// finish handles a statement that completed. countUntracked is false for
// simple Query messages, which are counted once at ReadyForQuery.
func (c *conn) finish(pt *portal, complete []byte, countUntracked bool) {
	info := pt.info
	if info == nil {
		return
	}
	c.applyMeta(info.meta)
	if info.tracked() {
		c.start(pt)
	} else if countUntracked && info.plan.Kind == rewrite.Untracked {
		c.p.cfg.Monitor.CountUntracked(info.plan)
	}
	tag, _, _ := cstring(complete)
	c.control(info, tag)
}

// control applies what a completed statement did to the transaction and
// the session.
func (c *conn) control(info *stmtInfo, tag string) {
	t := &c.tx
	switch info.ctl.kind {
	case ctlBegin:
		t.inBlock = true
		if info.ctl.iso != "" {
			t.iso, t.isoSet = info.ctl.iso, true
		}
		c.recorder() // so savepoints are followed from the start
	case ctlCommit, ctlRollback:
		// COMMIT of a failed transaction answers "ROLLBACK".
		committed := info.ctl.kind == ctlCommit && tag == "COMMIT"
		iso, isoSet := t.iso, t.isoSet
		c.endTxn(committed)
		if info.ctl.chain {
			// AND CHAIN starts the next transaction at once, with the
			// same settings.
			c.tx.inBlock, c.tx.iso, c.tx.isoSet = true, iso, isoSet
			c.recorder()
		}
	case ctlPrepare:
		// PREPARE TRANSACTION: it commits later, maybe from another
		// connection, so it is not reported.
		c.endTxn(false)
	case ctlTxnIso:
		if t.inBlock && info.ctl.iso != "" {
			t.iso, t.isoSet = info.ctl.iso, true
		}
	case ctlSessionIso:
		if !info.ctl.local && info.ctl.iso != "" {
			c.sessionIso = info.ctl.iso
		}
	case ctlResetIso:
		if !info.ctl.local {
			c.sessionIso = c.startIso
		}
	}
	switch info.plan.Kind {
	case rewrite.Savepoint, rewrite.Release, rewrite.RollbackTo:
		if rec := c.recorder(); rec != nil {
			rec.Done(info.plan)
		}
	}
}

// serverError handles an ErrorResponse. The failed request stays queued:
// in the extended protocol PostgreSQL then skips every message up to the
// next Sync, so ready removes the failed entry and the skipped ones
// together, and undoes what the client side changed for them.
func (c *conn) serverError(body []byte) error {
	c.tx.failed = true
	return c.send('E', body)
}

// ready handles ReadyForQuery. It removes the entry that asked for it (a
// Sync, Query or FunctionCall) and every entry before it, which PostgreSQL
// skipped after an error.
func (c *conn) ready(body []byte) error {
	if len(body) != 1 {
		return c.desync("bad ReadyForQuery")
	}
	status := body[0]
	if !c.started {
		c.started = true
		return c.send('Z', body)
	}
	for {
		e := c.pop()
		if e == nil {
			return c.desync("ReadyForQuery with no request waiting")
		}
		if e.typ == 'S' || e.typ == 'F' {
			break
		}
		if e.typ == 'Q' {
			c.queryDone(e)
			break
		}
		switch e.typ {
		case 'P':
			c.undoParse(e)
		case 'C':
			c.undoClose(e)
		case 'E':
			c.undoDealloc(e)
		}
	}
	switch status {
	case 'I':
		// No transaction is open, so the last one has ended. One without
		// BEGIN committed if there was no error.
		c.endTxn(!c.tx.failed)
	case 'T', 'E':
		c.tx.inBlock = true
		if c.tx.rec == nil && !c.tx.poisoned {
			// A transaction the proxy did not see begin.
			c.tx.poisoned = true
		}
	}
	return c.send('Z', body)
}

// queryDone finishes a simple Query at its ReadyForQuery.
func (c *conn) queryDone(e *entry) {
	info := e.info
	if info == nil {
		return
	}
	if info.plan.Kind == rewrite.Untracked {
		c.p.cfg.Monitor.CountUntracked(info.plan)
	}
	if info.ctl.kind == ctlMulti {
		// The statements in it are not followed one by one, so the
		// transaction they ran in is not reported: at once if it ended
		// (status I), or when it ends.
		c.tx.poisoned = true
		if info.ctl.sessionIso {
			c.sessionIso = isoUnknown
		}
	}
}

// undoParse undoes the client side's change to a statement name when
// PostgreSQL rejected or skipped the Parse.
func (c *conn) undoParse(e *entry) {
	if e.info == nil {
		return
	}
	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	if c.stmts[e.name] != e.info {
		return // already replaced by a later Parse
	}
	if e.prev == nil {
		delete(c.stmts, e.name)
	} else {
		c.stmts[e.name] = e.prev
	}
}

func (c *conn) undoClose(e *entry) {
	if e.target != 'S' || e.prev == nil {
		return
	}
	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	if _, ok := c.stmts[e.name]; !ok {
		c.stmts[e.name] = e.prev
	}
}

func (c *conn) undoDealloc(e *entry) {
	if len(e.restore) == 0 {
		return
	}
	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	for name, info := range e.restore {
		if _, ok := c.stmts[name]; !ok {
			c.stmts[name] = info
		}
	}
}

// stripRowDescription removes the last hidden fields of a RowDescription.
// ok is false unless those fields are IsoWatch's own columns.
func stripRowDescription(body []byte, hidden int) (out []byte, keep int, ok bool) {
	if len(body) < 2 {
		return nil, 0, false
	}
	n := int(binary.BigEndian.Uint16(body))
	if n < hidden {
		return nil, 0, false
	}
	keep = n - hidden
	pos, cut := 2, -1
	for i := 0; i < n; i++ {
		if i == keep {
			cut = pos
		}
		name, rest, err := cstring(body[pos:])
		if err != nil || len(rest) < 18 {
			return nil, 0, false
		}
		if i >= keep && !strings.HasPrefix(name, "__isowatch_") {
			return nil, 0, false
		}
		pos += len(name) + 1 + 18
	}
	if pos != len(body) {
		return nil, 0, false
	}
	if cut < 0 {
		cut = pos
	}
	out = make([]byte, 0, cut)
	out = binary.BigEndian.AppendUint16(out, uint16(keep))
	out = append(out, body[2:cut]...)
	return out, keep, true
}
