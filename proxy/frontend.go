package proxy

import (
	"encoding/binary"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// fromClient handles one client message. It queues what the server will
// answer before it forwards the message, so the reply never arrives before
// its entry.
func (c *conn) fromClient(typ byte, body []byte) error {
	switch typ {
	case 'Q':
		return c.query(body)
	case 'P':
		return c.parse(body)
	case 'B':
		return c.bind(body)
	case 'D':
		return c.describe(body)
	case 'E':
		return c.execute(body)
	case 'C':
		return c.closeMsg(body)
	case 'S', 'F':
		c.q.push(&entry{typ: typ})
	}
	return writeMsg(c.sw, typ, body)
}

// prepare plans one SQL string. Comments are removed before planning, so
// statements that differ only in their sqlcommenter tags share one plan.
func (c *conn) prepare(sql string) *stmtInfo {
	return c.prepareWith(sql, c.otherPath())
}

// otherPath reports whether the connection's search_path is not the one
// the Monitor read its tables with, or may be about to change.
func (c *conn) otherPath() bool {
	return c.pathPending.Load() > 0 || c.pathOther.Load()
}

func (c *conn) prepareWith(sql string, qualifiedOnly bool) *stmtInfo {
	stripped, comments := splitComments(sql)
	var plan *rewrite.Plan
	if qualifiedOnly {
		plan = c.qualifiedPlan(stripped)
	} else {
		plan = c.p.cfg.Monitor.Plan(stripped)
	}
	return &stmtInfo{
		plan:     plan,
		ctl:      c.p.classifyCached(stripped, plan.Reason == "multiple statements"),
		meta:     parseMeta(comments),
		original: stripped,
		comments: comments,
		sql:      sql,
	}
}

// sendSQL is the SQL to send for a statement: the rewritten SQL with the
// comments put back, or the original SQL unchanged.
func (i *stmtInfo) sendSQL(original string) string {
	if !i.tracked() {
		return original
	}
	return withComments(i.plan.SQL, i.comments)
}

// qualifiedCatalog only finds tables named with their schema. A connection
// with a different search_path uses it, because a name without a schema
// may mean another table there.
type qualifiedCatalog struct{ cat rewrite.Catalog }

func (q qualifiedCatalog) Lookup(schema, name string) (*rewrite.Table, bool) {
	if schema == "" {
		return nil, false
	}
	return q.cat.Lookup(schema, name)
}

func (c *conn) qualifiedPlan(sql string) *rewrite.Plan {
	if p, ok := c.qplans[sql]; ok {
		return p
	}
	if c.qplans == nil || len(c.qplans) >= 256 {
		c.qplans = map[string]*rewrite.Plan{}
	}
	p := rewrite.Make(sql, qualifiedCatalog{c.p.cfg.Monitor})
	c.qplans[sql] = p
	return p
}

func (c *conn) query(body []byte) error {
	sql, _, err := cstring(body)
	if err != nil {
		// PostgreSQL rejects it and ends the connection.
		c.q.push(&entry{typ: 'Q', portal: &portal{}})
		return writeMsg(c.sw, 'Q', body)
	}
	info := c.prepare(sql)
	e := &entry{typ: 'Q', info: info, portal: &portal{info: info}}
	c.sideEffects(e, info)
	c.q.push(e)
	if !info.tracked() {
		return writeMsg(c.sw, 'Q', body)
	}
	out := make([]byte, 0, len(info.plan.SQL)+64)
	out = append(out, info.sendSQL(sql)...)
	out = append(out, 0)
	return writeMsg(c.sw, 'Q', out)
}

// sideEffects applies what a statement does to prepared statements and
// search_path, as it is sent. Changes are undone if it fails.
func (c *conn) sideEffects(e *entry, info *stmtInfo) {
	if info == nil {
		return
	}
	if info.ctl.searchPath {
		c.pathPending.Add(1)
		e.pathChange = true
	}
	if !info.ctl.deallocAll && len(info.ctl.dealloc) == 0 {
		return
	}
	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	e.restore = map[string]*stmtInfo{}
	if info.ctl.deallocAll {
		for k, v := range c.stmts {
			e.restore[k] = v
		}
		c.stmts = map[string]*stmtInfo{}
		c.portals = map[string]*portal{}
		return
	}
	for _, name := range info.ctl.dealloc {
		if v, ok := c.stmts[name]; ok {
			e.restore[name] = v
			delete(c.stmts, name)
		}
	}
}

func (c *conn) parse(body []byte) error {
	name, rest, err := cstring(body)
	if err != nil {
		c.q.push(&entry{typ: 'P'})
		return writeMsg(c.sw, 'P', body)
	}
	sql, params, err := cstring(rest)
	if err != nil {
		c.q.push(&entry{typ: 'P'})
		return writeMsg(c.sw, 'P', body)
	}
	info := c.prepare(sql)
	info.params = append([]byte(nil), params...)
	c.stmtMu.Lock()
	prev := c.stmts[name]
	c.stmts[name] = info
	c.stmtMu.Unlock()
	c.q.push(&entry{typ: 'P', name: name, info: info, prev: prev})
	if !info.tracked() {
		return writeMsg(c.sw, 'P', body)
	}
	return writeMsg(c.sw, 'P', parseBody(name, info.sendSQL(sql), params))
}

func parseBody(name, sql string, params []byte) []byte {
	out := make([]byte, 0, len(name)+len(sql)+len(params)+2)
	out = append(out, name...)
	out = append(out, 0)
	out = append(out, sql...)
	out = append(out, 0)
	return append(out, params...)
}

// reprepare replaces a tracked prepared statement that was planned for the
// default search_path. PostgreSQL parses a prepared statement again when
// search_path changes, so its table names may now mean other tables, and
// the hidden columns could break it. The proxy closes it and prepares it
// again with a plan that only tracks names with a schema. The client does
// not see the replies.
func (c *conn) reprepare(name string, old *stmtInfo) (*stmtInfo, error) {
	info := c.prepareWith(old.sql, true)
	info.params = old.params
	if info.plan.SQL == old.plan.SQL && info.plan.Kind == old.plan.Kind {
		return old, nil
	}
	c.stmtMu.Lock()
	c.stmts[name] = info
	c.stmtMu.Unlock()
	c.q.push(&entry{typ: 'C', target: 'S', name: name, prev: old, internal: true})
	c.q.push(&entry{typ: 'P', name: name, info: info, prev: old, internal: true})
	closeBody := append(append([]byte{'S'}, name...), 0)
	if err := writeMsg(c.sw, 'C', closeBody); err != nil {
		return nil, err
	}
	return info, writeMsg(c.sw, 'P', parseBody(name, info.sendSQL(old.sql), old.params))
}

func (c *conn) lookupStmt(name string) *stmtInfo {
	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	return c.stmts[name]
}

func (c *conn) bind(body []byte) error {
	portalName, rest, err1 := cstring(body)
	stmtName, _, err2 := cstring(rest)
	if err1 != nil || err2 != nil {
		c.q.push(&entry{typ: 'B'})
		return writeMsg(c.sw, 'B', body)
	}
	info := c.lookupStmt(stmtName)
	if info.tracked() && info.params != nil && c.otherPath() {
		var err error
		if info, err = c.reprepare(stmtName, info); err != nil {
			return err
		}
	}
	// After reprepare: its Close and Parse go out before this Bind.
	c.q.push(&entry{typ: 'B'})
	if len(c.portals) >= 10000 {
		// Named portals end with their transaction. Clients that never
		// close them should not make this map grow forever.
		c.portals = map[string]*portal{}
	}
	c.portals[portalName] = &portal{info: info}
	if info.tracked() && info.plan.Hidden > 0 {
		if out, ok := addResultFormats(body, info.plan.Hidden); ok {
			return writeMsg(c.sw, 'B', out)
		}
	}
	return writeMsg(c.sw, 'B', body)
}

func (c *conn) describe(body []byte) error {
	e := &entry{typ: 'D'}
	if len(body) > 0 {
		e.target = body[0]
		if name, _, err := cstring(body[1:]); err == nil {
			if e.target == 'S' {
				e.info = c.lookupStmt(name)
			} else if pt := c.portals[name]; pt != nil {
				e.info = pt.info
			}
		}
	}
	c.q.push(e)
	return writeMsg(c.sw, 'D', body)
}

func (c *conn) execute(body []byte) error {
	pt := &portal{}
	if name, _, err := cstring(body); err == nil {
		if p := c.portals[name]; p != nil {
			pt = p
		}
	}
	e := &entry{typ: 'E', info: pt.info, portal: pt}
	c.sideEffects(e, pt.info)
	c.q.push(e)
	return writeMsg(c.sw, 'E', body)
}

func (c *conn) closeMsg(body []byte) error {
	e := &entry{typ: 'C'}
	if len(body) > 0 {
		e.target = body[0]
		if name, _, err := cstring(body[1:]); err == nil {
			e.name = name
			if e.target == 'S' {
				c.stmtMu.Lock()
				e.prev = c.stmts[name]
				delete(c.stmts, name)
				c.stmtMu.Unlock()
			} else {
				delete(c.portals, name)
			}
		}
	}
	c.q.push(e)
	return writeMsg(c.sw, 'C', body)
}

// addResultFormats adds a text format code for each hidden column to a
// Bind message. With zero codes (all text) or one code (used for every
// column) nothing changes: the hidden columns are text, and text in binary
// format is the same bytes. ok is false if the message cannot be read.
func addResultFormats(body []byte, hidden int) ([]byte, bool) {
	_, rest, err := cstring(body)
	if err != nil {
		return nil, false
	}
	_, rest, err = cstring(rest)
	if err != nil {
		return nil, false
	}
	pos := len(body) - len(rest)
	u16 := func() (int, bool) {
		if pos+2 > len(body) {
			return 0, false
		}
		v := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		return v, true
	}
	nf, ok := u16()
	if !ok {
		return nil, false
	}
	pos += 2 * nf
	np, ok := u16()
	if !ok {
		return nil, false
	}
	for i := 0; i < np; i++ {
		if pos+4 > len(body) {
			return nil, false
		}
		l := int(int32(binary.BigEndian.Uint32(body[pos:])))
		pos += 4
		if l > 0 {
			pos += l
		}
	}
	countAt := pos
	nr, ok := u16()
	if !ok || pos+2*nr != len(body) {
		return nil, false
	}
	if nr <= 1 {
		return body, true
	}
	out := make([]byte, 0, len(body)+2*hidden)
	out = append(out, body[:countAt]...)
	out = binary.BigEndian.AppendUint16(out, uint16(nr+hidden))
	out = append(out, body[countAt+2:]...)
	out = append(out, make([]byte, 2*hidden)...)
	return out, true
}
