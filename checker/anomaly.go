package checker

import (
	"fmt"
	"strings"

	"github.com/DCchoudhury15/isowatch/history"
)

// Kind names an anomaly. The G-names come from Adya's thesis.
type Kind string

const (
	// LostUpdate: a transaction overwrote a row based on an old read, so
	// another transaction's write to that row was silently lost. (G-single
	// where every edge is on the same row.)
	LostUpdate Kind = "lost-update"
	// WriteSkew: two transactions each read what the other one changed, then
	// both wrote. Each decision was based on stale data. (G2-item, 2 txns.)
	WriteSkew Kind = "write-skew"
	// GSingle: a cycle with exactly one rw edge (read skew and similar).
	GSingle Kind = "G-single"
	// G2Item: a cycle with two or more rw edges.
	G2Item Kind = "G2-item"
	// G1c: a cycle of ww and wr edges only. PostgreSQL should never allow it.
	G1c Kind = "G1c"
	// G0: a cycle of ww edges only (dirty write). PostgreSQL should never allow it.
	G0 Kind = "G0"
)

// Anomaly is one cycle found in the graph.
type Anomaly struct {
	Kind Kind `json:"kind"`
	// Unexpected is true when every transaction in the cycle ran at an
	// isolation level that should have prevented this anomaly. That points to
	// a database bug or a gap in what IsoWatch captured.
	Unexpected bool `json:"unexpected"`
	// Edges in cycle order. Edges[i].To == Edges[i+1].From.
	Edges []Edge `json:"edges"`
	// Txns in cycle order, one per node. In ByTrace mode a node can hold
	// several transactions; the first one is listed here.
	Txns []*history.Txn `json:"txns"`
	// SkippedVersions is, for a lost update, how many newer versions of the
	// row the stale writer replaced without ever seeing them. 0 when unknown.
	SkippedVersions int `json:"skipped_versions,omitempty"`
	// Summary is a one line, plain English description.
	Summary string `json:"summary"`
	// Steps explain the cycle, two per edge: what each transaction did to
	// the row version that links them, and with which SQL.
	Steps []Step `json:"steps"`
	// Fix is a plain English suggestion for how to fix it.
	Fix string `json:"fix"`
}

// Step is one action by one transaction in an anomaly.
type Step struct {
	Trace  string `json:"trace,omitempty"`
	Txn    string `json:"txn"`
	Name   string `json:"name,omitempty"`
	Action string `json:"action"` // "read", "wrote" or "replaced"
	Table  string `json:"table"`
	Key    string `json:"key"`
	Xmin   uint32 `json:"xmin"`
	SQL    string `json:"sql,omitempty"`
}

// String renders a step as one line.
func (s Step) String() string {
	who := "txn " + shortID(s.Txn)
	if s.Name != "" {
		who = s.Name + " (" + who + ")"
	}
	if s.Trace != "" {
		who += " in trace " + shortID(s.Trace)
	}
	line := fmt.Sprintf("%s %s %s(%s) version %d", who, s.Action, s.Table, s.Key, s.Xmin)
	if s.SQL != "" {
		line += ": " + s.SQL
	}
	return line
}

// Names returns the distinct transaction names involved, in cycle order.
func (a Anomaly) Names() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range a.Txns {
		if t.Name != "" && !seen[t.Name] {
			seen[t.Name] = true
			out = append(out, t.Name)
		}
	}
	return out
}

// TraceIDs returns the distinct trace ids involved, in cycle order.
func (a Anomaly) TraceIDs() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range a.Txns {
		if t.TraceID != "" && !seen[t.TraceID] {
			seen[t.TraceID] = true
			out = append(out, t.TraceID)
		}
	}
	return out
}

func (c *Checker) describe(cyc []Edge) Anomaly {
	a := Anomaly{Edges: cyc}
	for _, e := range cyc {
		if n := c.nodes[e.From]; n != nil && len(n.txns) > 0 {
			a.Txns = append(a.Txns, n.txns[0])
		}
	}

	rw := 0
	allWW := true
	sameRow := true
	first := cyc[0].Version
	for _, e := range cyc {
		if e.Kind == RW {
			rw++
		}
		if e.Kind != WW {
			allWW = false
		}
		if e.Version.Table != first.Table || e.Version.Key != first.Key {
			sameRow = false
		}
	}

	switch {
	case rw == 0 && allWW:
		a.Kind = G0
	case rw == 0:
		a.Kind = G1c
	case sameRow:
		// On a single row, the only way to get a cycle with an rw edge is
		// for someone to overwrite a version they did not see.
		a.Kind = LostUpdate
	case rw == 1:
		a.Kind = GSingle
	case len(cyc) == 2:
		a.Kind = WriteSkew
	default:
		a.Kind = G2Item
	}

	if c.opts.GroupBy == ByTransaction {
		a.Unexpected = true
		for _, t := range a.Txns {
			if !forbids(t.Isolation, a.Kind) {
				a.Unexpected = false
				break
			}
		}
	}

	if a.Kind == LostUpdate {
		a.SkippedVersions = c.skippedIn(cyc)
	}
	for _, e := range cyc {
		a.Steps = append(a.Steps, c.stepsFor(e)...)
	}
	a.Summary = summarize(a)
	a.Fix = fixFor(a)
	return a
}

// stepsFor explains one edge: what its two transactions did to the version
// that links them.
func (c *Checker) stepsFor(e Edge) []Step {
	v := e.Version
	var fromAct, toAct string
	switch e.Kind {
	case WR:
		fromAct, toAct = "wrote", "read"
	case WW:
		fromAct, toAct = "wrote", "replaced"
	case RW:
		fromAct, toAct = "read", "replaced"
	}
	return []Step{c.step(e.From, v, fromAct), c.step(e.To, v, toAct)}
}

// step finds which transaction in a node did the action on v, and its SQL.
func (c *Checker) step(nodeID string, v history.Version, action string) Step {
	st := Step{Action: action, Table: v.Table, Key: v.Key, Xmin: v.Xmin}
	n := c.nodes[nodeID]
	if n == nil {
		return st
	}
	fill := func(t *history.Txn, q int) {
		st.Txn, st.Trace, st.Name, st.SQL = t.ID, t.TraceID, t.Name, t.Statement(q)
	}
	for _, t := range n.txns {
		switch action {
		case "read":
			for _, r := range t.Reads {
				if r.Version() == v {
					fill(t, r.Q)
					return st
				}
			}
		case "wrote":
			for _, w := range t.Writes {
				if cv, ok := w.Created(); ok && cv == v {
					fill(t, w.Q)
					return st
				}
			}
		case "replaced":
			for _, w := range t.Writes {
				if rv, ok := w.Replaced(); ok && rv == v {
					fill(t, w.Q)
					return st
				}
			}
		}
	}
	if len(n.txns) > 0 {
		st.Txn, st.Trace, st.Name = n.txns[0].ID, n.txns[0].TraceID, n.txns[0].Name
	}
	return st
}

// staleReads returns the SQL of the reads that saw a version someone else
// was replacing: the read side of each rw edge. These are the statements
// that usually need a lock. Steps come in pairs, two per edge.
func staleReads(a Anomaly) []string {
	var out []string
	seen := map[string]bool{}
	for i, e := range a.Edges {
		if e.Kind != RW || 2*i >= len(a.Steps) {
			continue
		}
		if sql := a.Steps[2*i].SQL; sql != "" && !seen[sql] {
			seen[sql] = true
			out = append(out, sql)
		}
	}
	return out
}

func quoteList(xs []string) string {
	q := make([]string, len(xs))
	for i, x := range xs {
		q[i] = "\"" + strings.TrimSpace(x) + "\""
	}
	return strings.Join(q, " and ")
}

// fixFor suggests a fix in plain English.
func fixFor(a Anomaly) string {
	reads := staleReads(a)
	lock := "Lock the rows when you read them (SELECT ... FOR UPDATE)"
	if len(reads) > 0 {
		lock = "Lock the rows when you read them: add FOR UPDATE to " + quoteList(reads)
	}
	switch a.Kind {
	case LostUpdate:
		return lock + ". Or write it as one relative update (for example SET balance = balance + $1) instead of reading and then writing. Or run these transactions at REPEATABLE READ or SERIALIZABLE and retry when PostgreSQL returns error 40001."
	case WriteSkew:
		return "Each transaction made its decision from rows the other one was changing. " + lock + ". Or enforce the rule with a database constraint. Or run these transactions at SERIALIZABLE and retry when PostgreSQL returns error 40001."
	case GSingle, G2Item:
		return "Run these transactions at SERIALIZABLE and retry when PostgreSQL returns error 40001. Or: " + strings.ToLower(lock[:1]) + lock[1:] + "."
	default:
		return "PostgreSQL should never allow this. Check for writes IsoWatch could not see (untracked statements or other apps writing the same rows). If there are none, it may be a database bug."
	}
}

// forbids reports whether a PostgreSQL isolation level prevents an anomaly.
// PostgreSQL's "repeatable read" is snapshot isolation.
func forbids(iso string, k Kind) bool {
	iso = strings.ToLower(strings.TrimSpace(iso))
	switch k {
	case G0, G1c:
		return iso == "read committed" || iso == "repeatable read" || iso == "serializable"
	case LostUpdate, GSingle:
		return iso == "repeatable read" || iso == "serializable"
	case WriteSkew, G2Item:
		return iso == "serializable"
	}
	return false
}

// baseRead returns the last read of the replaced row made before write w,
// or nil if the transaction never read that row first (a blind write).
func baseRead(t *history.Txn, w *history.Write, row history.Version) *history.Read {
	var seen *history.Read
	for i := range t.Reads {
		r := &t.Reads[i]
		if r.Table == row.Table && r.Key == row.Key && r.Stmt < w.Stmt && !t.Owns(r.Xmin) {
			if seen == nil || r.Stmt > seen.Stmt {
				seen = r
			}
		}
	}
	return seen
}

// skippedRange returns the versions a write replaced without seeing: every
// version after the one it read, up to and including the one it replaced.
// It returns nil when the write was up to date or blind.
func (c *Checker) skippedRange(t *history.Txn, w *history.Write) []history.Version {
	replaced, ok := w.Replaced()
	if !ok || t.Owns(replaced.Xmin) {
		return nil
	}
	seen := baseRead(t, w, replaced)
	if seen == nil || seen.Xmin == replaced.Xmin {
		return nil
	}
	var out []history.Version
	cur := replaced
	for steps := 0; steps < 100000 && cur.Xmin != seen.Xmin; steps++ {
		out = append(out, cur)
		ref, ok := c.writer[cur.String()]
		if !ok {
			break // older history was evicted or never captured
		}
		prev, ok := ref.write.Replaced()
		if !ok {
			break
		}
		cur = prev
	}
	return out
}

// skippedIn finds the stale writer in a lost update cycle and returns how
// many versions it skipped.
func (c *Checker) skippedIn(cyc []Edge) int {
	best := 0
	for _, e := range cyc {
		n := c.nodes[e.From]
		if n == nil {
			continue
		}
		for _, t := range n.txns {
			for i := range t.Writes {
				if k := len(c.skippedRange(t, &t.Writes[i])); k > best {
					best = k
				}
			}
		}
	}
	return best
}

// LostWrites counts writes, among transactions still in the window, whose
// effect never reached the current state of their row.
//
// For each row we start from its newest version(s) and follow "this write
// was based on that read" back in time. Those versions survived. A version
// is lost when it is not on that path AND some later writer replaced it
// without seeing it. Blind writes (no read first) are last-writer-wins on
// purpose and are not counted.
//
// For a counter where every transaction does n = n + 1 through a read and a
// write, this equals the number of increments that went missing.
func (c *Checker) LostWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Versions some writer skipped.
	skipped := map[string]bool{}
	var starts []history.Version
	for _, n := range c.nodes {
		for _, t := range n.txns {
			for i := range t.Writes {
				w := &t.Writes[i]
				for _, v := range c.skippedRange(t, w) {
					skipped[v.String()] = true
				}
				if created, ok := w.Created(); ok {
					if _, replaced := c.overwriter[created.String()]; !replaced {
						starts = append(starts, created)
					}
				} else if old, ok := w.Replaced(); ok {
					// A delete: what it was based on survived until it.
					if r := baseRead(t, w, old); r != nil {
						starts = append(starts, r.Version())
					}
				}
			}
		}
	}

	// Walk back from each newest version along "based on" links.
	survived := map[string]bool{}
	for _, v := range starts {
		cur := v
		for steps := 0; steps < 100000; steps++ {
			k := cur.String()
			if survived[k] {
				break
			}
			survived[k] = true
			ref, ok := c.writer[k]
			if !ok {
				break
			}
			old, ok := ref.write.Replaced()
			if !ok {
				break
			}
			r := baseRead(ref.txn, ref.write, old)
			if r == nil {
				break // blind write: it was not based on anything
			}
			cur = r.Version()
		}
	}

	lost := 0
	for k := range skipped {
		if !survived[k] {
			lost++
		}
	}
	return lost
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func label(t *history.Txn) string {
	if t == nil {
		return "?"
	}
	if t.TraceID != "" {
		return "trace " + shortID(t.TraceID)
	}
	return "txn " + shortID(t.ID)
}

func summarize(a Anomaly) string {
	if len(a.Txns) == 0 {
		return string(a.Kind)
	}
	row := fmt.Sprintf("%s(%s)", a.Edges[0].Version.Table, a.Edges[0].Version.Key)
	var s string
	switch a.Kind {
	case LostUpdate:
		s = fmt.Sprintf("Lost update on %s: a transaction wrote this row based on an old read and replaced %d newer version(s) it never saw.", row, max(a.SkippedVersions, 1))
	case WriteSkew:
		s = fmt.Sprintf("Write skew between %s and %s: each one read data the other was changing, then both wrote.", label(a.Txns[0]), label(a.Txns[1]))
	case GSingle:
		s = fmt.Sprintf("Read skew style anomaly (G-single) across %d transactions starting at %s.", len(a.Txns), row)
	case G2Item:
		s = fmt.Sprintf("Anti-dependency cycle (G2-item) across %d transactions starting at %s.", len(a.Txns), row)
	case G1c:
		s = fmt.Sprintf("Circular information flow (G1c) across %d transactions. PostgreSQL should prevent this.", len(a.Txns))
	case G0:
		s = fmt.Sprintf("Dirty write cycle (G0) across %d transactions. PostgreSQL should prevent this.", len(a.Txns))
	}
	if a.Unexpected {
		s += " This should be impossible at the isolation level used."
	}
	return s
}
