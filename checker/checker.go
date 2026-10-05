// Package checker finds isolation anomalies in a stream of committed
// transactions.
//
// It follows Adya's model (the same model used by tools like Elle). Between
// two transactions there can be three kinds of dependency edges:
//
//	ww: B overwrote a row version that A wrote.
//	wr: B read a row version that A wrote.
//	rw: A read a row version that B later replaced. (A did not see B's write.)
//
// If these edges form a cycle, the transactions could not have run one after
// another in any order. That cycle is the anomaly. Because IsoWatch knows the
// exact row versions (from PostgreSQL's xmin), every edge is a fact, not a
// guess.
//
// Transactions may arrive in any order. Edges are found from both sides, so a
// cycle is found when its last transaction arrives.
package checker

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DCchoudhury15/isowatch/history"
)

// EdgeKind is the kind of dependency between two transactions.
type EdgeKind string

const (
	WW EdgeKind = "ww"
	WR EdgeKind = "wr"
	RW EdgeKind = "rw"
)

// GroupMode decides what one node in the graph stands for.
type GroupMode int

const (
	// ByTransaction makes each database transaction its own node. Cycles are
	// real isolation anomalies inside the database.
	ByTransaction GroupMode = iota
	// ByTrace puts every transaction from the same request (same trace id)
	// into one node. This finds anomalies across a whole request, for example
	// a read in one transaction and a write in a later transaction.
	ByTrace
)

// Options configure a Checker.
type Options struct {
	// Window is how long a committed transaction is kept. A cycle can only
	// form between transactions that overlap in time, so the window must be
	// longer than your longest transaction. Default: 2 minutes.
	Window time.Duration
	// GroupBy decides what a node is. Default: ByTransaction.
	GroupBy GroupMode
	// MaxDepth limits cycle length. Default: 16.
	MaxDepth int
	// MaxVisit limits how many nodes one search may visit. Default: 10000.
	MaxVisit int
	// IgnoreTables lists tables where anomalies are acceptable. An anomaly is
	// ignored when every row in its cycle belongs to one of these tables.
	// Use "schema.table" or just "table".
	IgnoreTables []string
	// IgnoreNames lists transaction names that are allowed to race. An
	// anomaly is ignored when any transaction in it has one of these names.
	IgnoreNames []string
}

// Edge is one dependency between two nodes.
type Edge struct {
	From    string          `json:"from"`
	To      string          `json:"to"`
	Kind    EdgeKind        `json:"kind"`
	Version history.Version `json:"version"` // the row version that links them
}

type writeRef struct {
	node  string
	txn   *history.Txn
	write *history.Write
}

type node struct {
	id         string
	txns       []*history.Txn
	out        map[string]map[EdgeKind]Edge // to -> kind -> edge
	in         map[string]bool              // nodes with an edge into this one
	versions   map[string]bool              // version keys this node is indexed under
	lastCommit time.Time
}

// Stats are running counters.
type Stats struct {
	Txns       int64 `json:"txns"`
	Edges      int64 `json:"edges"`
	Anomalies  int64 `json:"anomalies"`
	Suppressed int64 `json:"suppressed"` // anomalies matched by an ignore rule
	Evicted    int64 `json:"evicted"`
	Live       int   `json:"live"`
}

// Checker is safe for concurrent use.
type Checker struct {
	opts Options

	mu          sync.Mutex
	nodes       map[string]*node
	writer      map[string]writeRef // version -> who wrote it
	overwriter  map[string]writeRef // version -> who replaced it
	readers     map[string][]string // version -> nodes that read it
	reported    map[string]time.Time
	stats       Stats
	lastEvicted time.Time
}

// New returns a Checker.
func New(opts Options) *Checker {
	if opts.Window <= 0 {
		opts.Window = 2 * time.Minute
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = 16
	}
	if opts.MaxVisit <= 0 {
		opts.MaxVisit = 10000
	}
	return &Checker{
		opts:       opts,
		nodes:      map[string]*node{},
		writer:     map[string]writeRef{},
		overwriter: map[string]writeRef{},
		readers:    map[string][]string{},
		reported:   map[string]time.Time{},
	}
}

// Stats returns a copy of the counters.
func (c *Checker) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Live = len(c.nodes)
	return s
}

func (c *Checker) nodeID(t *history.Txn) string {
	if c.opts.GroupBy == ByTrace && t.TraceID != "" {
		return "trace:" + t.TraceID
	}
	return t.ID
}

// owns reports whether any transaction in the node wrote the given xmin.
func (n *node) owns(xmin uint32) bool {
	for _, t := range n.txns {
		if t.Owns(xmin) {
			return true
		}
	}
	return false
}

// Add records a committed transaction and returns any new anomalies.
func (c *Checker) Add(t *history.Txn) []Anomaly {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.Txns++
	id := c.nodeID(t)
	n := c.nodes[id]
	if n == nil {
		n = &node{
			id:       id,
			out:      map[string]map[EdgeKind]Edge{},
			in:       map[string]bool{},
			versions: map[string]bool{},
		}
		c.nodes[id] = n
	}
	n.txns = append(n.txns, t)
	if t.Commit.After(n.lastCommit) {
		n.lastCommit = t.Commit
	}

	// 1. Index the versions this transaction created. Transactions can
	// arrive in any order, so also link to anyone who already arrived and
	// read (wr) or replaced (ww) one of these versions.
	for i := range t.Writes {
		w := &t.Writes[i]
		v, ok := w.Created()
		if !ok {
			continue
		}
		k := v.String()
		if _, exists := c.writer[k]; !exists {
			c.writer[k] = writeRef{node: id, txn: t, write: w}
			n.versions[k] = true
		}
		if o, ok := c.overwriter[k]; ok && o.node != id {
			c.addEdge(id, o.node, WW, v)
		}
		for _, rd := range c.readers[k] {
			if rd != id {
				c.addEdge(id, rd, WR, v)
			}
		}
	}

	// 2. Reads: who wrote what we read (wr), and who replaced it (rw).
	for _, r := range t.Reads {
		if n.owns(r.Xmin) {
			continue // reading our own write is not a dependency
		}
		v := r.Version()
		k := v.String()
		if w, ok := c.writer[k]; ok && w.node != id {
			c.addEdge(w.node, id, WR, v)
		}
		if !contains(c.readers[k], id) {
			c.readers[k] = append(c.readers[k], id)
			n.versions[k] = true
		}
		if o, ok := c.overwriter[k]; ok && o.node != id {
			c.addEdge(id, o.node, RW, v)
		}
	}

	// 3. Writes: whose version we replaced (ww), and who had read it (rw).
	for i := range t.Writes {
		w := &t.Writes[i]
		v, ok := w.Replaced()
		if !ok {
			continue
		}
		// Updating a row twice in one transaction replaces our own first
		// version. Both versions share our xmin, so this is not a dependency
		// and must not mark us as the overwriter of that version.
		if n.owns(v.Xmin) {
			continue
		}
		k := v.String()
		if _, exists := c.overwriter[k]; !exists {
			c.overwriter[k] = writeRef{node: id, txn: t, write: w}
			n.versions[k] = true
		}
		if wr, ok := c.writer[k]; ok && wr.node != id {
			c.addEdge(wr.node, id, WW, v)
		}
		for _, rd := range c.readers[k] {
			if rd != id {
				c.addEdge(rd, id, RW, v)
			}
		}
	}

	// 4. Every new edge touches this node, so every new cycle passes
	// through it. Look for the shortest cycle through each neighbor.
	var found []Anomaly
	for _, cyc := range c.cyclesThrough(n) {
		key := cycleKey(cyc)
		if _, seen := c.reported[key]; seen {
			continue
		}
		c.reported[key] = t.Commit
		a := c.describe(cyc)
		if c.ignored(a) {
			c.stats.Suppressed++
			continue
		}
		c.stats.Anomalies++
		found = append(found, a)
	}
	return found
}

// ignored reports whether an ignore rule matches the anomaly.
func (c *Checker) ignored(a Anomaly) bool {
	for _, t := range a.Txns {
		if t.Name != "" && contains(c.opts.IgnoreNames, t.Name) {
			return true
		}
	}
	if len(c.opts.IgnoreTables) == 0 {
		return false
	}
	for _, e := range a.Edges {
		if !tableMatches(c.opts.IgnoreTables, e.Version.Table) {
			return false
		}
	}
	return true
}

// tableMatches matches "schema.table" against a list of "schema.table" or
// bare "table" names.
func tableMatches(list []string, qualified string) bool {
	bare := qualified
	if i := strings.LastIndexByte(qualified, '.'); i >= 0 {
		bare = qualified[i+1:]
	}
	for _, x := range list {
		if x == qualified || x == bare {
			return true
		}
	}
	return false
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func (c *Checker) addEdge(from, to string, kind EdgeKind, v history.Version) {
	if from == to {
		return
	}
	fn, tn := c.nodes[from], c.nodes[to]
	if fn == nil || tn == nil {
		return
	}
	m := fn.out[to]
	if m == nil {
		m = map[EdgeKind]Edge{}
		fn.out[to] = m
	}
	if _, ok := m[kind]; ok {
		return
	}
	m[kind] = Edge{From: from, To: to, Kind: kind, Version: v}
	tn.in[from] = true
	c.stats.Edges++
}

// bestEdge picks one edge between two nodes. It prefers ww, then wr, then
// rw, so a cycle is labelled with the most specific anomaly.
func bestEdge(m map[EdgeKind]Edge) Edge {
	for _, k := range []EdgeKind{WW, WR, RW} {
		if e, ok := m[k]; ok {
			return e
		}
	}
	return Edge{}
}

// cyclesThrough returns, for each out-neighbor X of n, the shortest cycle
// n -> X -> ... -> n (if one exists).
func (c *Checker) cyclesThrough(n *node) [][]Edge {
	var cycles [][]Edge
	seen := map[string]bool{}
	targets := make([]string, 0, len(n.out))
	for to := range n.out {
		targets = append(targets, to)
	}
	sort.Strings(targets) // deterministic results
	for _, x := range targets {
		path := c.shortestPath(x, n.id)
		if path == nil {
			continue
		}
		cyc := append([]Edge{bestEdge(n.out[x])}, path...)
		k := cycleKey(cyc)
		if !seen[k] {
			seen[k] = true
			cycles = append(cycles, cyc)
		}
	}
	return cycles
}

// shortestPath finds the shortest edge path from -> to with BFS.
func (c *Checker) shortestPath(from, to string) []Edge {
	if from == to {
		return []Edge{}
	}
	type step struct {
		prev string
		edge Edge
	}
	parent := map[string]step{from: {}}
	frontier := []string{from}
	visits := 0
	for depth := 1; depth < c.opts.MaxDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, id := range frontier {
			nd := c.nodes[id]
			if nd == nil {
				continue
			}
			outs := make([]string, 0, len(nd.out))
			for o := range nd.out {
				outs = append(outs, o)
			}
			sort.Strings(outs)
			for _, o := range outs {
				if _, ok := parent[o]; ok {
					continue
				}
				visits++
				if visits > c.opts.MaxVisit {
					return nil
				}
				parent[o] = step{prev: id, edge: bestEdge(nd.out[o])}
				if o == to {
					var path []Edge
					for cur := o; cur != from; cur = parent[cur].prev {
						path = append([]Edge{parent[cur].edge}, path...)
					}
					return path
				}
				next = append(next, o)
			}
		}
		frontier = next
	}
	return nil
}

func cycleKey(cyc []Edge) string {
	ids := make([]string, 0, len(cyc))
	for _, e := range cyc {
		ids = append(ids, e.From)
	}
	sort.Strings(ids)
	return strings.Join(ids, "|")
}

// Evict drops transactions that committed more than Window before now.
// Call it on a timer (for example every few seconds) to bound memory.
func (c *Checker) Evict(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictLocked(now)
}

func (c *Checker) evictLocked(now time.Time) int {
	cutoff := now.Add(-c.opts.Window)
	removed := 0
	for id, n := range c.nodes {
		if !n.lastCommit.Before(cutoff) {
			continue
		}
		for k := range n.versions {
			if w, ok := c.writer[k]; ok && w.node == id {
				delete(c.writer, k)
			}
			if o, ok := c.overwriter[k]; ok && o.node == id {
				delete(c.overwriter, k)
			}
			if rs, ok := c.readers[k]; ok {
				kept := rs[:0]
				for _, r := range rs {
					if r != id {
						kept = append(kept, r)
					}
				}
				if len(kept) == 0 {
					delete(c.readers, k)
				} else {
					c.readers[k] = kept
				}
			}
		}
		for from := range n.in {
			if fn := c.nodes[from]; fn != nil {
				delete(fn.out, id)
			}
		}
		for to := range n.out {
			if tn := c.nodes[to]; tn != nil {
				delete(tn.in, id)
			}
		}
		delete(c.nodes, id)
		removed++
	}
	for k, at := range c.reported {
		if at.Before(cutoff) {
			delete(c.reported, k)
		}
	}
	c.stats.Evicted += int64(removed)
	return removed
}
