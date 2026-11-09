package isowatchprocessor

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DCchoudhury15/isowatch/checker"
)

// An issue is one bug: every anomaly with the same signature. The signature
// is the kind, the tables, the transaction names and the fingerprints of the
// SQL statements involved. Values in the SQL do not matter, so one buggy
// endpoint gives one issue however many rows and requests it hits.
type issue struct {
	id         string
	kind       checker.Kind
	tables     []string
	names      []string
	statements []string

	firstSeen  time.Time
	lastSeen   time.Time
	count      int64
	lostWrites int64

	lastSpan   time.Time // when an anomaly span was last added for it
	unreported int64     // occurrences since then without a span
	lastNotify time.Time // when the webhook was last called for it

	elem *list.Element
}

// issueView is a copy of an issue at one moment, safe to use without the lock.
type issueView struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Tables     []string  `json:"tables"`
	Names      []string  `json:"names,omitempty"`
	Statements []string  `json:"statements,omitempty"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	Count      int64     `json:"count"`
	LostWrites int64     `json:"lost_writes"`
	// Unreported is how many occurrences before this one got no span.
	Unreported int64 `json:"-"`
}

func (i *issue) view() issueView {
	return issueView{
		ID: i.id, Kind: string(i.kind), Tables: i.tables, Names: i.names, Statements: i.statements,
		FirstSeen: i.firstSeen, LastSeen: i.lastSeen, Count: i.count, LostWrites: i.lostWrites,
		Unreported: i.unreported,
	}
}

// seen is what observe decided about one occurrence.
type seen struct {
	issue  issueView
	isNew  bool
	span   bool // add an anomaly span for this occurrence
	notify bool // call the webhook for this occurrence (new, or a due repeat)
}

// issues keeps the known issues in memory, least recently seen first out.
type issues struct {
	mu       sync.Mutex
	byID     map[string]*issue
	lru      *list.List // front = most recently seen
	max      int
	interval time.Duration
	every    bool // report: every
	repeat   bool // webhook.repeat

	// Issues seen since the last lost writes measurement.
	active map[string]bool
}

func newIssues(cfg *Config) *issues {
	return &issues{
		byID:     map[string]*issue{},
		lru:      list.New(),
		max:      cfg.MaxIssues,
		interval: cfg.IssueInterval,
		every:    cfg.Report == reportEvery,
		repeat:   cfg.Webhook.Repeat,
		active:   map[string]bool{},
	}
}

// observe records one occurrence of an issue at time now.
func (s *issues) observe(g sig, now time.Time) seen {
	id := g.id
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[id] = true

	is := s.byID[id]
	if is == nil {
		is = &issue{id: id, kind: g.kind, tables: g.tables, names: g.names, statements: g.stmts, firstSeen: now}
		is.elem = s.lru.PushFront(is)
		s.byID[id] = is
		for s.lru.Len() > s.max {
			old := s.lru.Remove(s.lru.Back()).(*issue)
			delete(s.byID, old.id)
			delete(s.active, old.id)
		}
		is.count = 1
		is.lastSeen = now
		is.lastSpan = now
		is.lastNotify = now
		return seen{issue: is.view(), isNew: true, span: true, notify: true}
	}

	s.lru.MoveToFront(is.elem)
	is.count++
	is.lastSeen = now
	out := seen{}
	if s.every || now.Sub(is.lastSpan) >= s.interval {
		out.span = true
	}
	if s.repeat && now.Sub(is.lastNotify) >= s.interval {
		out.notify = true
		is.lastNotify = now
	}
	out.issue = is.view()
	if out.span {
		is.lastSpan = now
		is.unreported = 0
	} else {
		is.unreported++
	}
	return out
}

// takeActive returns the issues seen since the last call, and forgets them.
func (s *issues) takeActive() []issueView {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []issueView
	for id := range s.active {
		if is := s.byID[id]; is != nil {
			out = append(out, is.view())
		}
	}
	clear(s.active)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// addLostWrites adds n lost writes to an issue, if it is still known.
func (s *issues) addLostWrites(id string, n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if is := s.byID[id]; is != nil {
		is.lostWrites += n
	}
}

func (s *issues) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// sig is the signature of an anomaly: what makes two anomalies one issue.
type sig struct {
	id     string
	kind   checker.Kind
	tables []string
	names  []string
	stmts  []string
}

// signature returns the issue signature of an anomaly.
func signature(a checker.Anomaly) sig {
	kind := a.Kind
	tables := anomalyTables(a)
	names := append([]string(nil), a.Names()...)
	sort.Strings(names)
	var stmts []string
	seenStmt := map[string]bool{}
	for _, st := range a.Steps {
		if st.SQL == "" {
			continue
		}
		f := fingerprint(st.SQL)
		if f != "" && !seenStmt[f] {
			seenStmt[f] = true
			stmts = append(stmts, f)
		}
	}
	sort.Strings(stmts)

	h := sha256.New()
	write := func(section string, xs []string) {
		h.Write([]byte(section))
		for _, x := range xs {
			h.Write([]byte{0})
			h.Write([]byte(x))
		}
		h.Write([]byte{'\n'})
	}
	write("kind", []string{string(kind)})
	write("tables", tables)
	write("names", names)
	write("statements", stmts)
	return sig{id: hex.EncodeToString(h.Sum(nil)[:8]), kind: kind, tables: tables, names: names, stmts: stmts}
}

// anomalyTables returns the distinct tables in an anomaly's cycle, sorted.
func anomalyTables(a checker.Anomaly) []string {
	var out []string
	seen := map[string]bool{}
	for _, e := range a.Edges {
		if t := e.Version.Table; t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// fingerprint reduces SQL to its shape: literal values and placeholders
// become "?", lists of them become one "?", comments are removed, words are
// lower case and spacing is the same everywhere. So
//
//	SELECT * FROM t WHERE id IN (1, 2, 3) AND name = 'x'
//	select *  from t where id in ($1, $2) and name = $3
//
// both become "select * from t where id in ( ? ) and name = ?".
func fingerprint(sql string) string {
	var toks []string
	emit := func(t string) { toks = append(toks, t) }
	isIdent := func(c byte) bool {
		return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c >= 0x80
	}
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	const ops = "+-*/<>=~!@#%^&|`?:"

	s := sql
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			depth := 0
			for i < len(s) {
				if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(s) && s[i] == '*' && s[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
		case c == '\'':
			// A string. E'...' strings allow backslash escapes.
			escapes := len(toks) > 0 && toks[len(toks)-1] == "e" && i > 0 && (s[i-1] == 'e' || s[i-1] == 'E')
			if escapes {
				toks = toks[:len(toks)-1]
			}
			i++
			for i < len(s) {
				if escapes && s[i] == '\\' {
					i += 2
					continue
				}
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			emit("?")
		case c == '"':
			j := i + 1
			for j < len(s) {
				if s[j] == '"' {
					if j+1 < len(s) && s[j+1] == '"' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			emit(s[i:j])
			i = j
		case c == '$' && i+1 < len(s) && isDigit(s[i+1]):
			i++
			for i < len(s) && isDigit(s[i]) {
				i++
			}
			emit("?")
		case c == '$':
			// A dollar quoted string: $$...$$ or $tag$...$tag$.
			j := i + 1
			for j < len(s) && s[j] != '$' && isIdent(s[j]) {
				j++
			}
			if j < len(s) && s[j] == '$' {
				tag := s[i : j+1]
				if end := strings.Index(s[j+1:], tag); end >= 0 {
					i = j + 1 + end + len(tag)
					emit("?")
					continue
				}
			}
			emit("$")
			i++
		case isDigit(c) || (c == '.' && i+1 < len(s) && isDigit(s[i+1])):
			for i < len(s) && (isIdent(s[i]) || s[i] == '.' ||
				((s[i] == '+' || s[i] == '-') && (s[i-1] == 'e' || s[i-1] == 'E'))) {
				i++
			}
			emit("?")
		case isIdent(c):
			j := i
			for j < len(s) && isIdent(s[j]) {
				j++
			}
			emit(strings.ToLower(s[i:j]))
			i = j
		case strings.IndexByte(ops, c) >= 0:
			j := i
			for j < len(s) && strings.IndexByte(ops, s[j]) >= 0 {
				j++
			}
			emit(s[i:j])
			i = j
		default:
			emit(string(c))
			i++
		}
	}

	// Collapse lists: ( ? , ? , ? ) and [ ? , ? ] become one item, and so do
	// repeated groups like VALUES ( ? ) , ( ? ).
	toks = collapseLists(toks)
	return strings.Join(toks, " ")
}

func collapseLists(toks []string) []string {
	for changed := true; changed; {
		changed = false
		var out []string
		for i := 0; i < len(toks); i++ {
			// ( ? , ? ... ) or [ ? , ? ... ]
			if (toks[i] == "(" || toks[i] == "[") && i+1 < len(toks) && toks[i+1] == "?" {
				closer := ")"
				if toks[i] == "[" {
					closer = "]"
				}
				j := i + 2
				for j+1 < len(toks) && toks[j] == "," && toks[j+1] == "?" {
					j += 2
				}
				if j < len(toks) && toks[j] == closer && j > i+2 {
					out = append(out, toks[i], "?", closer)
					i = j
					changed = true
					continue
				}
			}
			// ( ? ) , ( ? ) , ...
			if toks[i] == "(" && i+4 < len(toks) && toks[i+1] == "?" && toks[i+2] == ")" &&
				toks[i+3] == "," && toks[i+4] == "(" {
				j := i + 3
				for j+3 < len(toks) && toks[j] == "," && toks[j+1] == "(" && toks[j+2] == "?" && toks[j+3] == ")" {
					j += 4
				}
				if j > i+3 {
					out = append(out, "(", "?", ")")
					i = j - 1
					changed = true
					continue
				}
			}
			out = append(out, toks[i])
		}
		toks = out
	}
	return toks
}
