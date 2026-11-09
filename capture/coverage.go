package capture

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// Coverage tells you how much of your SQL IsoWatch could check. A statement
// IsoWatch could not rewrite safely runs unchanged and is "untracked": an
// anomaly that involves it can be missed. Coverage shows which ones they are.
type Coverage struct {
	// Tracked statements used a watched table and were recorded.
	Tracked int64 `json:"tracked"`
	// Untracked statements used a watched table but ran unchanged.
	Untracked int64 `json:"untracked"`
	// Other statements ran unchanged and used no watched table, or were
	// commands such as SET or CREATE INDEX. They do not count against
	// coverage.
	Other int64 `json:"other"`
	// Percent is Tracked / (Tracked + Untracked) * 100, or 0 when no
	// statement has used a watched table yet.
	Percent float64 `json:"tracked_percent"`
	// Reasons counts the untracked statements by why they were not tracked.
	Reasons map[string]int64 `json:"reasons"`
	// Top lists the most frequent untracked statements, most frequent first.
	// At most 200 distinct statements are kept, see UntrackedStatement.Over.
	Top []UntrackedStatement `json:"top"`
	// Watched lists the watched tables. Skipped lists the tables
	// Config.WatchAll found but could not watch.
	Watched []string       `json:"watched"`
	Skipped []SkippedTable `json:"skipped,omitempty"`
}

// UntrackedStatement is one kind of statement that ran unchanged.
type UntrackedStatement struct {
	// SQL is the normalized statement: literal values are replaced by $1,
	// $2, ..., whitespace is collapsed, and very long text is cut. It is
	// empty when the SQL could not be parsed.
	SQL    string `json:"sql"`
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
	// Over is how much Count may be too high. The list keeps at most 200
	// statements. When it is full, a new statement replaces the least
	// frequent one and starts from that one's count, so frequent statements
	// stay in the list even if they first appear late. Over is 0 for
	// statements whose count is exact.
	Over int64 `json:"over,omitempty"`
}

const (
	maxCoverageStatements = 200
	maxCoverageSQL        = 1000 // bytes of SQL text kept per statement
	reportTop             = 10   // statements shown by CoverageReport
)

type coverageKey struct{ sql, reason string }

// coverage counts untracked statements. Its zero value is ready to use.
type coverage struct {
	other     atomic.Int64
	untracked atomic.Int64

	mu      sync.Mutex
	reasons map[string]int64
	stmts   map[coverageKey]*UntrackedStatement

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// add counts one untracked statement.
func (c *coverage) add(p *rewrite.Plan) {
	if !p.Watched {
		c.other.Add(1)
		return
	}
	c.untracked.Add(1)
	sql := p.Normalized
	if sql == "" && p.Reason != "parse error" {
		sql = rewrite.Normalize(p.SQL) // only for plans made on the fly
	}
	k := coverageKey{sql: compactSQL(sql), reason: p.Reason}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reasons == nil {
		c.reasons = map[string]int64{}
		c.stmts = map[coverageKey]*UntrackedStatement{}
	}
	c.reasons[p.Reason]++
	if s, ok := c.stmts[k]; ok {
		s.Count++
		return
	}
	if len(c.stmts) < maxCoverageStatements {
		c.stmts[k] = &UntrackedStatement{SQL: k.sql, Reason: k.reason, Count: 1}
		return
	}
	// The list is full: replace the least frequent statement (the
	// "space saving" method), so memory stays bounded.
	var minKey coverageKey
	var minStmt *UntrackedStatement
	for key, s := range c.stmts {
		if minStmt == nil || s.Count < minStmt.Count {
			minKey, minStmt = key, s
		}
	}
	delete(c.stmts, minKey)
	c.stmts[k] = &UntrackedStatement{SQL: k.sql, Reason: k.reason, Count: minStmt.Count + 1, Over: minStmt.Count}
}

// compactSQL collapses whitespace and cuts very long text, so the report is
// readable and memory stays bounded.
func compactSQL(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= maxCoverageSQL {
		return s
	}
	cut := maxCoverageSQL
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + " ..."
}

// Coverage returns a snapshot of what IsoWatch tracked and what it could
// not, on the watched tables.
func (m *Monitor) Coverage() Coverage {
	c := Coverage{
		Tracked:   m.tracked.Load(),
		Untracked: m.cov.untracked.Load(),
		Other:     m.cov.other.Load(),
		Reasons:   map[string]int64{},
		Top:       []UntrackedStatement{},
		Watched:   m.Tables(),
		Skipped:   m.SkippedTables(),
	}
	m.cov.mu.Lock()
	for r, n := range m.cov.reasons {
		c.Reasons[r] = n
	}
	for _, s := range m.cov.stmts {
		c.Top = append(c.Top, *s)
	}
	m.cov.mu.Unlock()
	sort.Slice(c.Top, func(i, j int) bool {
		a, b := c.Top[i], c.Top[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		if a.Reason != b.Reason {
			return a.Reason < b.Reason
		}
		return a.SQL < b.SQL
	})
	if total := c.Tracked + c.Untracked; total > 0 {
		c.Percent = float64(c.Tracked) * 100 / float64(total)
	}
	return c
}

// summary is the first line of the report.
func (c Coverage) summary() string {
	total := c.Tracked + c.Untracked
	if total == 0 {
		return fmt.Sprintf("isowatch: no statements on the %d watched tables yet", len(c.Watched))
	}
	// Round down, so 100% means nothing was missed.
	pct := c.Tracked * 100 / total
	return fmt.Sprintf("isowatch: tracked %d%% of statements on watched tables (%d of %d)", pct, c.Tracked, total)
}

// reasonList renders "41 GROUP BY or HAVING, 3 WITH query", most first.
func (c Coverage) reasonList() string {
	type rc struct {
		reason string
		n      int64
	}
	list := make([]rc, 0, len(c.Reasons))
	for r, n := range c.Reasons {
		list = append(list, rc{r, n})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].n != list[j].n {
			return list[i].n > list[j].n
		}
		return list[i].reason < list[j].reason
	})
	parts := make([]string, len(list))
	for i, x := range list {
		parts[i] = fmt.Sprintf("%d %s", x.n, x.reason)
	}
	return strings.Join(parts, ", ")
}

// String renders the statement as one report line.
func (s UntrackedStatement) String() string {
	n := fmt.Sprint(s.Count)
	if s.Over > 0 {
		n = "~" + n
	}
	sql := s.SQL
	if sql == "" {
		sql = "(SQL that could not be parsed)"
	}
	return fmt.Sprintf("%6s  %s: %s", n, s.Reason, sql)
}

// CoverageReport returns a short report for people, for example:
//
//	isowatch: tracked 92% of statements on watched tables (1104 of 1200)
//	untracked: 41 GROUP BY or HAVING, 3 WITH query
//	top untracked statements:
//	    41  GROUP BY or HAVING: SELECT owner, sum(balance) FROM accounts GROUP BY owner
//	     3  WITH query: WITH x AS (SELECT ...) SELECT ...
//	not watched: public.events (no primary key)
func (m *Monitor) CoverageReport() string {
	return m.Coverage().report()
}

func (c Coverage) report() string {
	var b strings.Builder
	b.WriteString(c.summary())
	b.WriteString("\n")
	if c.Untracked > 0 {
		b.WriteString("untracked: " + c.reasonList() + "\n")
		b.WriteString("top untracked statements:\n")
		for i, s := range c.Top {
			if i == reportTop {
				break
			}
			b.WriteString(s.String() + "\n")
		}
	}
	if len(c.Skipped) > 0 {
		parts := make([]string, len(c.Skipped))
		for i, s := range c.Skipped {
			parts[i] = s.Table + " (" + s.Reason + ")"
		}
		b.WriteString("not watched: " + strings.Join(parts, ", ") + "\n")
	}
	return b.String()
}

// startReports logs the coverage report every Config.ReportEvery.
func (m *Monitor) startReports() {
	if m.cfg.Logger == nil || m.cfg.ReportEvery <= 0 {
		return
	}
	m.cov.stop = make(chan struct{})
	m.cov.done = make(chan struct{})
	go func() {
		defer close(m.cov.done)
		tick := time.NewTicker(m.cfg.ReportEvery)
		defer tick.Stop()
		for {
			select {
			case <-m.cov.stop:
				return
			case <-tick.C:
				m.logCoverage()
			}
		}
	}()
}

// logCoverage writes the report as one structured log line.
func (m *Monitor) logCoverage() {
	c := m.Coverage()
	args := []any{"tracked", c.Tracked, "untracked", c.Untracked, "other", c.Other}
	if c.Untracked > 0 {
		args = append(args, "untracked_by_reason", c.reasonList())
		for i, s := range c.Top {
			if i == 3 {
				break
			}
			args = append(args, fmt.Sprintf("top_%d", i+1), strings.TrimSpace(s.String()))
		}
	}
	m.cfg.Logger.Info(c.summary(), args...)
}

// stopReports stops the periodic coverage report. Safe to call more than
// once.
func (m *Monitor) stopReports() {
	m.cov.stopOnce.Do(func() {
		if m.cov.stop != nil {
			close(m.cov.stop)
			<-m.cov.done
		}
	})
}

// statsJSON is Stats with JSON names.
type statsJSON struct {
	Committed int64            `json:"committed"`
	Retried   int64            `json:"retried"`
	Protected []string         `json:"protected"`
	Tracked   int64            `json:"tracked"`
	Untracked int64            `json:"untracked"`
	Reasons   map[string]int64 `json:"reasons"`
}

// Handler serves the coverage and the Stats as JSON:
//
//	{"coverage": {...}, "stats": {...}}
//
// With ?format=text it serves CoverageReport instead. Mount it on an
// internal port, for example mux.Handle("/debug/isowatch", mon.Handler()).
// Normalized SQL contains no literal values, but it does show your queries.
func (m *Monitor) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Query().Get("format") == "text" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = fmt.Fprint(w, m.CoverageReport())
			return
		}
		s := m.Stats()
		body := struct {
			Coverage Coverage  `json:"coverage"`
			Stats    statsJSON `json:"stats"`
		}{
			Coverage: m.Coverage(),
			Stats: statsJSON{
				Committed: s.Committed, Retried: s.Retried, Protected: s.Protected,
				Tracked: s.Tracked, Untracked: s.Untracked, Reasons: s.Reasons,
			},
		}
		if body.Stats.Protected == nil {
			body.Stats.Protected = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(body)
	})
}
