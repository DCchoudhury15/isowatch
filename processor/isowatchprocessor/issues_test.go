package isowatchprocessor

import (
	"fmt"
	"testing"
	"time"

	"github.com/DCchoudhury15/isowatch/history"
)

func TestFingerprintIgnoresValuesAndSpacing(t *testing.T) {
	same := [][]string{
		{
			"SELECT balance FROM acct WHERE id = $1",
			"select balance\n  from acct\twhere id=$7",
			"SELECT balance FROM acct WHERE id = 42",
			"SELECT balance FROM acct WHERE id = 42 -- by id",
			"SELECT /* hint */ balance FROM acct WHERE id = 4.2e1",
		},
		{
			"SELECT * FROM t WHERE id IN (1, 2, 3) AND name = 'x'",
			"select * from t where id in ($1, $2) and name = $3",
			"SELECT * FROM t WHERE id IN ($1) AND name = E'it\\'s'",
			"SELECT * FROM t WHERE id IN (7,8,9,10) AND name = 'it''s'",
		},
		{
			"INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)",
			"INSERT INTO t (a, b) VALUES (1, 'x')",
			"insert into t (a, b) values ($1, $2), ($3, $4), ($5, $6)",
		},
		{
			"SELECT * FROM t WHERE id = ANY(ARRAY[$1, $2, $3])",
			"SELECT * FROM t WHERE id = ANY(ARRAY[1])",
		},
		{
			"UPDATE t SET body = $$it's$$ WHERE id = 1",
			"UPDATE t SET body = $tag$a $$ b$tag$ WHERE id = 2",
			"UPDATE t SET body = 'x' WHERE id = 3",
		},
	}
	for _, group := range same {
		want := fingerprint(group[0])
		for _, q := range group[1:] {
			if got := fingerprint(q); got != want {
				t.Errorf("fingerprint(%q)\n  = %q\nwant %q", q, got, want)
			}
		}
	}

	different := [][2]string{
		{"SELECT balance FROM acct WHERE id = $1", "SELECT balance FROM acct WHERE id = $1 FOR UPDATE"},
		{"SELECT balance FROM acct WHERE id = $1", "SELECT balance FROM accounts WHERE id = $1"},
		{`SELECT "Balance" FROM acct`, `SELECT "balance" FROM acct`},
		{"SELECT a FROM t WHERE x = 1", "SELECT a FROM t WHERE y = 1"},
		{"SELECT t1.a FROM t1", "SELECT t2.a FROM t2"},
	}
	for _, d := range different {
		if fingerprint(d[0]) == fingerprint(d[1]) {
			t.Errorf("%q and %q must not share a fingerprint (%q)", d[0], d[1], fingerprint(d[0]))
		}
	}
}

// One bug hit on many rows, with different values in the SQL, is one issue.
// In per_issue mode it gets one span, then at most one more per interval.
func TestPerIssueReportsOncePerInterval(t *testing.T) {
	cfg := createDefaultConfig() // report: per_issue, issue_interval: 1m
	p, sink, clock := startProcessor(t, nopSettings(), cfg)

	for n := 0; n < 20; n++ {
		r := depositRace
		if n%2 == 1 {
			// The same code, sent with values inlined.
			r.sel = fmt.Sprintf("SELECT balance FROM acct WHERE id = %d", n)
			r.upd = fmt.Sprintf("UPDATE acct SET balance = %d WHERE id = %d", 100+n, n)
		}
		sendRace(t, p, n, r)
		clock.Add(time.Second)
	}
	got := spansNamed(sink, history.AnomalySpan)
	if len(got) != 1 {
		t.Fatalf("20 occurrences of one bug in 20s should give 1 span, got %d", len(got))
	}
	first := got[0]
	id := strAttr(t, first, "isowatch.issue.id")
	if c := intAttr(t, first, "isowatch.issue.count"); c != 1 {
		t.Fatalf("first span count = %d", c)
	}
	if v, _ := first.Attributes().Get("isowatch.issue.new"); !v.Bool() {
		t.Fatal("first span must say the issue is new")
	}
	firstSeen := strAttr(t, first, "isowatch.issue.first_seen")
	if firstSeen != "2026-11-06T10:00:00Z" {
		t.Fatalf("first_seen = %q", firstSeen)
	}
	if p.issues.len() != 1 {
		t.Fatalf("want 1 issue, got %d", p.issues.len())
	}

	// After the interval, the next occurrence is reported again, with the
	// running count.
	clock.Add(time.Minute)
	sendRace(t, p, 20, depositRace)
	got = spansNamed(sink, history.AnomalySpan)
	if len(got) != 2 {
		t.Fatalf("after the interval want 2 spans, got %d", len(got))
	}
	again := got[1]
	if strAttr(t, again, "isowatch.issue.id") != id {
		t.Fatal("the same bug must keep its issue id")
	}
	if c := intAttr(t, again, "isowatch.issue.count"); c != 21 {
		t.Fatalf("count = %d, want 21", c)
	}
	if u := intAttr(t, again, "isowatch.issue.unreported"); u != 19 {
		t.Fatalf("unreported = %d, want 19", u)
	}
	if strAttr(t, again, "isowatch.issue.first_seen") != firstSeen {
		t.Fatal("first_seen must not change")
	}
	if v, _ := again.Attributes().Get("isowatch.issue.new"); v.Bool() {
		t.Fatal("a repeat is not new")
	}
	// The repeat is a full anomaly span in the trace that completed it.
	if again.TraceID() != traceID(20, 2) || strAttr(t, again, "isowatch.anomaly.kind") != "lost-update" {
		t.Fatal("repeat span must be the full anomaly in its own trace")
	}
}

func TestReportEveryAddsOneSpanPerOccurrence(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Report = "every"
	p, sink, _ := startProcessor(t, nopSettings(), cfg)
	for n := 0; n < 5; n++ {
		sendRace(t, p, n, depositRace)
	}
	got := spansNamed(sink, history.AnomalySpan)
	if len(got) != 5 {
		t.Fatalf("want 5 spans, got %d", len(got))
	}
	id := strAttr(t, got[0], "isowatch.issue.id")
	for i, s := range got {
		if strAttr(t, s, "isowatch.issue.id") != id {
			t.Fatal("all occurrences belong to one issue")
		}
		if c := intAttr(t, s, "isowatch.issue.count"); c != int64(i+1) {
			t.Fatalf("span %d count = %d", i, c)
		}
	}
}

func TestDifferentBugsAreDifferentIssues(t *testing.T) {
	p, sink, _ := startProcessor(t, nopSettings(), createDefaultConfig())

	other := depositRace
	other.table = "public.orders"
	other.sel = "SELECT qty FROM orders WHERE id = $1"
	other.upd = "UPDATE orders SET qty = $1 WHERE id = $2"

	renamed := depositRace
	renamed.nameA, renamed.nameB = "withdraw", "withdraw"

	locked := depositRace // same table and names, different statement
	locked.sel = "SELECT balance, owner FROM acct WHERE id = $1"

	for i, r := range []race{depositRace, other, renamed, locked, depositRace} {
		sendRace(t, p, i, r)
	}
	got := spansNamed(sink, history.AnomalySpan)
	if len(got) != 4 {
		t.Fatalf("want 4 issues (the last race repeats the first), got %d spans", len(got))
	}
	ids := map[string]bool{}
	for _, s := range got {
		ids[strAttr(t, s, "isowatch.issue.id")] = true
	}
	if len(ids) != 4 {
		t.Fatalf("want 4 different issue ids, got %v", ids)
	}
}

func TestIssuesAreBounded(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.MaxIssues = 2
	p, sink, _ := startProcessor(t, nopSettings(), cfg)

	bug := func(name string) race {
		r := depositRace
		r.nameA, r.nameB = name, name
		return r
	}
	sendRace(t, p, 0, bug("a"))
	sendRace(t, p, 1, bug("b"))
	sendRace(t, p, 2, bug("a")) // a is now the most recently seen
	sendRace(t, p, 3, bug("c")) // evicts b, the least recently seen
	if n := p.issues.len(); n != 2 {
		t.Fatalf("want 2 issues kept, got %d", n)
	}
	if len(spansNamed(sink, history.AnomalySpan)) != 3 {
		t.Fatal("a, b and c are each new once")
	}
	sendRace(t, p, 4, bug("a")) // still known: no new span
	if len(spansNamed(sink, history.AnomalySpan)) != 3 {
		t.Fatal("a must still be known")
	}
	sendRace(t, p, 5, bug("b")) // forgotten, so it is new again
	got := spansNamed(sink, history.AnomalySpan)
	if len(got) != 4 {
		t.Fatalf("b was forgotten and must be reported as new, got %d spans", len(got))
	}
	if v, _ := got[3].Attributes().Get("isowatch.issue.new"); !v.Bool() {
		t.Fatal("b must be new again")
	}
}
