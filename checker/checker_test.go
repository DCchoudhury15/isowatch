package checker

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DCchoudhury15/isowatch/history"
)

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func rd(table, key string, xmin uint32, stmt int) history.Read {
	return history.Read{Table: table, Key: key, Xmin: xmin, Stmt: stmt}
}

func upd(table, key string, oldX, newX uint32, stmt int) history.Write {
	return history.Write{Table: table, OldKey: key, OldXmin: oldX, NewKey: key, NewXmin: newX, Stmt: stmt}
}

func txn(id string, xids []uint32, reads []history.Read, writes []history.Write) *history.Txn {
	return &history.Txn{ID: id, Xids: xids, Isolation: "read committed", Start: at(0), Commit: at(1), Reads: reads, Writes: writes}
}

// The exact history PostgreSQL 18 produced in our two-session experiment:
// both read balance at xmin 760, T1 wrote 761, T2 then replaced 761 with 762.
func lostUpdatePair() (*history.Txn, *history.Txn) {
	t1 := txn("T1", []uint32{761}, []history.Read{rd("acct", "1", 760, 0)}, []history.Write{upd("acct", "1", 760, 761, 1)})
	t2 := txn("T2", []uint32{762}, []history.Read{rd("acct", "1", 760, 0)}, []history.Write{upd("acct", "1", 761, 762, 1)})
	return t1, t2
}

func mustOne(t *testing.T, got []Anomaly, kind Kind) Anomaly {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("want 1 anomaly of kind %s, got %d: %+v", kind, len(got), got)
	}
	if got[0].Kind != kind {
		t.Fatalf("want kind %s, got %s (%s)", kind, got[0].Kind, got[0].Summary)
	}
	return got[0]
}

func TestLostUpdate(t *testing.T) {
	c := New(Options{})
	t1, t2 := lostUpdatePair()
	if got := c.Add(t1); len(got) != 0 {
		t.Fatalf("T1 alone should be clean, got %+v", got)
	}
	a := mustOne(t, c.Add(t2), LostUpdate)
	if a.SkippedVersions != 1 {
		t.Fatalf("want 1 skipped version, got %d", a.SkippedVersions)
	}
	if a.Unexpected {
		t.Fatal("lost update is allowed at read committed")
	}
	if c.LostWrites() != 1 {
		t.Fatalf("LostWrites() = %d, want 1", c.LostWrites())
	}
}

func TestLostUpdateReverseArrival(t *testing.T) {
	c := New(Options{})
	t1, t2 := lostUpdatePair()
	if got := c.Add(t2); len(got) != 0 {
		t.Fatalf("T2 alone should be clean, got %+v", got)
	}
	mustOne(t, c.Add(t1), LostUpdate)
}

func TestWriteSkew(t *testing.T) {
	// Two doctors on call (rows a and b, both written by xid 100).
	// Each transaction reads both rows, sees two on call, and takes itself off.
	c := New(Options{})
	ta := txn("A", []uint32{201},
		[]history.Read{rd("doctors", "a", 100, 0), rd("doctors", "b", 100, 0)},
		[]history.Write{upd("doctors", "a", 100, 201, 1)})
	tb := txn("B", []uint32{202},
		[]history.Read{rd("doctors", "a", 100, 0), rd("doctors", "b", 100, 0)},
		[]history.Write{upd("doctors", "b", 100, 202, 1)})
	c.Add(ta)
	a := mustOne(t, c.Add(tb), WriteSkew)
	if a.Unexpected {
		t.Fatal("write skew is allowed at read committed")
	}
}

func TestSerialHistoryIsClean(t *testing.T) {
	c := New(Options{})
	t1 := txn("T1", []uint32{301}, []history.Read{rd("acct", "1", 300, 0)}, []history.Write{upd("acct", "1", 300, 301, 1)})
	t2 := txn("T2", []uint32{302}, []history.Read{rd("acct", "1", 301, 0)}, []history.Write{upd("acct", "1", 301, 302, 1)})
	t3 := txn("T3", nil, []history.Read{rd("acct", "1", 302, 0)}, nil)
	for _, x := range []*history.Txn{t3, t1, t2} { // any arrival order
		if got := c.Add(x); len(got) != 0 {
			t.Fatalf("serial history must be clean, got %+v", got)
		}
	}
	if c.LostWrites() != 0 {
		t.Fatalf("no writes were lost, got %d", c.LostWrites())
	}
}

func TestOwnReadsAndSavepointsIgnored(t *testing.T) {
	c := New(Options{})
	// Top xid 400, savepoint xid 401. It reads its own savepoint write.
	t1 := txn("T1", []uint32{400, 401},
		[]history.Read{rd("acct", "1", 401, 1)},
		[]history.Write{upd("acct", "1", 399, 401, 0), upd("acct", "1", 401, 400, 2)})
	if got := c.Add(t1); len(got) != 0 {
		t.Fatalf("own reads must not create edges, got %+v", got)
	}
	if c.Stats().Edges != 0 {
		t.Fatalf("want 0 edges, got %d", c.Stats().Edges)
	}
}

func TestThreeWayLostUpdateCountsBothLostWrites(t *testing.T) {
	// Counter at version 500. T1 and T2 both write it in turn (501, 502).
	// T3 read version 500 long ago and now writes over 502.
	c := New(Options{})
	t1 := txn("T1", []uint32{501}, []history.Read{rd("ctr", "x", 500, 0)}, []history.Write{upd("ctr", "x", 500, 501, 1)})
	t2 := txn("T2", []uint32{502}, []history.Read{rd("ctr", "x", 501, 0)}, []history.Write{upd("ctr", "x", 501, 502, 1)})
	t3 := txn("T3", []uint32{503}, []history.Read{rd("ctr", "x", 500, 0)}, []history.Write{upd("ctr", "x", 502, 503, 1)})
	c.Add(t1)
	c.Add(t2)
	got := c.Add(t3)
	if len(got) == 0 {
		t.Fatal("expected a lost update")
	}
	if got[0].Kind != LostUpdate {
		t.Fatalf("want lost update, got %s", got[0].Kind)
	}
	if c.LostWrites() != 2 {
		t.Fatalf("T3 destroyed the writes of T1 and T2, want 2, got %d", c.LostWrites())
	}
}

func TestUnexpectedAtStrongerIsolation(t *testing.T) {
	c := New(Options{})
	t1, t2 := lostUpdatePair()
	t1.Isolation, t2.Isolation = "repeatable read", "serializable"
	c.Add(t1)
	a := mustOne(t, c.Add(t2), LostUpdate)
	if !a.Unexpected {
		t.Fatal("a lost update at repeatable read or serializable must be flagged unexpected")
	}

	// Mixed levels: one transaction at read committed explains the anomaly.
	c2 := New(Options{})
	u1, u2 := lostUpdatePair()
	u1.Isolation = "serializable"
	c2.Add(u1)
	if mustOne(t, c2.Add(u2), LostUpdate).Unexpected {
		t.Fatal("not unexpected when one side ran at read committed")
	}
}

func TestG0Detected(t *testing.T) {
	// Two rows, each written by A then B in opposite orders. Impossible in
	// PostgreSQL, so it must be reported as unexpected.
	c := New(Options{})
	a := txn("A", []uint32{601}, nil, []history.Write{upd("r", "1", 600, 601, 0), upd("r", "2", 602, 601, 1)})
	b := txn("B", []uint32{602}, nil, []history.Write{upd("r", "1", 601, 602, 0), upd("r", "2", 600, 602, 1)})
	c.Add(a)
	got := mustOne(t, c.Add(b), G0)
	if !got.Unexpected {
		t.Fatal("G0 must be unexpected at read committed")
	}
}

func TestByTraceFindsCrossTransactionLostUpdate(t *testing.T) {
	// Each request reads in one transaction and writes in another one.
	// Inside the database every transaction is fine; across the request it
	// is a lost update.
	mk := func(id, trace string, xids []uint32, reads []history.Read, writes []history.Write, sec int) *history.Txn {
		x := txn(id, xids, reads, writes)
		x.TraceID = trace
		x.Commit = at(sec)
		return x
	}
	readA := mk("ra", "reqA", nil, []history.Read{rd("acct", "1", 700, 0)}, nil, 1)
	readB := mk("rb", "reqB", nil, []history.Read{rd("acct", "1", 700, 0)}, nil, 1)
	writeA := mk("wa", "reqA", []uint32{701}, nil, []history.Write{upd("acct", "1", 700, 701, 0)}, 2)
	writeB := mk("wb", "reqB", []uint32{702}, nil, []history.Write{upd("acct", "1", 701, 702, 0)}, 3)

	plain := New(Options{})
	for _, x := range []*history.Txn{readA, readB, writeA, writeB} {
		if got := plain.Add(x); len(got) != 0 {
			t.Fatalf("per transaction there is no cycle, got %+v", got)
		}
	}

	traced := New(Options{GroupBy: ByTrace})
	var all []Anomaly
	for _, x := range []*history.Txn{readA, readB, writeA, writeB} {
		all = append(all, traced.Add(x)...)
	}
	if len(all) != 1 || all[0].Kind != LostUpdate {
		t.Fatalf("want 1 request-level lost update, got %+v", all)
	}
	if all[0].Unexpected {
		t.Fatal("request-level anomalies are never 'unexpected'")
	}
}

func TestEvictForgetsOldTransactions(t *testing.T) {
	c := New(Options{Window: time.Minute})
	t1, t2 := lostUpdatePair()
	c.Add(t1)
	if n := c.Evict(at(1).Add(2 * time.Minute)); n != 1 {
		t.Fatalf("want 1 evicted, got %d", n)
	}
	if got := c.Add(t2); len(got) != 0 {
		t.Fatalf("T1 was evicted, no cycle possible, got %+v", got)
	}
	if s := c.Stats(); s.Live != 1 || s.Evicted != 1 {
		t.Fatalf("unexpected stats %+v", s)
	}
}

func TestNoDuplicateReports(t *testing.T) {
	c := New(Options{})
	t1, t2 := lostUpdatePair()
	c.Add(t1)
	mustOne(t, c.Add(t2), LostUpdate)
	// A later reader of the final version adds edges but no new cycle.
	t3 := txn("T3", nil, []history.Read{rd("acct", "1", 762, 0)}, nil)
	if got := c.Add(t3); len(got) != 0 {
		t.Fatalf("no new cycle, got %+v", got)
	}
	if c.Stats().Anomalies != 1 {
		t.Fatalf("want 1 anomaly total, got %d", c.Stats().Anomalies)
	}
}

func TestConcurrentAddIsSafe(t *testing.T) {
	c := New(Options{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			x := uint32(1000 + i)
			c.Add(txn(fmt.Sprintf("T%d", i), []uint32{x},
				[]history.Read{rd("acct", "1", 999, 0)},
				[]history.Write{upd("acct", fmt.Sprint(i), 999, x, 1)}))
		}(i)
	}
	wg.Wait()
	if c.Stats().Txns != 50 {
		t.Fatalf("want 50 txns, got %d", c.Stats().Txns)
	}
}

func permutations(xs []*history.Txn) [][]*history.Txn {
	if len(xs) <= 1 {
		return [][]*history.Txn{append([]*history.Txn(nil), xs...)}
	}
	var out [][]*history.Txn
	for i := range xs {
		rest := append(append([]*history.Txn(nil), xs[:i]...), xs[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]*history.Txn{xs[i]}, p...))
		}
	}
	return out
}

func TestArrivalOrderDoesNotMatter(t *testing.T) {
	mk := func() []*history.Txn {
		return []*history.Txn{
			txn("T1", []uint32{501}, []history.Read{rd("ctr", "x", 500, 0)}, []history.Write{upd("ctr", "x", 500, 501, 1)}),
			txn("T2", []uint32{502}, []history.Read{rd("ctr", "x", 501, 0)}, []history.Write{upd("ctr", "x", 501, 502, 1)}),
			txn("T3", []uint32{503}, []history.Read{rd("ctr", "x", 500, 0)}, []history.Write{upd("ctr", "x", 502, 503, 1)}),
			txn("T4", nil, []history.Read{rd("ctr", "x", 503, 0)}, nil),
		}
	}
	for i, order := range permutations(mk()) {
		c := New(Options{})
		var found []Anomaly
		for _, x := range order {
			found = append(found, c.Add(x)...)
		}
		if len(found) == 0 {
			t.Fatalf("order %d: lost update not found", i)
		}
		for _, a := range found {
			if a.Kind != LostUpdate {
				t.Fatalf("order %d: want only lost updates, got %s", i, a.Kind)
			}
		}
		if c.LostWrites() != 2 {
			t.Fatalf("order %d: want 2 lost writes, got %d", i, c.LostWrites())
		}
	}
}

func TestSelfUpdateTwiceIsNotADependency(t *testing.T) {
	// The serializable false positive found against real PostgreSQL:
	// A updates row 1 twice (both versions have xmin 3139). B later reads
	// A's final version and updates it. That history is serial (A then B).
	c := New(Options{})
	a := txn("A", []uint32{3139},
		[]history.Read{rd("acct", "1", 3126, 0)},
		[]history.Write{upd("acct", "1", 3126, 3139, 1), upd("acct", "1", 3139, 3139, 2)})
	b := txn("B", []uint32{3143},
		[]history.Read{rd("acct", "1", 3139, 0)},
		[]history.Write{upd("acct", "1", 3139, 3143, 1)})
	for _, order := range [][]*history.Txn{{a, b}, {b, a}} {
		c := New(Options{})
		for _, x := range order {
			if got := c.Add(x); len(got) != 0 {
				t.Fatalf("serial history, got %+v", got)
			}
		}
	}
	_ = c
}

func TestLostWritesCounterSemantics(t *testing.T) {
	// From a real run: base value a at xmin 2614. Each txn does n = n + 1.
	//   2615 read 2614 | 2616 read 2615 | 2617 read 2615 (replaced 2616)
	//   2618 read 2616 (replaced 2617) | 2619 read 2616 (replaced 2618)
	// Values: 2615=a+1, 2616=a+2, 2617=a+2, 2618=a+3, 2619=a+3.
	// 5 commits, final a+3, so exactly 2 increments were lost.
	c := New(Options{})
	c.Add(txn("A", []uint32{2615}, []history.Read{rd("c", "1", 2614, 0)}, []history.Write{upd("c", "1", 2614, 2615, 1)}))
	c.Add(txn("B", []uint32{2616}, []history.Read{rd("c", "1", 2615, 0)}, []history.Write{upd("c", "1", 2615, 2616, 1)}))
	c.Add(txn("C", []uint32{2617}, []history.Read{rd("c", "1", 2615, 0)}, []history.Write{upd("c", "1", 2616, 2617, 1)}))
	c.Add(txn("D", []uint32{2618}, []history.Read{rd("c", "1", 2616, 0)}, []history.Write{upd("c", "1", 2617, 2618, 1)}))
	c.Add(txn("E", []uint32{2619}, []history.Read{rd("c", "1", 2616, 0)}, []history.Write{upd("c", "1", 2618, 2619, 1)}))
	if got := c.LostWrites(); got != 2 {
		t.Fatalf("want 2 lost increments, got %d", got)
	}
}

func TestStepsAndFix(t *testing.T) {
	c := New(Options{})
	t1, t2 := lostUpdatePair()
	t1.Name, t2.Name = "deposit", "withdraw"
	t1.Statements = []string{"SELECT balance FROM acct WHERE id = $1", "UPDATE acct SET balance = $1 WHERE id = $2"}
	t2.Statements = t1.Statements
	t1.Reads[0].Q, t1.Writes[0].Q = 0, 1
	t2.Reads[0].Q, t2.Writes[0].Q = 0, 1
	c.Add(t1)
	a := mustOne(t, c.Add(t2), LostUpdate)
	if len(a.Steps) != 2*len(a.Edges) {
		t.Fatalf("want 2 steps per edge, got %d for %d edges", len(a.Steps), len(a.Edges))
	}
	var sawRead, sawReplace bool
	for _, st := range a.Steps {
		if st.Txn == "" || st.SQL == "" {
			t.Fatalf("step without txn or SQL: %+v", st)
		}
		if st.Action == "read" && st.Xmin == 760 {
			sawRead = true
		}
		if st.Action == "replaced" && st.Xmin == 761 && st.Name == "withdraw" {
			sawReplace = true
		}
	}
	if !sawRead || !sawReplace {
		t.Fatalf("steps miss the key facts: %+v", a.Steps)
	}
	if !strings.Contains(a.Fix, "FOR UPDATE") || !strings.Contains(a.Fix, "SELECT balance FROM acct WHERE id = $1") {
		t.Fatalf("fix should name the stale read: %s", a.Fix)
	}
	if got := a.Names(); len(got) != 2 {
		t.Fatalf("names = %v", got)
	}
}

func TestIgnoreRules(t *testing.T) {
	t1, t2 := lostUpdatePair()
	t2.Name = "touch-last-seen"

	byName := New(Options{IgnoreNames: []string{"touch-last-seen"}})
	byName.Add(t1)
	if got := byName.Add(t2); len(got) != 0 {
		t.Fatalf("ignored by name, got %+v", got)
	}
	if s := byName.Stats(); s.Suppressed != 1 || s.Anomalies != 0 {
		t.Fatalf("stats %+v", s)
	}

	u1, u2 := lostUpdatePair()
	byTable := New(Options{IgnoreTables: []string{"acct"}})
	byTable.Add(u1)
	if got := byTable.Add(u2); len(got) != 0 {
		t.Fatalf("ignored by table, got %+v", got)
	}

	w1, w2 := lostUpdatePair()
	other := New(Options{IgnoreTables: []string{"public.sessions"}, IgnoreNames: []string{"x"}})
	other.Add(w1)
	mustOne(t, other.Add(w2), LostUpdate)
}
