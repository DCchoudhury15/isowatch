package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
	"github.com/DCchoudhury15/isowatch/history"
	"github.com/DCchoudhury15/isowatch/protection"
)

// schema creates a fresh schema and returns its name.
func schema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := fmt.Sprintf("iwcov_%d", rand.Int63())
	if _, err := pool.Exec(context.Background(), "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA "+name+" CASCADE") })
	return name
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

var nopSink = SinkFunc(func(context.Context, *history.Txn) {})

// WatchAll must watch every plain table with a primary key, skip (and say
// so) tables it cannot watch, and honor all three ExcludeTables forms.
func TestWatchAllFindsTablesAndSkipsOthers(t *testing.T) {
	pool := testPool(t)
	s, s2 := schema(t, pool), schema(t, pool)
	mustExec(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s.accounts (id int PRIMARY KEY, balance int NOT NULL);
CREATE TABLE %[1]s.events (at timestamptz, what text);
CREATE TABLE %[1]s.parted (id int PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE %[1]s.parted_1 PARTITION OF %[1]s.parted FOR VALUES FROM (0) TO (100);
CREATE TABLE %[1]s.secret (id int PRIMARY KEY);
CREATE TABLE %[1]s.iwcov_by_name (id int PRIMARY KEY);
CREATE TABLE %[2]s.other (id int PRIMARY KEY);
INSERT INTO %[1]s.accounts VALUES (1, 0);`, s, s2))

	log := &syncLog{}
	m, err := New(context.Background(), pool, Config{
		WatchAll:      true,
		ExcludeTables: []string{s + ".secret", "iwcov_by_name", s2 + ".*"},
		Sink:          nopSink,
		Logger:        slog.New(slog.NewTextHandler(log, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	watched := m.Tables()
	if !slices.Contains(watched, s+".accounts") {
		t.Errorf("%s.accounts has a primary key and should be watched; watched: %v", s, watched)
	}
	for _, not := range []string{s + ".events", s + ".parted", s + ".parted_1", s + ".secret", s + ".iwcov_by_name", s2 + ".other"} {
		if slices.Contains(watched, not) {
			t.Errorf("%s should not be watched", not)
		}
	}

	skipped := map[string]string{}
	for _, st := range m.SkippedTables() {
		skipped[st.Table] = st.Reason
	}
	if r := skipped[s+".events"]; r != "no primary key" {
		t.Errorf("events skipped with reason %q, want %q", r, "no primary key")
	}
	if r := skipped[s+".parted"]; !strings.Contains(r, "partitioned") {
		t.Errorf("parted skipped with reason %q, want a partitioned table reason", r)
	}
	for _, not := range []string{s + ".parted_1", s + ".secret", s2 + ".other"} {
		if _, ok := skipped[not]; ok {
			t.Errorf("%s should not be listed as skipped (partitions and excluded tables are left out)", not)
		}
	}
	if got := log.String(); !strings.Contains(got, "some tables are not watched") || !strings.Contains(got, s+".events (no primary key)") {
		t.Errorf("the logger should say which tables are not watched and why, got:\n%s", got)
	}

	// A table found by WatchAll is really tracked.
	ctx := context.Background()
	err = m.Do(ctx, pool, pgx.TxOptions{}, func(tx *Tx) error {
		var b int
		if err := tx.QueryRow(ctx, "SELECT balance FROM "+s+".accounts WHERE id = 1").Scan(&b); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "UPDATE "+s+".accounts SET balance = $1 WHERE id = 1", b+1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if c := m.Coverage(); c.Tracked != 2 || c.Untracked != 0 {
		t.Errorf("tracked %d, untracked %d; want 2 and 0", c.Tracked, c.Untracked)
	}
}

// ExcludeTables also removes tables listed in Config.Tables.
func TestExcludeTablesWinsOverTables(t *testing.T) {
	pool := testPool(t)
	a := table(t, pool, "id int PRIMARY KEY")
	b := table(t, pool, "id int PRIMARY KEY")
	m, err := New(context.Background(), pool, Config{Tables: []string{a, b}, ExcludeTables: []string{b}, Sink: nopSink})
	if err != nil {
		t.Fatal(err)
	}
	got := m.Tables()
	if len(got) != 1 || !strings.HasSuffix(got[0], "."+a) {
		t.Fatalf("watched %v, want only %s", got, a)
	}
}

// The protection table has a primary key, so WatchAll would find it. It
// must be left out, or New refuses to start because the protection table
// is watched.
func TestWatchAllSkipsProtectionTable(t *testing.T) {
	pool := testPool(t)
	s := schema(t, pool)
	store, err := protection.NewPostgres(pool, s+".protected")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	m, err := New(context.Background(), pool, Config{WatchAll: true, Sink: nopSink, ProtectionStore: store})
	if err != nil {
		t.Fatalf("WatchAll with a ProtectionStore must start: %v", err)
	}
	t.Cleanup(m.Close)
	if slices.Contains(m.Tables(), s+".protected") {
		t.Fatal("the protection table must not be watched")
	}
}

// Coverage counts tracked and untracked statements on watched tables,
// groups untracked ones by normalized SQL (no literal values), and keeps
// statements on no watched table out of the percentage.
func TestCoverageCountsAndGroups(t *testing.T) {
	pool := testPool(t)
	acc := table(t, pool, "id int PRIMARY KEY, owner text NOT NULL, balance int NOT NULL")
	mustExec(t, pool, "INSERT INTO "+acc+" VALUES (1, 'ann', 10), (2, 'bob', 20)")
	m, err := New(context.Background(), pool, Config{Tables: []string{acc}, Sink: nopSink})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = m.Do(ctx, pool, pgx.TxOptions{}, func(tx *Tx) error {
		run := func(sql string) error {
			rows, err := tx.Query(ctx, sql)
			if err != nil {
				return err
			}
			rows.Close()
			return rows.Err()
		}
		for _, sql := range []string{
			"SELECT balance FROM " + acc + " WHERE id = 1",                                  // tracked
			"UPDATE " + acc + " SET balance = 11 WHERE id = 1",                              // tracked
			"SELECT owner, sum(balance) FROM " + acc + " WHERE balance > 5 GROUP BY owner",  // untracked
			"SELECT owner, sum(balance) FROM " + acc + " WHERE balance > 7 GROUP BY owner",  // same, other literal
			"SELECT owner, sum(balance) FROM " + acc + " WHERE balance > 9  GROUP BY owner", // same, other spacing
			"WITH x AS (SELECT id FROM " + acc + ") SELECT count(*) FROM x",                 // untracked
			"SELECT 1", // other
		} {
			if err := run(sql); err != nil {
				return fmt.Errorf("%s: %w", sql, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	c := m.Coverage()
	if c.Tracked != 2 || c.Untracked != 4 || c.Other != 1 {
		t.Fatalf("tracked %d, untracked %d, other %d; want 2, 4, 1", c.Tracked, c.Untracked, c.Other)
	}
	if want := 2.0 * 100 / 6; c.Percent != want {
		t.Errorf("percent %.2f, want %.2f", c.Percent, want)
	}
	if c.Reasons["GROUP BY or HAVING"] != 3 || c.Reasons["WITH query"] != 1 {
		t.Errorf("reasons %v, want 3 GROUP BY or HAVING and 1 WITH query", c.Reasons)
	}
	if len(c.Top) != 2 {
		t.Fatalf("top has %d statements, want 2 (the three GROUP BY queries are one): %+v", len(c.Top), c.Top)
	}
	top := c.Top[0]
	if top.Count != 3 || top.Reason != "GROUP BY or HAVING" {
		t.Errorf("top statement %+v, want the GROUP BY query 3 times", top)
	}
	for _, lit := range []string{"> 5", "> 7", "> 9", "  "} {
		if strings.Contains(top.SQL, lit) {
			t.Errorf("top SQL %q should be normalized (no %q)", top.SQL, lit)
		}
	}

	report := m.CoverageReport()
	for _, want := range []string{
		"isowatch: tracked 33% of statements on watched tables (2 of 6)",
		"untracked: 3 GROUP BY or HAVING, 1 WITH query",
		"top untracked statements:",
		"     3  GROUP BY or HAVING: " + top.SQL,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report is missing %q:\n%s", want, report)
		}
	}
}

// The list of untracked statements is bounded. A frequent statement that
// shows up only after the list is full must still get in, with Over saying
// how much its count may be too high.
func TestCoverageListIsBounded(t *testing.T) {
	var c coverage
	plan := func(sql string) *rewrite.Plan {
		return &rewrite.Plan{Kind: rewrite.Untracked, Watched: true, Normalized: sql, Reason: "GROUP BY or HAVING"}
	}
	for i := range maxCoverageStatements + 50 {
		c.add(plan(fmt.Sprintf("SELECT count(*) FROM t%d GROUP BY a", i)))
	}
	const late = "SELECT owner FROM late GROUP BY owner"
	for range 5 {
		c.add(plan(late))
	}
	if n := len(c.stmts); n > maxCoverageStatements {
		t.Fatalf("%d statements kept, want at most %d", n, maxCoverageStatements)
	}
	s, ok := c.stmts[coverageKey{sql: late, reason: "GROUP BY or HAVING"}]
	if !ok {
		t.Fatal("a frequent statement that came late was not kept")
	}
	if s.Count < 5 || s.Over == 0 || s.Count-s.Over != 5 {
		t.Errorf("late statement count %d over %d; want count-over == 5 (its exact count) and over > 0", s.Count, s.Over)
	}
	if got := c.untracked.Load(); got != maxCoverageStatements+55 {
		t.Errorf("untracked total %d, want %d (the bound limits the list, not the count)", got, maxCoverageStatements+55)
	}
}

// Handler serves the coverage as JSON, the report as text, and nothing
// else.
func TestCoverageHandler(t *testing.T) {
	pool := testPool(t)
	acc := table(t, pool, "id int PRIMARY KEY, balance int NOT NULL")
	m, err := New(context.Background(), pool, Config{Tables: []string{acc}, Sink: nopSink})
	if err != nil {
		t.Fatal(err)
	}
	db := m.Wrap(pool)
	if _, err := db.Exec(context.Background(), "INSERT INTO "+acc+" VALUES (1, 0)"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Coverage Coverage `json:"coverage"`
		Stats    struct {
			Committed int64    `json:"committed"`
			Tracked   int64    `json:"tracked"`
			Protected []string `json:"protected"`
		} `json:"stats"`
	}
	err = json.NewDecoder(res.Body).Decode(&body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type %q", ct)
	}
	if body.Coverage.Tracked != 1 || body.Stats.Tracked != 1 || body.Stats.Committed != 1 {
		t.Errorf("got coverage.tracked %d, stats.tracked %d, stats.committed %d; want 1, 1, 1", body.Coverage.Tracked, body.Stats.Tracked, body.Stats.Committed)
	}
	if body.Stats.Protected == nil {
		t.Error(`stats.protected should be [] not null, so tools can read it as a list`)
	}

	res, err = http.Get(srv.URL + "?format=text")
	if err != nil {
		t.Fatal(err)
	}
	if text := mustRead(t, res); !strings.HasPrefix(text, "isowatch: tracked 100% of statements on watched tables (1 of 1)") {
		t.Errorf("text report:\n%s", text)
	}

	res, err = http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed || res.Header.Get("Allow") != "GET, HEAD" {
		t.Errorf("POST got %d, Allow %q; want 405 and GET, HEAD", res.StatusCode, res.Header.Get("Allow"))
	}
}

func mustRead(t *testing.T, res *http.Response) string {
	t.Helper()
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ReportEvery logs the coverage report, and Close stops it.
func TestReportEveryLogsUntilClose(t *testing.T) {
	pool := testPool(t)
	acc := table(t, pool, "id int PRIMARY KEY")
	log := &syncLog{}
	m, err := New(context.Background(), pool, Config{
		Tables:      []string{acc},
		Sink:        nopSink,
		Logger:      slog.New(slog.NewTextHandler(log, nil)),
		ReportEvery: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	eventually(t, 5*time.Second, "a coverage report in the log", func() bool {
		return strings.Contains(log.String(), "isowatch: no statements on the 1 watched tables yet")
	})

	m.Close()
	m.Close() // twice is fine
	after := log.String()
	time.Sleep(100 * time.Millisecond) // ten report periods
	if got := log.String(); got != after {
		t.Fatalf("the report kept being logged after Close:\n%s", strings.TrimPrefix(got, after))
	}
}

// An explicit WithName wins over the name in the context.
func TestExplicitNameWinsOverContext(t *testing.T) {
	pool := testPool(t)
	acc := table(t, pool, "id int PRIMARY KEY, n int NOT NULL")
	mustExec(t, pool, "INSERT INTO "+acc+" VALUES (1, 0)")
	var mu sync.Mutex
	var names []string
	sink := SinkFunc(func(_ context.Context, txn *history.Txn) {
		mu.Lock()
		names = append(names, txn.Name)
		mu.Unlock()
	})
	m, err := New(context.Background(), pool, Config{Tables: []string{acc}, Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithNameContext(context.Background(), "from-context")
	bump := func(tx *Tx) error {
		_, err := tx.Exec(ctx, "UPDATE "+acc+" SET n = n + 1 WHERE id = 1")
		return err
	}
	if err := m.Do(ctx, pool, pgx.TxOptions{}, bump, WithName("explicit")); err != nil {
		t.Fatal(err)
	}
	if err := m.Do(ctx, pool, pgx.TxOptions{}, bump); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(names, []string{"explicit", "from-context"}) {
		t.Fatalf("transaction names %q, want [explicit from-context]", names)
	}
}
