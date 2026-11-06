// Package isowatchtest turns IsoWatch into a check for "go test" and CI.
//
// Wrap your pool in a test, run the code under test through the returned
// Monitor, and the test fails if IsoWatch sees a lost update, write skew or
// any other isolation anomaly actually happen:
//
//	func TestDeposit(t *testing.T) {
//		pool := ... // a pool on PostgreSQL 18
//		m := isowatchtest.New(t, pool, isowatchtest.Tables("accounts"))
//		isowatchtest.Concurrently(t, 20, func(i int) error {
//			return m.Do(t.Context(), pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
//				... read the balance, write it back ...
//			}, capture.WithName("deposit"))
//		})
//	}
//
// Only the test database needs PostgreSQL 18. Production can run an older
// version: the races are the same.
package isowatchtest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/checker"
)

// The kinds of anomaly a test usually expects. They are the same values as
// in package checker, repeated here so a test needs one import less.
const (
	LostUpdate = checker.LostUpdate
	WriteSkew  = checker.WriteSkew
	GSingle    = checker.GSingle
	G2Item     = checker.G2Item
)

// defaultWindow keeps every transaction of a normal test run in memory, so
// LostWrites counts the whole test. It is longer than go test's default
// timeout of 10 minutes.
const defaultWindow = time.Hour

// Option configures New.
type Option func(*config)

type config struct {
	tables       []string
	checker      checker.Options
	ignoreTables []string
	ignoreNames  []string
	byTrace      bool
}

// Tables sets the tables to watch, like "accounts" or "billing.ledger".
// Each must be a plain table with a primary key. Without this option, New
// watches every plain table with a primary key in the schemas on the
// pool's search_path, as they exist when New is called.
func Tables(names ...string) Option {
	return func(c *config) { c.tables = append(c.tables, names...) }
}

// IgnoreTables lists tables where races are acceptable. An anomaly is
// ignored when every row in it belongs to one of these tables.
func IgnoreTables(names ...string) Option {
	return func(c *config) { c.ignoreTables = append(c.ignoreTables, names...) }
}

// IgnoreNames lists transaction names (see capture.WithName) that are
// allowed to race. An anomaly is ignored when any transaction in it has one
// of these names.
func IgnoreNames(names ...string) Option {
	return func(c *config) { c.ignoreNames = append(c.ignoreNames, names...) }
}

// CheckerOptions sets the checker options. IgnoreTables and IgnoreNames
// given as separate options are added to the lists in o. A zero Window
// becomes one hour, so the whole test stays in memory.
func CheckerOptions(o checker.Options) Option {
	return func(c *config) { c.checker = o }
}

// GroupByTrace checks whole requests instead of single transactions: every
// transaction with the same trace id becomes one node in the graph. This
// finds races where a request reads in one transaction and writes in a
// later one.
//
// It is off by default, for two reasons. First, every concurrent request
// must have its own trace id (use NewTrace): if the calls in a test share
// one trace, they become one node and the races between them are not seen.
// Second, a request that reads a row in one transaction and later changes
// it with a relative update (SET n = n + 1) in another is reported even
// though no data was lost.
func GroupByTrace() Option {
	return func(c *config) { c.byTrace = true }
}

// Monitor is a capture.Monitor that reports to the test. Use it exactly like
// capture.Monitor: m.Do(...), m.Begin(...). The embedded field gives the
// *capture.Monitor itself, for code that takes one.
type Monitor struct {
	*capture.Monitor

	t    testing.TB
	sink *capture.CheckerSink

	mu        sync.Mutex
	anomalies []checker.Anomaly
	expected  map[checker.Kind]bool
	reported  int // anomalies[:reported] were already reported by ExpectNoAnomalies
}

// New starts watching the tables and returns a Monitor. When the test ends,
// the test fails with a report of every anomaly IsoWatch saw, unless the
// test expected it (see ExpectAnomaly).
//
// Transactions are checked one by one (checker.ByTransaction), so every
// report is a real isolation anomaly between database transactions. Keys
// are shown as their real values (capture.RawKeys), which makes reports
// easier to read.
//
// New also opens all of the pool's connections (up to MaxConns) right away,
// so that calls started together by Concurrently do not wait for new
// connections and drift apart.
//
// New fails the test if the server is older than PostgreSQL 18 or a table
// cannot be watched.
func New(t testing.TB, pool *pgxpool.Pool, opts ...Option) *Monitor {
	t.Helper()
	var c config
	for _, o := range opts {
		o(&c)
	}
	copts := c.checker
	if copts.Window <= 0 {
		copts.Window = defaultWindow
	}
	copts.IgnoreTables = append(append([]string(nil), copts.IgnoreTables...), c.ignoreTables...)
	copts.IgnoreNames = append(append([]string(nil), copts.IgnoreNames...), c.ignoreNames...)
	if c.byTrace {
		copts.GroupBy = checker.ByTrace
	}

	ctx := context.Background()
	tables := c.tables
	if len(tables) == 0 {
		var err error
		if tables, err = discoverTables(ctx, pool); err != nil {
			t.Fatalf("isowatchtest: finding tables to watch: %v", err)
		}
		if len(tables) == 0 {
			t.Fatalf("isowatchtest: no table with a primary key found on the search_path. Create the tables before calling isowatchtest.New, or name them with isowatchtest.Tables(...).")
		}
	}

	m := &Monitor{t: t, expected: map[checker.Kind]bool{}}
	m.sink = capture.NewCheckerSink(copts, m.add)
	mon, err := capture.New(ctx, pool, capture.Config{
		Tables:  tables,
		Sink:    m.sink,
		KeyMode: capture.RawKeys,
	})
	if err != nil {
		m.sink.Close()
		if errors.Is(err, capture.ErrUnsupportedServer) {
			var version string
			_ = pool.QueryRow(ctx, "SHOW server_version").Scan(&version)
			t.Fatalf("isowatchtest: the test database runs PostgreSQL %s, but IsoWatch needs PostgreSQL 18 or newer (it uses RETURNING old.xmin). Run your tests against postgres:18. Production can stay on an older version.", version)
		}
		t.Fatalf("isowatchtest: %v", err)
	}
	m.Monitor = mon
	t.Cleanup(m.finish)
	if err := warm(ctx, pool); err != nil {
		t.Logf("isowatchtest: could not open all %d pool connections up front (%v); concurrent calls may wait for connections and race less", pool.Config().MaxConns, err)
	}
	return m
}

// warm opens every connection the pool may have, then returns them to it.
// A new pool opens connections only when they are needed, and opening one
// takes milliseconds. Without this, calls that Concurrently starts at the
// same moment wait for new connections one by one and drift apart, so the
// races they should show often do not happen.
//
// It only takes the connections nobody is using, and gives up after a few
// seconds, so a caller that holds a connection cannot make it wait forever.
func warm(ctx context.Context, pool *pgxpool.Pool) error {
	n := int(pool.Config().MaxConns - pool.Stat().AcquiredConns())
	if n <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conns := make([]*pgxpool.Conn, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conns[i], errs[i] = pool.Acquire(ctx)
		}()
	}
	wg.Wait()
	for _, c := range conns {
		if c != nil {
			c.Release()
		}
	}
	return errors.Join(errs...)
}

// discoverTables lists the plain tables with a primary key in the schemas
// on the search_path.
func discoverTables(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
SELECT format('%I.%I', n.nspname, c.relname)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND NOT c.relispartition
  AND n.nspname = ANY (current_schemas(false))
  AND EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// add is called by the checker for each anomaly, on the goroutine that
// committed the transaction.
func (m *Monitor) add(a checker.Anomaly) {
	m.mu.Lock()
	m.anomalies = append(m.anomalies, a)
	m.mu.Unlock()
}

var knownKinds = map[checker.Kind]bool{
	checker.LostUpdate: true, checker.WriteSkew: true, checker.GSingle: true,
	checker.G2Item: true, checker.G1c: true, checker.G0: true,
}

// ExpectAnomaly says this test expects IsoWatch to see at least one anomaly
// of this kind, for example a test that proves a bug exists. Anomalies of
// this kind no longer fail the test. If none is seen by the end of the test,
// the test fails. Anomalies of other kinds still fail it.
func (m *Monitor) ExpectAnomaly(kind checker.Kind) {
	m.t.Helper()
	if !knownKinds[kind] {
		m.t.Errorf("isowatchtest: ExpectAnomaly(%q): unknown kind. Use isowatchtest.LostUpdate, WriteSkew, GSingle or G2Item.", kind)
		return
	}
	m.mu.Lock()
	m.expected[kind] = true
	m.mu.Unlock()
}

// ExpectNoAnomalies fails the test now if IsoWatch has seen any anomaly so
// far, expected or not. It also fails if IsoWatch has not checked a single
// transaction, because then "no anomalies" proves nothing: the code under
// test must start its transactions with m.Do or m.Begin.
func (m *Monitor) ExpectNoAnomalies() {
	m.t.Helper()
	m.mu.Lock()
	all := append([]checker.Anomaly(nil), m.anomalies...)
	m.reported = len(m.anomalies)
	m.mu.Unlock()
	if len(all) > 0 {
		m.t.Error(m.report(all, "IsoWatch saw %s, and the test expects none.", true))
		return
	}
	if m.sink.Checker().Stats().Txns == 0 {
		m.t.Error("isowatchtest: ExpectNoAnomalies: IsoWatch has not checked any transaction, so this proves nothing. " +
			"Start the transactions of the code under test with m.Do or m.Begin (not pool.Begin), " +
			"and make sure they read or write a watched table.\n" + m.statsLine())
	}
}

// Anomalies returns every anomaly seen so far, in the order they were found.
// Anomalies matched by IgnoreTables or IgnoreNames are not included.
func (m *Monitor) Anomalies() []checker.Anomaly {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]checker.Anomaly(nil), m.anomalies...)
}

// LostWrites counts the writes whose effect never reached the current value
// of their row, because a later writer replaced them without seeing them.
// For a counter that every call increments with a read and a write, this is
// exactly how many increments the database lost.
func (m *Monitor) LostWrites() int {
	return m.sink.Checker().LostWrites()
}

// finish runs when the test ends.
func (m *Monitor) finish() {
	m.t.Helper()
	failedBefore := m.t.Failed()
	m.sink.Close()

	m.mu.Lock()
	all := append([]checker.Anomaly(nil), m.anomalies...)
	reported := m.reported
	expected := make(map[checker.Kind]bool, len(m.expected))
	for k, v := range m.expected {
		expected[k] = v
	}
	m.mu.Unlock()

	var unexpected, seenExpected []checker.Anomaly
	for i, a := range all {
		switch {
		case expected[a.Kind]:
			seenExpected = append(seenExpected, a)
		case i >= reported:
			unexpected = append(unexpected, a)
		}
	}
	if len(unexpected) > 0 {
		m.t.Error(m.report(unexpected, "IsoWatch saw %s that this test did not expect.", true))
	}

	kinds := make([]string, 0, len(expected))
	for k := range expected {
		kinds = append(kinds, string(k))
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		if count(all, checker.Kind(k)) > 0 {
			continue
		}
		msg := fmt.Sprintf("isowatchtest: the test expected a %s anomaly, but IsoWatch saw none.\n%s\n"+
			"A race only happens when the transactions overlap. Run more calls at once with isowatchtest.Concurrently, "+
			"repeat them with isowatchtest.Repeat, and give the pool at least as many connections as calls.", k, m.statsLine())
		if failedBefore {
			m.t.Log(msg) // the test already failed for another reason
		} else {
			m.t.Error(msg)
		}
	}
	if len(seenExpected) > 0 {
		m.t.Log(m.report(seenExpected, "IsoWatch saw %s, as the test expected.", false))
	}
}

func count(as []checker.Anomaly, k checker.Kind) int {
	n := 0
	for _, a := range as {
		if a.Kind == k {
			n++
		}
	}
	return n
}

// statsLine says how much IsoWatch checked, and what it could not track.
func (m *Monitor) statsLine() string {
	cs := m.sink.Checker().Stats()
	s := m.Stats()
	line := fmt.Sprintf("IsoWatch checked %d committed transaction(s) and tracked %d statement(s).", cs.Txns, s.Tracked)
	if cs.Suppressed > 0 {
		line += fmt.Sprintf(" %d anomaly(s) matched IgnoreTables or IgnoreNames.", cs.Suppressed)
	}
	if s.Untracked > 0 {
		reasons := make([]string, 0, len(s.Reasons))
		for r, n := range s.Reasons {
			reasons = append(reasons, fmt.Sprintf("%s (%d)", r, n))
		}
		sort.Strings(reasons)
		line += fmt.Sprintf(" %d statement(s) were run unchanged and not tracked: %s.", s.Untracked, strings.Join(reasons, ", "))
	}
	return line
}
