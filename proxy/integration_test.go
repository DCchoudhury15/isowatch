package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

// These tests need PostgreSQL 18:
//
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./proxy/

var modes = []pgx.QueryExecMode{
	pgx.QueryExecModeCacheStatement,
	pgx.QueryExecModeCacheDescribe,
	pgx.QueryExecModeDescribeExec,
	pgx.QueryExecModeExec,
	pgx.QueryExecModeSimpleProtocol,
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ISOWATCH_TEST_DSN")
	if dsn == "" {
		t.Skip("ISOWATCH_TEST_DSN not set")
	}
	return dsn
}

// harness runs a proxy in front of the test database.
type harness struct {
	t      *testing.T
	direct *pgxpool.Pool
	mon    *capture.Monitor
	px     *Proxy
	port   uint16

	mu        sync.Mutex
	txns      []*history.Txn
	anomalies []checker.Anomaly
	checker   *capture.CheckerSink
}

// newHarness creates the tables (name -> columns) and starts a proxy that
// watches them.
func newHarness(t *testing.T, tables map[string]string) *harness {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()
	direct, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(direct.Close)
	h := &harness{t: t, direct: direct}
	var names []string
	for name, cols := range tables {
		h.exec(fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE TABLE %s (%s)", name, name, cols))
		t.Cleanup(func() { _, _ = direct.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
		names = append(names, name)
	}
	h.checker = capture.NewCheckerSink(checker.Options{Window: 10 * time.Minute}, func(a checker.Anomaly) {
		h.mu.Lock()
		h.anomalies = append(h.anomalies, a)
		h.mu.Unlock()
	})
	t.Cleanup(h.checker.Close)
	keep := capture.SinkFunc(func(_ context.Context, txn *history.Txn) {
		h.mu.Lock()
		h.txns = append(h.txns, txn)
		h.mu.Unlock()
	})
	h.mon, err = capture.New(ctx, direct, capture.Config{
		Tables: names, Sink: capture.MultiSink{keep, h.checker}, KeyMode: capture.RawKeys,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	h.px, err = New(Config{
		Upstream: net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))),
		Monitor:  h.mon,
		Logger:   slog.New(slog.NewTextHandler(testLog{t}, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.port = uint16(ln.Addr().(*net.TCPAddr).Port)
	go func() { _ = h.px.Serve(ln) }()
	t.Cleanup(func() {
		h.px.Close()
		if d := h.px.Stats().Desyncs; d != 0 {
			t.Errorf("the proxy lost track of the protocol on %d connections", d)
		}
	})
	return h
}

// testLog sends the proxy's log to the test output.
type testLog struct{ t *testing.T }

func (l testLog) Write(b []byte) (int, error) {
	l.t.Log(strings.TrimSpace(string(b)))
	return len(b), nil
}

func (h *harness) exec(sql string, args ...any) {
	h.t.Helper()
	if _, err := h.direct.Exec(context.Background(), sql, args...); err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
}

// pool connects through the proxy with one query exec mode.
func (h *harness) pool(mode pgx.QueryExecMode) *pgxpool.Pool {
	h.t.Helper()
	cfg, err := pgxpool.ParseConfig(testDSN(h.t))
	if err != nil {
		h.t.Fatal(err)
	}
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.Port = h.port
	cfg.ConnConfig.Fallbacks = nil
	cfg.ConnConfig.TLSConfig = nil
	cfg.ConnConfig.DefaultQueryExecMode = mode
	cfg.MaxConns = 32
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(p.Close)
	return p
}

// directPool connects straight to PostgreSQL with one query exec mode.
func (h *harness) directPool(mode pgx.QueryExecMode) *pgxpool.Pool {
	h.t.Helper()
	cfg, err := pgxpool.ParseConfig(testDSN(h.t))
	if err != nil {
		h.t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = mode
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(p.Close)
	return p
}

func (h *harness) reported() ([]*history.Txn, []checker.Anomaly) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*history.Txn(nil), h.txns...), append([]checker.Anomaly(nil), h.anomalies...)
}

func (h *harness) reset() {
	h.mu.Lock()
	h.txns, h.anomalies = nil, nil
	h.mu.Unlock()
	h.checker.Reset()
}

func kinds(as []checker.Anomaly) map[checker.Kind]int {
	out := map[checker.Kind]int{}
	for _, a := range as {
		out[a.Kind]++
	}
	return out
}

func tableName(prefix string) string { return fmt.Sprintf("%s_%d", prefix, rand.Int63()) }

// TestLostUpdateCountedExactly is the accuracy test through the proxy, in
// every query exec mode: many workers add 1 with a read-then-write pattern.
// IsoWatch must count exactly the writes the database lost.
func TestLostUpdateCountedExactly(t *testing.T) {
	ctr := tableName("px_ctr")
	h := newHarness(t, map[string]string{ctr: "id int PRIMARY KEY, n int"})
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; INSERT INTO %s VALUES (1, 0)", ctr, ctr))
			pool := h.pool(mode)
			ctx := context.Background()
			const workers, each = 16, 30
			var committed atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < each; i++ {
						err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
							var n int
							if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = $1", ctr), 1).Scan(&n); err != nil {
								return err
							}
							_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = $2", ctr), n+1, 1)
							return err
						})
						if err != nil {
							t.Error(err)
							return
						}
						committed.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()
			var final int64
			if err := h.direct.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)).Scan(&final); err != nil {
				t.Fatal(err)
			}
			lostInDB := committed.Load() - final
			lostSeen := int64(h.checker.Checker().LostWrites())
			txns, anomalies := h.reported()
			t.Logf("committed=%d final=%d lost(db)=%d lost(isowatch)=%d txns=%d anomalies=%v",
				committed.Load(), final, lostInDB, lostSeen, len(txns), kinds(anomalies))
			if int64(len(txns)) != committed.Load() {
				t.Fatalf("%d transactions committed, %d reported", committed.Load(), len(txns))
			}
			if lostInDB == 0 {
				t.Fatal("no write was lost, so this run proves nothing; the race did not happen")
			}
			if lostSeen != lostInDB {
				t.Fatalf("IsoWatch counted %d lost writes, the database lost %d", lostSeen, lostInDB)
			}
			for _, a := range anomalies {
				if a.Kind != checker.LostUpdate {
					t.Fatalf("only lost updates are possible here, got %s", a.Kind)
				}
			}
		})
	}
}

// TestAtomicUpdateIsQuiet: UPDATE ... SET n = n + 1 never loses a write, so
// IsoWatch must report nothing, in every mode, while still recording.
func TestAtomicUpdateIsQuiet(t *testing.T) {
	ctr := tableName("px_safe")
	h := newHarness(t, map[string]string{ctr: "id int PRIMARY KEY, n int"})
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; INSERT INTO %s VALUES (1, 0)", ctr, ctr))
			pool := h.pool(mode)
			ctx := context.Background()
			var wg sync.WaitGroup
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 20; i++ {
						if _, err := pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", ctr), 1); err != nil {
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			var final int
			h.direct.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)).Scan(&final)
			txns, anomalies := h.reported()
			if final != 320 {
				t.Fatalf("final = %d, want 320", final)
			}
			if len(txns) != 320 {
				t.Fatalf("%d transactions reported, want 320 (one per statement)", len(txns))
			}
			for _, txn := range txns {
				if len(txn.Writes) != 1 {
					t.Fatalf("each statement writes one row, got %+v", txn.Writes)
				}
			}
			if len(anomalies) != 0 {
				t.Fatalf("false alarm: %v", kinds(anomalies))
			}
		})
	}
}

// TestWriteSkewAcrossTables: two transactions each read two rows in two
// tables and each change a different one. Run step by step on two
// connections, so the skew always happens.
func TestWriteSkewAcrossTables(t *testing.T) {
	a, b := tableName("px_left"), tableName("px_right")
	h := newHarness(t, map[string]string{a: "id int PRIMARY KEY, on_call bool", b: "id int PRIMARY KEY, on_call bool"})
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; DELETE FROM %s; INSERT INTO %s VALUES (1, true); INSERT INTO %s VALUES (1, true)", a, b, a, b))
			pool := h.pool(mode)
			ctx := context.Background()
			c1, _ := pool.Acquire(ctx)
			defer c1.Release()
			c2, _ := pool.Acquire(ctx)
			defer c2.Release()
			opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead}
			t1, err := c1.BeginTx(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			t2, err := c2.BeginTx(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			count := func(tx pgx.Tx) int {
				n := 0
				var x, y bool
				if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT on_call FROM %s WHERE id = 1", a)).Scan(&x); err != nil {
					t.Fatal(err)
				}
				if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT on_call FROM %s WHERE id = 1", b)).Scan(&y); err != nil {
					t.Fatal(err)
				}
				if x {
					n++
				}
				if y {
					n++
				}
				return n
			}
			if count(t1) != 2 || count(t2) != 2 {
				t.Fatal("both should see two on call")
			}
			if _, err := t1.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 1", a)); err != nil {
				t.Fatal(err)
			}
			if _, err := t2.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 1", b)); err != nil {
				t.Fatal(err)
			}
			if err := t1.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := t2.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			txns, anomalies := h.reported()
			if len(txns) != 2 {
				t.Fatalf("%d transactions reported, want 2", len(txns))
			}
			for _, txn := range txns {
				if txn.Isolation != "repeatable read" {
					t.Fatalf("isolation %q, want repeatable read", txn.Isolation)
				}
			}
			if kinds(anomalies)[checker.WriteSkew] != 1 {
				t.Fatalf("want one write skew, got %v", kinds(anomalies))
			}
		})
	}
}

// TestFailedTransactionsAreNotReported: only transactions PostgreSQL
// committed are reported, and a rollback to a savepoint forgets the
// writes made after it.
func TestFailedTransactionsAreNotReported(t *testing.T) {
	tb := tableName("px_fail")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; INSERT INTO %s VALUES (1, 0), (2, 0)", tb, tb))
			pool := h.pool(mode)
			ctx := context.Background()
			upd := fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb)

			// 1. ROLLBACK.
			tx, _ := pool.Begin(ctx)
			if _, err := tx.Exec(ctx, upd, 1); err != nil {
				t.Fatal(err)
			}
			tx.Rollback(ctx)

			// 2. An error, then COMMIT: PostgreSQL answers ROLLBACK.
			c, _ := pool.Acquire(ctx)
			conn := c.Conn()
			if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, upd, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, "SELECT 1/0"); err == nil {
				t.Fatal("division by zero should fail")
			}
			if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
				t.Fatal(err)
			}

			// 3. A statement without BEGIN that fails.
			if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = n / 0 WHERE id = $1", tb), 1); err == nil {
				t.Fatal("division by zero should fail")
			}
			c.Release()

			// 4. Savepoint: the write after it is rolled back, the one before
			// it commits.
			tx, _ = pool.Begin(ctx)
			if _, err := tx.Exec(ctx, upd, 1); err != nil {
				t.Fatal(err)
			}
			sp, err := tx.Begin(ctx) // SAVEPOINT
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sp.Exec(ctx, upd, 2); err != nil {
				t.Fatal(err)
			}
			if err := sp.Rollback(ctx); err != nil { // ROLLBACK TO SAVEPOINT
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			txns, _ := h.reported()
			if len(txns) != 1 {
				t.Fatalf("%d transactions reported, want only the savepoint one: %+v", len(txns), txns)
			}
			w := txns[0].Writes
			if len(w) != 1 || w[0].NewKey != "1" {
				t.Fatalf("want only the write to row 1, got %+v", w)
			}
			var n1, n2 int
			h.direct.QueryRow(ctx, fmt.Sprintf("SELECT (SELECT n FROM %s WHERE id = 1), (SELECT n FROM %s WHERE id = 2)", tb, tb)).Scan(&n1, &n2)
			if n1 != 1 || n2 != 0 {
				t.Fatalf("database has n1=%d n2=%d, want 1 and 0", n1, n2)
			}
		})
	}
}

// TestTraceAndNameFromComments: a sqlcommenter comment links the
// transaction to the request's trace and names it.
func TestTraceAndNameFromComments(t *testing.T) {
	tb := tableName("px_trace")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	const traceID, spanID = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; INSERT INTO %s VALUES (1, 0)", tb, tb))
			pool := h.pool(mode)
			ctx := context.Background()
			comment := fmt.Sprintf("/*route='%%2Fdeposit',traceparent='00-%s-%s-01'*/", traceID, spanID)
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				var n int
				if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = $1 %s", tb, comment), 1).Scan(&n); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = 1 %s", tb, comment), n+1)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			txns, _ := h.reported()
			if len(txns) != 1 {
				t.Fatalf("%d transactions reported, want 1", len(txns))
			}
			got := txns[0]
			if got.TraceID != traceID || got.SpanID != spanID {
				t.Fatalf("trace %s/%s, want %s/%s", got.TraceID, got.SpanID, traceID, spanID)
			}
			if got.Name != "/deposit" {
				t.Fatalf("name %q, want /deposit", got.Name)
			}
			for _, s := range got.Statements {
				if len(s) > 0 && containsAny(s, "traceparent", "/*") {
					t.Fatalf("comments must not be recorded in the statement text: %q", s)
				}
			}
		})
	}
}

func containsAny(s string, subs ...string) bool {
	for _, x := range subs {
		for i := 0; i+len(x) <= len(s); i++ {
			if s[i:i+len(x)] == x {
				return true
			}
		}
	}
	return false
}

// TestBatchWithAnErrorInTheMiddle: a pipelined batch where one statement
// fails. PostgreSQL skips the rest of the batch; the proxy must keep its
// place in the protocol and record nothing from the failed batch.
func TestBatchWithAnErrorInTheMiddle(t *testing.T) {
	tb := tableName("px_batch")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			h.exec(fmt.Sprintf("DELETE FROM %s; INSERT INTO %s SELECT g, 0 FROM generate_series(1, 10) g", tb, tb))
			pool := h.pool(mode)
			ctx := context.Background()

			b := &pgx.Batch{}
			b.Queue(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb), 1)
			b.Queue("SELECT 1/0")
			b.Queue(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb), 2)
			err := pool.SendBatch(ctx, b).Close()
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "22012" {
				t.Fatalf("want division by zero, got %v", err)
			}
			if txns, _ := h.reported(); len(txns) != 0 {
				t.Fatalf("a failed batch is one rolled back transaction, got %d reported", len(txns))
			}

			// The same connections keep working, and a good batch is
			// recorded.
			b = &pgx.Batch{}
			for id := 1; id <= 10; id++ {
				b.Queue(fmt.Sprintf("SELECT n FROM %s WHERE id = $1", tb), id)
				b.Queue(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb), id)
			}
			br := pool.SendBatch(ctx, b)
			for id := 1; id <= 10; id++ {
				var n int
				if err := br.QueryRow().Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("row %d: n = %d, want 0", id, n)
				}
				if _, err := br.Exec(); err != nil {
					t.Fatal(err)
				}
			}
			if err := br.Close(); err != nil {
				t.Fatal(err)
			}
			txns, anomalies := h.reported()
			reads, writes := 0, 0
			for _, txn := range txns {
				reads += len(txn.Reads)
				writes += len(txn.Writes)
			}
			want := 10
			if mode == pgx.QueryExecModeSimpleProtocol {
				// pgx sends the batch as one string of many statements,
				// which the proxy forwards without tracking.
				want = 0
			}
			if reads != want || writes != want {
				t.Fatalf("recorded %d reads and %d writes, want %d and %d", reads, writes, want, want)
			}
			if len(anomalies) != 0 {
				t.Fatalf("false alarm: %v", kinds(anomalies))
			}
		})
	}
}
