package isowatchtest_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/isowatchtest"
)

// These tests need a real PostgreSQL 18 server. Example:
//
//	docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:18
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./isowatchtest/

// raceWindow stands in for the work real code does between a read and a
// write. It makes the races in these tests happen every time.
const raceWindow = 10 * time.Millisecond

const bankSQL = `
CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL);
CREATE TABLE doctors (id text PRIMARY KEY, on_call bool NOT NULL);
INSERT INTO accounts VALUES (1, 0);
INSERT INTO doctors VALUES ('alice', true), ('bob', true);
`

// testPool returns a pool whose search_path is a fresh schema with the bank
// tables in it, so tests never see each other's tables.
func testPool(t *testing.T, extraSQL ...string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ISOWATCH_TEST_DSN")
	if dsn == "" {
		t.Skip("ISOWATCH_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("iwtest_%d", rand.Int64())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 32
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, s := range append([]string{bankSQL}, extraSQL...) {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

// deposit adds 1 to account 1. With lock == "" it has the classic bug: read
// the balance, compute the new one in Go, write it back. With
// lock == "FOR UPDATE" the read locks the row and the bug is gone.
func deposit(ctx context.Context, m *isowatchtest.Monitor, pool *pgxpool.Pool, lock string) func(int) error {
	return func(int) error {
		read := "SELECT balance FROM accounts WHERE id = $1"
		if lock != "" {
			read += " " + lock
		}
		return m.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
			var balance int
			if err := tx.QueryRow(ctx, read, 1).Scan(&balance); err != nil {
				return err
			}
			time.Sleep(raceWindow)
			_, err := tx.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", balance+1, 1)
			return err
		}, capture.WithName("deposit"))
	}
}

func balance(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var b int
	if err := pool.QueryRow(context.Background(), "SELECT balance FROM accounts WHERE id = 1").Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func mustContain(t *testing.T, msg string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(msg, w) {
			t.Errorf("message does not contain %q. Message:\n%s", w, msg)
		}
	}
}

// The main promise: buggy code that loses money fails its test, with a
// report that says what happened and how to fix it.
func TestBuggyDepositFailsTheTest(t *testing.T) {
	pool := testPool(t)
	const n = 16
	ft := runFake(t, func(ft *fakeT) {
		m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
		isowatchtest.Concurrently(ft, n, deposit(context.Background(), m, pool, ""))
	})
	lost := n - balance(t, pool)
	if lost == 0 {
		t.Fatal("the race did not happen: no deposit was lost, so this test proves nothing")
	}
	if !ft.Failed() {
		t.Fatalf("the database lost %d of %d deposits, but the test passed", lost, n)
	}
	if len(ft.errors) != 1 || len(ft.fatals) != 0 {
		t.Fatalf("want the whole report as one error, got %d errors and %d fatals:\n%s", len(ft.errors), len(ft.fatals), ft.all())
	}
	msg := ft.errors[0]
	t.Logf("failure message:\n%s", msg)
	mustContain(t, msg,
		"lost-update",
		"that this test did not expect",
		`between transactions named "deposit"`,
		"Lost update on iwtest_",
		"accounts(1)",
		"SELECT balance FROM accounts WHERE id = $1",
		"UPDATE accounts SET balance = $1 WHERE id = $2",
		"Fix: Lock the rows when you read them: add FOR UPDATE",
		fmt.Sprintf("IsoWatch counted %d lost write(s)", lost),
		"Next: apply the fix above",
		"m.ExpectAnomaly(isowatchtest.LostUpdate)",
		`isowatchtest.IgnoreNames("deposit")`,
	)
	// 16 calls racing on one row give many anomalies of one bug. They are
	// shown as one group with a count, not 15 copies.
	if lost > 1 {
		mustContain(t, msg, "more like this one")
	}
	if strings.Contains(msg, "\n2) ") {
		t.Errorf("one bug should be one group in the report:\n%s", msg)
	}
}

// The fixed code passes, and IsoWatch stays quiet about it.
func TestSafeDepositPasses(t *testing.T) {
	pool := testPool(t)
	const n = 16
	var m *isowatchtest.Monitor
	ft := runFake(t, func(ft *fakeT) {
		m = isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
		isowatchtest.Concurrently(ft, n, deposit(context.Background(), m, pool, "FOR UPDATE"))
		m.ExpectNoAnomalies()
	})
	if got := balance(t, pool); got != n {
		t.Fatalf("FOR UPDATE must keep every deposit: balance %d, want %d", got, n)
	}
	if ft.Failed() {
		t.Fatalf("the safe deposit failed its test:\n%s", ft.all())
	}
	if s := m.Stats(); s.Tracked < 2*n || s.Untracked != 0 {
		t.Fatalf("every statement should be tracked, stats %+v", s)
	}
}

func TestExpectAnomaly(t *testing.T) {
	t.Run("seen", func(t *testing.T) {
		pool := testPool(t)
		ft := runFake(t, func(ft *fakeT) {
			m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
			m.ExpectAnomaly(isowatchtest.LostUpdate)
			isowatchtest.Concurrently(ft, 8, deposit(context.Background(), m, pool, ""))
			if len(m.Anomalies()) == 0 || m.LostWrites() == 0 {
				ft.Errorf("Anomalies() and LostWrites() should show the race: %d anomalies, %d lost writes", len(m.Anomalies()), m.LostWrites())
			}
		})
		if ft.Failed() {
			t.Fatalf("the anomaly was expected, but the test failed:\n%s", ft.all())
		}
		mustContain(t, strings.Join(ft.logs, "\n"), "as the test expected", "lost-update")
	})

	t.Run("not seen", func(t *testing.T) {
		pool := testPool(t)
		ft := runFake(t, func(ft *fakeT) {
			m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
			m.ExpectAnomaly(isowatchtest.LostUpdate)
			isowatchtest.Concurrently(ft, 8, deposit(context.Background(), m, pool, "FOR UPDATE"))
		})
		if !ft.Failed() {
			t.Fatal("the test expected a lost update that never happened, but it passed")
		}
		mustContain(t, ft.all(),
			"the test expected a lost-update anomaly, but IsoWatch saw none",
			"IsoWatch checked 8 committed transaction(s)",
			"isowatchtest.Repeat")
	})

	t.Run("other kinds still fail", func(t *testing.T) {
		pool := testPool(t)
		ft := runFake(t, func(ft *fakeT) {
			m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
			m.ExpectAnomaly(isowatchtest.WriteSkew)
			isowatchtest.Concurrently(ft, 8, deposit(context.Background(), m, pool, ""))
		})
		msg := ft.all()
		mustContain(t, msg,
			"lost-update",
			"that this test did not expect",
			"the test expected a write-skew anomaly, but IsoWatch saw none")
	})

	t.Run("unknown kind", func(t *testing.T) {
		pool := testPool(t)
		ft := runFake(t, func(ft *fakeT) {
			m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
			m.ExpectAnomaly("lost_update")
		})
		mustContain(t, ft.all(), `ExpectAnomaly("lost_update"): unknown kind`)
	})
}

// The doctors on call: each one checks that someone else is on call, then
// leaves. Both leave, and nobody is on call.
func TestWriteSkewIsCaught(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ft := runFake(t, func(ft *fakeT) {
		m := isowatchtest.New(ft, pool, isowatchtest.Tables("doctors"))
		isowatchtest.Concurrently(ft, 2, func(i int) error {
			doctor := []string{"alice", "bob"}[i]
			return m.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
				var onCall int
				if err := tx.QueryRow(ctx, "SELECT count(*) FROM doctors WHERE on_call").Scan(&onCall); err != nil {
					return err
				}
				if onCall < 2 {
					return nil // the other doctor already left, so stay
				}
				time.Sleep(raceWindow)
				_, err := tx.Exec(ctx, "UPDATE doctors SET on_call = false WHERE id = $1", doctor)
				return err
			}, capture.WithName("leave"))
		})
	})
	var onCall int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM doctors WHERE on_call").Scan(&onCall); err != nil {
		t.Fatal(err)
	}
	if onCall != 0 {
		t.Fatalf("the race did not happen: %d doctor(s) still on call, so this test proves nothing", onCall)
	}
	if !ft.Failed() {
		t.Fatal("nobody is on call, but the test passed")
	}
	msg := ft.all()
	t.Logf("failure message:\n%s", msg)
	mustContain(t, msg,
		"write-skew",
		`between transactions named "leave"`,
		"SELECT count(*) FROM doctors WHERE on_call",
		"UPDATE doctors SET on_call = $2 WHERE id = $1", // literals are normalized
		"m.ExpectAnomaly(isowatchtest.WriteSkew)")
}

// ExpectNoAnomalies must not pass when IsoWatch saw nothing at all, for
// example because the code under test started its transactions on the pool.
func TestExpectNoAnomaliesNeedsTransactions(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ft := runFake(t, func(ft *fakeT) {
		m := isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"))
		tx, err := pool.Begin(ctx) // not m.Begin: IsoWatch cannot see this
		if err != nil {
			ft.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + 1 WHERE id = 1"); err != nil {
			ft.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			ft.Fatal(err)
		}
		m.ExpectNoAnomalies()
	})
	if !ft.Failed() {
		t.Fatal("ExpectNoAnomalies passed although IsoWatch checked no transaction")
	}
	mustContain(t, ft.all(), "has not checked any transaction", "m.Do or m.Begin")
}

// Calls that share one trace (for example a test wrapped in one span) must
// still be checked against each other. With trace grouping as the default,
// they would be one node and the race would be hidden.
func TestDefaultSeesRacesInsideOneTrace(t *testing.T) {
	pool := testPool(t)
	m := isowatchtest.New(t, pool, isowatchtest.Tables("accounts"))
	m.ExpectAnomaly(isowatchtest.LostUpdate)
	ctx := isowatchtest.NewTrace(context.Background())
	isowatchtest.Concurrently(t, 8, deposit(ctx, m, pool, ""))
	if as := m.Anomalies(); len(as) > 0 && as[0].Steps[0].Trace == "" {
		t.Errorf("the steps should carry the trace id from NewTrace: %+v", as[0].Steps[0])
	}
}

// A request that reads in one transaction and writes in another loses money
// too. Each transaction alone is fine, so only GroupByTrace finds it.
func TestGroupByTraceFindsRacesAcrossTransactions(t *testing.T) {
	request := func(m *isowatchtest.Monitor, pool *pgxpool.Pool) func(int) error {
		return func(int) error {
			ctx := isowatchtest.NewTrace(context.Background())
			var b int
			err := m.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
				return tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1", 1).Scan(&b)
			}, capture.WithName("read-balance"))
			if err != nil {
				return err
			}
			time.Sleep(raceWindow)
			return m.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
				_, err := tx.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", b+1, 1)
				return err
			}, capture.WithName("write-balance"))
		}
	}
	const n = 8

	t.Run("GroupByTrace", func(t *testing.T) {
		pool := testPool(t)
		m := isowatchtest.New(t, pool, isowatchtest.Tables("accounts"), isowatchtest.GroupByTrace())
		m.ExpectAnomaly(isowatchtest.LostUpdate)
		isowatchtest.Concurrently(t, n, request(m, pool))
		if balance(t, pool) == n {
			t.Fatal("the race did not happen: nothing was lost, so this test proves nothing")
		}
	})

	t.Run("default", func(t *testing.T) {
		pool := testPool(t)
		m := isowatchtest.New(t, pool, isowatchtest.Tables("accounts"))
		isowatchtest.Concurrently(t, n, request(m, pool))
		if balance(t, pool) == n {
			t.Fatal("the race did not happen: nothing was lost, so this test proves nothing")
		}
		if as := m.Anomalies(); len(as) != 0 {
			t.Fatalf("each transaction alone is serializable, so the default reports nothing; got %d anomalies", len(as))
		}
	})
}

// Without Tables, New watches every table with a primary key it can find,
// and skips the ones capture cannot watch.
func TestFindsTablesOnItsOwn(t *testing.T) {
	pool := testPool(t, `
CREATE TABLE audit_log (msg text);
CREATE VIEW rich AS SELECT * FROM accounts WHERE balance > 100;
`)
	m := isowatchtest.New(t, pool)
	m.ExpectAnomaly(isowatchtest.LostUpdate)
	isowatchtest.Concurrently(t, 8, deposit(context.Background(), m, pool, ""))
}

func TestIgnore(t *testing.T) {
	for name, opt := range map[string]isowatchtest.Option{
		"IgnoreNames":  isowatchtest.IgnoreNames("deposit"),
		"IgnoreTables": isowatchtest.IgnoreTables("accounts"),
	} {
		t.Run(name, func(t *testing.T) {
			pool := testPool(t)
			var m *isowatchtest.Monitor
			ft := runFake(t, func(ft *fakeT) {
				m = isowatchtest.New(ft, pool, isowatchtest.Tables("accounts"), opt)
				isowatchtest.Concurrently(ft, 8, deposit(context.Background(), m, pool, ""))
			})
			if balance(t, pool) == 8 {
				t.Fatal("the race did not happen, so this test proves nothing")
			}
			if ft.Failed() {
				t.Fatalf("the race was ignored, but the test failed:\n%s", ft.all())
			}
			if len(m.Anomalies()) != 0 {
				t.Fatalf("ignored anomalies should not be listed, got %d", len(m.Anomalies()))
			}
		})
	}
}
