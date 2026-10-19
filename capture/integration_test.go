package capture

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

// These tests need a real PostgreSQL 18 server. Example:
//
//	docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:18
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./capture/
func testDSN() string { return os.Getenv("ISOWATCH_TEST_DSN") }

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := testDSN()
	if dsn == "" {
		t.Skip("ISOWATCH_TEST_DSN not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// table creates a fresh table with a unique name and returns that name.
func table(t *testing.T, pool *pgxpool.Pool, columns string) string {
	t.Helper()
	name := fmt.Sprintf("iw_%d", rand.Int63())
	ctx := context.Background()
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", name, columns)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
	return name
}

type collected struct {
	mu        sync.Mutex
	anomalies []checker.Anomaly
	sink      *CheckerSink
}

func (c *collected) all() []checker.Anomaly {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]checker.Anomaly(nil), c.anomalies...)
}

func monitor(t *testing.T, pool *pgxpool.Pool, tables ...string) (*Monitor, *collected) {
	t.Helper()
	col := &collected{}
	col.sink = NewCheckerSink(checker.Options{Window: 10 * time.Minute}, func(a checker.Anomaly) {
		col.mu.Lock()
		col.anomalies = append(col.anomalies, a)
		col.mu.Unlock()
	})
	t.Cleanup(col.sink.Close)
	m, err := New(context.Background(), pool, Config{Tables: tables, Sink: col.sink, KeyMode: RawKeys})
	if err != nil {
		t.Fatal(err)
	}
	return m, col
}

func kinds(as []checker.Anomaly) map[checker.Kind]int {
	out := map[checker.Kind]int{}
	for _, a := range as {
		out[a.Kind]++
	}
	return out
}

func TestTransparentResults(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, owner text, balance int")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1,'ann',100),(2,'bob',50)", acct)); err != nil {
		t.Fatal(err)
	}
	m, _ := monitor(t, pool, acct)
	tx, err := m.Begin(ctx, pool, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	// QueryRow + Scan
	var owner string
	var bal int
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT owner, balance FROM %s WHERE id = $1", acct), 1).Scan(&owner, &bal); err != nil {
		t.Fatal(err)
	}
	if owner != "ann" || bal != 100 {
		t.Fatalf("got %s %d", owner, bal)
	}
	// ErrNoRows
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT owner FROM %s WHERE id = 99", acct)).Scan(&owner); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("want ErrNoRows, got %v", err)
	}
	// CollectRows into structs by column name sees only the real columns
	type acc struct {
		ID      int
		Owner   string
		Balance int
	}
	rs, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", acct))
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rs, pgx.RowToStructByName[acc])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Owner != "bob" {
		t.Fatalf("got %+v", got)
	}
	// Values and FieldDescriptions hide the extra columns
	rs, err = tx.Query(ctx, fmt.Sprintf("SELECT id, owner FROM %s WHERE id = 1", acct))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(rs.FieldDescriptions()); n != 2 {
		t.Fatalf("want 2 visible columns, got %d", n)
	}
	rs.Next()
	vals, _ := rs.Values()
	rs.Close()
	if len(vals) != 2 {
		t.Fatalf("want 2 values, got %d", len(vals))
	}
	// Exec keeps the real command tag
	tag, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = balance + 1", acct))
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "UPDATE 2" {
		t.Fatalf("got tag %q", tag.String())
	}
	// User RETURNING still works
	var newBal int
	if err := tx.QueryRow(ctx, fmt.Sprintf("UPDATE %s SET balance = 7 WHERE id = 2 RETURNING balance", acct)).Scan(&newBal); err != nil || newBal != 7 {
		t.Fatalf("returning: %v %d", err, newBal)
	}
	// Named arguments
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = @id", acct), pgx.NamedArgs{"id": 2}).Scan(&newBal); err != nil || newBal != 7 {
		t.Fatalf("named args: %v %d", err, newBal)
	}
	// Transaction control SQL is refused
	if _, err := tx.Exec(ctx, "COMMIT"); !errors.Is(err, ErrTxControl) {
		t.Fatalf("want ErrTxControl, got %v", err)
	}
	if s := m.Stats(); s.Tracked < 6 {
		t.Fatalf("expected tracked statements, got %+v", s)
	}
}

// lostUpdate runs the classic interleaving: both read, A writes and commits,
// then B writes a value computed from its old read.
func lostUpdate(t *testing.T, m *Monitor, pool *pgxpool.Pool, acct string, lock string, iso pgx.TxIsoLevel) (errB error) {
	t.Helper()
	ctx := context.Background()
	a, err := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		t.Fatal(err)
	}
	var balA, balB int
	if err := a.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1 %s", acct, lock)).Scan(&balA); err != nil {
		t.Fatal(err)
	}
	if lock != "" {
		// With FOR UPDATE, B's read must wait for A. Run B in the background.
		done := make(chan error, 1)
		go func() {
			if err := b.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1 %s", acct, lock)).Scan(&balB); err != nil {
				done <- err
				return
			}
			_, err := b.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = 1", acct), balB-30)
			if err == nil {
				err = b.Commit(ctx)
			}
			done <- err
		}()
		time.Sleep(100 * time.Millisecond)
		if _, err := a.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = 1", acct), balA+50); err != nil {
			t.Fatal(err)
		}
		if err := a.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return <-done
	}
	if err := b.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", acct)).Scan(&balB); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = 1", acct), balA+50); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = b.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = 1", acct), balB-30)
	if err == nil {
		err = b.Commit(ctx)
	} else {
		_ = b.Rollback(ctx)
	}
	return err
}

func TestDetectsLostUpdate(t *testing.T) {
	pool := testPool(t)
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", acct))
	m, col := monitor(t, pool, acct)

	if err := lostUpdate(t, m, pool, acct, "", pgx.ReadCommitted); err != nil {
		t.Fatal(err)
	}
	var final int
	pool.QueryRow(context.Background(), fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", acct)).Scan(&final)
	if final != 70 {
		t.Fatalf("expected the deposit to be lost (balance 70), got %d", final)
	}
	got := col.all()
	if len(got) != 1 || got[0].Kind != checker.LostUpdate {
		t.Fatalf("want 1 lost update, got %+v", got)
	}
	if got[0].SkippedVersions != 1 || got[0].Unexpected {
		t.Fatalf("bad details: %+v", got[0])
	}
}

func TestNoFalseAlarmWithForUpdate(t *testing.T) {
	pool := testPool(t)
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", acct))
	m, col := monitor(t, pool, acct)
	if err := lostUpdate(t, m, pool, acct, "FOR UPDATE", pgx.ReadCommitted); err != nil {
		t.Fatal(err)
	}
	var final int
	pool.QueryRow(context.Background(), fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", acct)).Scan(&final)
	if final != 120 {
		t.Fatalf("FOR UPDATE should keep both changes (120), got %d", final)
	}
	if got := col.all(); len(got) != 0 {
		t.Fatalf("no anomaly expected, got %+v", got)
	}
}

func TestSerializablePreventsAndStaysQuiet(t *testing.T) {
	pool := testPool(t)
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", acct))
	m, col := monitor(t, pool, acct)
	err := lostUpdate(t, m, pool, acct, "", pgx.Serializable)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "40001" {
		t.Fatalf("PostgreSQL should reject B with 40001, got %v", err)
	}
	if got := col.all(); len(got) != 0 {
		t.Fatalf("no anomaly expected, got %+v", got)
	}
}

func TestDetectsWriteSkew(t *testing.T) {
	for _, iso := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(iso), func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()
			docs := table(t, pool, "id text PRIMARY KEY, on_call bool")
			pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ('alice', true), ('bob', true)", docs))
			m, col := monitor(t, pool, docs)

			leave := func(tx *Tx) int {
				rs, err := tx.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE on_call", docs))
				if err != nil {
					t.Fatal(err)
				}
				ids, _ := pgx.CollectRows(rs, pgx.RowTo[string])
				return len(ids)
			}
			a, _ := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
			b, _ := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
			if leave(a) != 2 || leave(b) != 2 {
				t.Fatal("both should see two doctors on call")
			}
			a.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 'alice'", docs))
			b.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 'bob'", docs))
			errA := a.Commit(ctx)
			errB := b.Commit(ctx)

			got := col.all()
			if iso == pgx.Serializable {
				if errA == nil && errB == nil {
					t.Fatal("serializable should reject one of them")
				}
				if len(got) != 0 {
					t.Fatalf("no anomaly expected at serializable, got %+v", got)
				}
				return
			}
			if errA != nil || errB != nil {
				t.Fatalf("both commit at %s: %v %v", iso, errA, errB)
			}
			if len(got) != 1 || got[0].Kind != checker.WriteSkew || got[0].Unexpected {
				t.Fatalf("want 1 expected write skew, got %+v", got)
			}
		})
	}
}

// The same write skew, written the way most apps write it: count the doctors
// still on call, then go off call if there is someone else.
func TestDetectsWriteSkewThroughCount(t *testing.T) {
	for _, iso := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(iso), func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()
			docs := table(t, pool, "id text PRIMARY KEY, on_call bool")
			pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ('alice', true), ('bob', true), ('carol', false)", docs))
			m, col := monitor(t, pool, docs)

			onCall := func(tx *Tx) int {
				var n int
				if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE on_call", docs)).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			a, _ := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
			b, _ := m.Begin(ctx, pool, pgx.TxOptions{IsoLevel: iso})
			if onCall(a) != 2 || onCall(b) != 2 {
				t.Fatal("both should count two doctors on call")
			}
			a.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 'alice'", docs))
			b.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = false WHERE id = 'bob'", docs))
			errA := a.Commit(ctx)
			errB := b.Commit(ctx)

			got := col.all()
			if iso == pgx.Serializable {
				if errA == nil && errB == nil {
					t.Fatal("serializable should reject one of them")
				}
				if len(got) != 0 {
					t.Fatalf("no anomaly expected at serializable, got %+v", got)
				}
				return
			}
			if errA != nil || errB != nil {
				t.Fatalf("both commit at %s: %v %v", iso, errA, errB)
			}
			if len(got) != 1 || got[0].Kind != checker.WriteSkew || got[0].Unexpected {
				t.Fatalf("want 1 expected write skew, got %+v", got)
			}
		})
	}
}

// Counting rows and then touching only rows nobody else changed is not an
// anomaly, and the aggregate tracking must stay quiet about it.
func TestCountWithoutConflictStaysQuiet(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	docs := table(t, pool, "id text PRIMARY KEY, on_call bool")
	pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ('alice', true), ('bob', true), ('carol', false)", docs))
	m, col := monitor(t, pool, docs)

	for i, who := range []string{"alice", "bob"} {
		tx, _ := m.Begin(ctx, pool, pgx.TxOptions{})
		var n int
		// A parameter in WHERE is used twice: by the count and by the hidden subquery.
		if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE on_call = $1", docs), true).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if want := 2 - i; n != want { // alice went off call first
			tx.Rollback(ctx)
			t.Fatalf("count = %d, want %d", n, want)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET on_call = NOT on_call WHERE id = $1", docs), who); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := col.all(); len(got) != 0 {
		t.Fatalf("one after the other is serializable, got %+v", got)
	}
	if s := m.Stats(); s.Untracked != 0 {
		t.Fatalf("count(*) should be tracked now, stats %+v", s)
	}
}

func TestSavepointRollbackForgetsWrites(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 100), (2, 100)", acct))

	var txns []*history.Txn
	m, err := New(ctx, pool, Config{Tables: []string{acct}, KeyMode: RawKeys, Sink: SinkFunc(func(_ context.Context, x *history.Txn) { txns = append(txns, x) })})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Do(ctx, pool, pgx.TxOptions{}, func(tx *Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = 1 WHERE id = 1", acct)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SAVEPOINT s1"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = 2 WHERE id = 2", acct)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 1 || len(txns[0].Writes) != 1 || txns[0].Writes[0].NewKey != "1" {
		t.Fatalf("only the write to row 1 survived, got %+v", txns)
	}
}

// TestLostWritesMatchTheMoney is the accuracy test. Many workers add 1 to the
// same counter with a read-then-write pattern at read committed. Every lost
// update makes the final value smaller. IsoWatch must count exactly the
// number of writes that were lost.
func TestLostWritesMatchTheMoney(t *testing.T) {
	cases := []struct {
		name string
		lock string
	}{
		{"buggy read then write", ""},
		{"fixed with FOR UPDATE", "FOR UPDATE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()
			ctr := table(t, pool, "id int PRIMARY KEY, n int")
			pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", ctr))
			m, col := monitor(t, pool, ctr)

			const workers, each = 16, 40
			var committed atomic.Int64
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < each; i++ {
						err := m.Do(ctx, pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx *Tx) error {
							var n int
							if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1 %s", ctr, c.lock)).Scan(&n); err != nil {
								return err
							}
							_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = 1", ctr), n+1)
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
			wg.Wait()
			var final int64
			pool.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)).Scan(&final)
			lostInDB := committed.Load() - final
			lostSeen := int64(col.sink.Checker().LostWrites())
			t.Logf("committed=%d final=%d lost(db)=%d lost(isowatch)=%d anomalies=%v",
				committed.Load(), final, lostInDB, lostSeen, kinds(col.all()))

			if lostSeen != lostInDB {
				t.Fatalf("IsoWatch counted %d lost writes, the database lost %d", lostSeen, lostInDB)
			}
			if lostInDB > 0 && len(col.all()) == 0 {
				t.Fatal("writes were lost but no anomaly was reported")
			}
			if lostInDB == 0 && len(col.all()) != 0 {
				t.Fatalf("no writes lost, but anomalies reported: %+v", col.all())
			}
			for _, a := range col.all() {
				if a.Kind != checker.LostUpdate {
					t.Fatalf("only lost updates are possible here, got %s", a.Kind)
				}
			}
			if c.lock != "" && lostInDB != 0 {
				t.Fatalf("FOR UPDATE must not lose writes, lost %d", lostInDB)
			}
		})
	}
}

// TestNoFalsePositivesUnderSerializable runs a random multi-row workload at
// serializable. PostgreSQL guarantees no anomalies, so IsoWatch must report
// none.
func TestNoFalsePositivesUnderSerializable(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	for i := 1; i <= 5; i++ {
		pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, 1000)", acct), i)
	}
	m, col := monitor(t, pool, acct)
	var ok, retried atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 40; i++ {
				from, to := 1+r.Intn(5), 1+r.Intn(5)
				err := m.Do(ctx, pool, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx *Tx) error {
					var a, b int
					if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = $1", acct), from).Scan(&a); err != nil {
						return err
					}
					if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = $1", acct), to).Scan(&b); err != nil {
						return err
					}
					if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = $2", acct), a-1, from); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET balance = balance + 1 WHERE id = $1", acct), to)
					return err
				})
				var pgErr *pgconn.PgError
				switch {
				case err == nil:
					ok.Add(1)
				case errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01"):
					retried.Add(1)
				default:
					t.Error(err)
					return
				}
			}
		}(int64(w))
	}
	wg.Wait()
	t.Logf("committed=%d rejected-by-postgres=%d anomalies=%d", ok.Load(), retried.Load(), len(col.all()))
	if got := col.all(); len(got) != 0 {
		t.Fatalf("serializable must produce no anomalies, got %d: %s", len(got), got[0].Summary)
	}
	if ok.Load() == 0 {
		t.Fatal("nothing committed")
	}
}

// TestProtectionStopsLosses runs the buggy counter twice. In phase one,
// IsoWatch sees lost updates and protects the "incr" transactions. In phase
// two the same buggy code runs at SERIALIZABLE with retries, so nothing is
// lost and nothing is reported.
func TestProtectionStopsLosses(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ctr := table(t, pool, "id int PRIMARY KEY, n int")
	pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", ctr))

	var mon *Monitor
	var mu sync.Mutex
	var anomalies []checker.Anomaly
	sink := NewCheckerSink(checker.Options{Window: 10 * time.Minute}, func(a checker.Anomaly) {
		mu.Lock()
		anomalies = append(anomalies, a)
		mu.Unlock()
		mon.ProtectFrom(a)
	})
	defer sink.Close()
	mon, err := New(ctx, pool, Config{Tables: []string{ctr}, Sink: sink, MaxRetries: 200})
	if err != nil {
		t.Fatal(err)
	}

	incr := func(tx *Tx) error {
		var n int
		if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)).Scan(&n); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = 1", ctr), n+1)
		return err
	}
	run := func() int64 {
		var ok atomic.Int64
		var wg sync.WaitGroup
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 40; i++ {
					if err := mon.Do(ctx, pool, pgx.TxOptions{}, incr, WithName("incr")); err != nil {
						t.Error(err)
						return
					}
					ok.Add(1)
				}
			}()
		}
		wg.Wait()
		return ok.Load()
	}
	value := func() int64 {
		var n int64
		pool.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)).Scan(&n)
		return n
	}

	c1 := run()
	v1 := value()
	mu.Lock()
	a1 := len(anomalies)
	mu.Unlock()
	if a1 == 0 || !mon.IsProtected("incr") {
		t.Fatalf("phase one should find anomalies and protect incr (anomalies=%d protected=%v)", a1, mon.Protected())
	}
	t.Logf("phase 1: committed=%d value=%d lost=%d anomalies=%d", c1, v1, c1-v1, a1)

	c2 := run()
	v2 := value()
	mu.Lock()
	a2 := len(anomalies) - a1
	mu.Unlock()
	t.Logf("phase 2: committed=%d gained=%d lost=%d new anomalies=%d retries=%d", c2, v2-v1, c2-(v2-v1), a2, mon.Stats().Retried)
	if v2-v1 != c2 {
		t.Fatalf("protected phase lost %d increments", c2-(v2-v1))
	}
	if a2 != 0 {
		t.Fatalf("protected phase reported %d anomalies", a2)
	}
	if lost := int64(sink.Checker().LostWrites()); lost != c1-v1 {
		t.Fatalf("IsoWatch counted %d lost writes, database lost %d", lost, c1-v1)
	}
}

func TestNamesReachTheSpan(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, n int")
	pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", acct))
	var got *history.Txn
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: SinkFunc(func(_ context.Context, x *history.Txn) { got = x })})
	if err != nil {
		t.Fatal(err)
	}
	m.Protect("pay")
	err = m.Do(ctx, pool, pgx.TxOptions{}, func(tx *Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = 1 WHERE id = 1", acct))
		return err
	}, WithName("pay"))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "pay" || got.Isolation != "serializable" {
		t.Fatalf("protected name must run serializable, got %+v", got)
	}
}

func TestProtectReportsOnlyNewNames(t *testing.T) {
	m := &Monitor{}
	if got := m.Protect("a", "b", ""); len(got) != 2 {
		t.Fatalf("first time: %v", got)
	}
	if got := m.Protect("a", "c"); len(got) != 1 || got[0] != "c" {
		t.Fatalf("second time: %v", got)
	}
	if p := m.Protected(); len(p) != 3 {
		t.Fatalf("protected = %v", p)
	}
	m.Unprotect("a")
	if m.IsProtected("a") || !m.IsProtected("b") {
		t.Fatal("unprotect failed")
	}
}
