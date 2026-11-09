package capture

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

// requestCtx is what an instrumented HTTP handler would get: a context with
// its own trace.
func requestCtx() context.Context {
	var tid trace.TraceID
	var sid trace.SpanID
	_, _ = crand.Read(tid[:])
	_, _ = crand.Read(sid[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func newCollected(t *testing.T, opts checker.Options) *collected {
	t.Helper()
	col := &collected{}
	opts.Window = 10 * time.Minute
	col.sink = NewCheckerSink(opts, func(a checker.Anomaly) {
		col.mu.Lock()
		col.anomalies = append(col.anomalies, a)
		col.mu.Unlock()
	})
	t.Cleanup(col.sink.Close)
	return col
}

// noLeakedConns fails if a wrapped call kept a pool connection.
func noLeakedConns(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if n := pool.Stat().AcquiredConns(); n != 0 {
		t.Fatalf("%d pool connections were not given back", n)
	}
}

// The most common lost update in web apps: read with one statement, write
// with another, both straight on the pool. Each statement is its own
// transaction, so per transaction there is nothing wrong. Per request
// (checker.ByTrace) it is a lost update, and IsoWatch must count exactly
// the deposits the database lost.
func TestPoolReadThenWriteLostUpdate(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", acct)); err != nil {
		t.Fatal(err)
	}
	byTrace := newCollected(t, checker.Options{GroupBy: checker.ByTrace})
	byTxn := newCollected(t, checker.Options{})
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: MultiSink{byTrace.sink, byTxn.sink}, KeyMode: RawKeys})
	if err != nil {
		t.Fatal(err)
	}
	db := m.Wrap(pool)

	const workers, each = 16, 40
	var committed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				rctx := WithNameContext(requestCtx(), "deposit")
				var bal int
				if err := db.QueryRow(rctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = $1", acct), 1).Scan(&bal); err != nil {
					t.Error(err)
					return
				}
				if _, err := db.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = $2", acct), bal+1, 1); err != nil {
					t.Error(err)
					return
				}
				committed.Add(1)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	var final int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", acct)).Scan(&final); err != nil {
		t.Fatal(err)
	}
	lostInDB := committed.Load() - final
	lostSeen := int64(byTrace.sink.Checker().LostWrites())
	got := byTrace.all()
	t.Logf("committed=%d final=%d lost(db)=%d lost(isowatch)=%d anomalies=%v", committed.Load(), final, lostInDB, lostSeen, kinds(got))

	if lostInDB == 0 {
		t.Fatal("16 concurrent workers on one row should lose deposits; the test did not race")
	}
	if lostSeen != lostInDB {
		t.Fatalf("IsoWatch counted %d lost writes, the database lost %d", lostSeen, lostInDB)
	}
	if len(got) == 0 {
		t.Fatal("deposits were lost but no anomaly was reported")
	}
	for _, a := range got {
		if a.Kind != checker.LostUpdate || a.Unexpected {
			t.Fatalf("only expected lost updates are possible here, got %s: %s", a.Kind, a.Summary)
		}
		if len(a.TraceIDs()) < 2 || len(a.Names()) != 1 || a.Names()[0] != "deposit" {
			t.Fatalf("anomaly must name the requests: traces %v names %v", a.TraceIDs(), a.Names())
		}
	}
	if n := len(byTxn.all()); n != 0 {
		t.Fatalf("one-statement transactions cannot form a cycle on one row, got %d anomalies", n)
	}
	if c := m.Coverage(); c.Tracked != 2*committed.Load() || c.Untracked != 0 {
		t.Fatalf("every statement should be tracked, coverage %+v", c)
	}
	noLeakedConns(t, pool)
}

// The fixed version, one relative update per request, loses nothing and
// IsoWatch stays quiet, while still tracking every write.
func TestPoolRelativeUpdateStaysQuiet(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", acct)); err != nil {
		t.Fatal(err)
	}
	byTrace := newCollected(t, checker.Options{GroupBy: checker.ByTrace})
	var writes atomic.Int64
	count := SinkFunc(func(_ context.Context, x *history.Txn) { writes.Add(int64(len(x.Writes))) })
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: MultiSink{byTrace.sink, count}})
	if err != nil {
		t.Fatal(err)
	}
	db := m.Wrap(pool)

	const workers, each = 16, 40
	var committed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				rctx := WithNameContext(requestCtx(), "deposit")
				if _, err := db.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = balance + $1 WHERE id = $2", acct), 1, 1); err != nil {
					t.Error(err)
					return
				}
				committed.Add(1)
			}
		}()
	}
	wg.Wait()
	var final int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", acct)).Scan(&final); err != nil {
		t.Fatal(err)
	}
	if final != committed.Load() {
		t.Fatalf("relative updates must not lose anything: committed %d, balance %d", committed.Load(), final)
	}
	if got := byTrace.all(); len(got) != 0 {
		t.Fatalf("no anomaly expected, got %d: %s", len(got), got[0].Summary)
	}
	if lost := byTrace.sink.Checker().LostWrites(); lost != 0 {
		t.Fatalf("LostWrites = %d, want 0", lost)
	}
	if writes.Load() != committed.Load() {
		t.Fatalf("every write must reach the sink: %d of %d", writes.Load(), committed.Load())
	}
	noLeakedConns(t, pool)
}

// result is everything a caller can see from one statement.
type result struct {
	fields []string
	rows   []string
	tag    string
	err    string
}

func readAll(rs pgx.Rows, err error) result {
	if err != nil {
		return result{err: err.Error()}
	}
	defer rs.Close()
	var out result
	for _, fd := range rs.FieldDescriptions() {
		out.fields = append(out.fields, fmt.Sprintf("%s/%d/%d/%d/%d", fd.Name, fd.DataTypeOID, fd.Format, fd.TableOID, fd.TableAttributeNumber))
	}
	for rs.Next() {
		raw := fmt.Sprintf("%q", rs.RawValues())
		v, err := rs.Values()
		if err != nil {
			out.err = err.Error()
			break
		}
		out.rows = append(out.rows, fmt.Sprintf("%v %s", v, raw))
	}
	rs.Close()
	out.tag = rs.CommandTag().String()
	if rs.Err() != nil && out.err == "" {
		out.err = rs.Err().Error()
	}
	return out
}

// The wrapped pool must return exactly what the raw pool returns, for
// tracked and untracked statements alike.
func TestPoolResultsMatchRawPool(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, owner text, balance int")
	other := table(t, pool, "id int PRIMARY KEY, note text")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'ann', 100), (2, 'bob', 50), (3, 'ann', 7)", acct)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'x')", other)); err != nil {
		t.Fatal(err)
	}
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: SinkFunc(func(context.Context, *history.Txn) {})})
	if err != nil {
		t.Fatal(err)
	}
	db := m.Wrap(pool)

	q := func(s string) string { return fmt.Sprintf(s, acct) }
	cases := []struct {
		name    string
		sql     string
		args    []any
		tracked bool
	}{
		{"select star", q("SELECT * FROM %s ORDER BY id"), nil, true},
		{"select with arg", q("SELECT owner, balance FROM %s WHERE id = $1"), []any{1}, true},
		{"alias", q("SELECT a.id, a.owner FROM %s a WHERE a.balance > $1 ORDER BY a.id"), []any{10}, true},
		{"no rows", q("SELECT owner FROM %s WHERE id = $1"), []any{99}, true},
		{"aggregate", q("SELECT count(*), sum(balance) FROM %s WHERE owner = $1"), []any{"ann"}, true},
		{"named args", q("SELECT * FROM %s WHERE id = @id"), []any{pgx.NamedArgs{"id": 2}}, true},
		{"simple protocol", q("SELECT owner FROM %s WHERE id = $1"), []any{pgx.QueryExecModeSimpleProtocol, 1}, true},
		{"for update", q("SELECT id FROM %s WHERE id = 1 FOR UPDATE"), nil, true},
		{"update returning", q("UPDATE %s SET owner = owner WHERE id = 1 RETURNING *"), nil, true},
		{"update no returning", q("UPDATE %s SET owner = owner WHERE id = 2"), nil, true},
		{"delete nothing", q("DELETE FROM %s WHERE id = 999"), nil, true},
		{"insert nothing", q("INSERT INTO %s VALUES (1, 'z', 0) ON CONFLICT (id) DO NOTHING RETURNING id"), nil, true},
		{"group by", q("SELECT owner, sum(balance) FROM %s GROUP BY owner ORDER BY owner"), nil, false},
		{"with", q("WITH x AS (SELECT * FROM %s) SELECT id FROM x ORDER BY id"), nil, false},
		{"subquery", q("SELECT * FROM (SELECT id FROM %s) s ORDER BY id"), nil, false},
		{"result formats", q("SELECT id, owner FROM %s WHERE id = 1"), []any{pgx.QueryResultFormats{pgx.TextFormatCode, pgx.TextFormatCode}}, false},
		{"other table", fmt.Sprintf("SELECT * FROM %s", other), nil, false},
		{"no table", "SELECT 1 AS one, 'a'::text AS two", nil, false},
		{"error", q("SELECT nope FROM %s"), nil, false},
		{"constraint error", q("INSERT INTO %s VALUES (1, 'dup', 0)"), nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := m.Stats().Tracked
			want := readAll(pool.Query(ctx, c.sql, c.args...))
			got := readAll(db.Query(ctx, c.sql, c.args...))
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("Query differs\n raw  %+v\n wrap %+v", want, got)
			}
			wantTag, wantErr := pool.Exec(ctx, c.sql, c.args...)
			gotTag, gotErr := db.Exec(ctx, c.sql, c.args...)
			if wantTag.String() != gotTag.String() || fmt.Sprint(wantErr) != fmt.Sprint(gotErr) {
				t.Fatalf("Exec differs: raw %q %v, wrap %q %v", wantTag, wantErr, gotTag, gotErr)
			}
			if tracked := m.Stats().Tracked > before; tracked != c.tracked && c.name != "error" {
				t.Fatalf("tracked = %v, want %v", tracked, c.tracked)
			}
		})
	}

	// QueryRow and the pgx helpers.
	var owner string
	var bal int
	if err := db.QueryRow(ctx, q("SELECT owner, balance FROM %s WHERE id = $1"), 2).Scan(&owner, &bal); err != nil || owner != "bob" || bal != 50 {
		t.Fatalf("QueryRow: %v %q %d", err, owner, bal)
	}
	if err := db.QueryRow(ctx, q("SELECT owner FROM %s WHERE id = $1"), 99).Scan(&owner); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("want ErrNoRows, got %v", err)
	}
	type account struct {
		ID      int
		Owner   string
		Balance int
	}
	rs, err := db.Query(ctx, q("SELECT * FROM %s ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	accs, err := pgx.CollectRows(rs, pgx.RowToStructByName[account])
	if err != nil || len(accs) != 3 || accs[2].Owner != "ann" {
		t.Fatalf("CollectRows: %v %+v", err, accs)
	}
	// Closing early still gives the connection back.
	rs, err = db.Query(ctx, q("SELECT id FROM %s ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	rs.Next()
	rs.Close()
	// A failing Query returns rows that are safe to close, like pgxpool.
	rs, err = db.Query(ctx, "SELECT FROM nowhere_at_all")
	if err == nil {
		t.Fatal("expected an error")
	}
	rs.Close()
	noLeakedConns(t, pool)
}

// Each statement on the pool reaches the sink as its own transaction, with
// the trace, the name and the exact row versions.
func TestPoolStatementsReachTheSink(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int CHECK (balance >= 0), code int UNIQUE DEFERRABLE INITIALLY DEFERRED")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3)", acct)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var txns []*history.Txn
	sink := SinkFunc(func(_ context.Context, x *history.Txn) {
		mu.Lock()
		txns = append(txns, x)
		mu.Unlock()
	})
	take := func() []*history.Txn {
		mu.Lock()
		defer mu.Unlock()
		out := txns
		txns = nil
		return out
	}
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: sink, KeyMode: RawKeys})
	if err != nil {
		t.Fatal(err)
	}
	db := m.Wrap(pool)
	rctx := WithNameContext(requestCtx(), "pay")
	traceID := trace.SpanContextFromContext(rctx).TraceID().String()
	xmin := func() uint32 {
		var s string
		if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT xmin::text FROM %s WHERE id = 1", acct)).Scan(&s); err != nil {
			t.Fatal(err)
		}
		x, _ := strconv.ParseUint(s, 10, 32)
		return uint32(x)
	}

	// A write: its new version's xmin is the statement's own transaction id.
	if _, err := db.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = 11 WHERE id = 1", acct)); err != nil {
		t.Fatal(err)
	}
	got := take()
	if len(got) != 1 || len(got[0].Writes) != 1 {
		t.Fatalf("want one transaction with one write, got %+v", got)
	}
	w := got[0]
	if w.Name != "pay" || w.TraceID != traceID || w.SpanID == "" || w.Isolation != "read committed" {
		t.Fatalf("name, trace or isolation missing: %+v", w)
	}
	if x := xmin(); w.Writes[0].NewXmin != x || len(w.Xids) != 1 || w.Xids[0] != x {
		t.Fatalf("write must record its own xid %d, got write %+v xids %v", x, w.Writes[0], w.Xids)
	}
	if w.Commit.Before(w.Start) || len(w.Statements) != 1 || w.Statements[0] == "" {
		t.Fatalf("bad times or statements: %+v", w)
	}

	// A read sees that version.
	var bal int
	if err := db.QueryRow(rctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = $1", acct), 1).Scan(&bal); err != nil || bal != 11 {
		t.Fatalf("read: %v %d", err, bal)
	}
	got = take()
	if len(got) != 1 || len(got[0].Reads) != 1 || got[0].Reads[0].Xmin != xmin() || got[0].Reads[0].Key != "1" || got[0].TraceID != traceID {
		t.Fatalf("want one read of the new version, got %+v", got)
	}

	// A failed statement rolled back, so nothing is reported.
	if _, err := db.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = -1 WHERE id = 1", acct)); err == nil {
		t.Fatal("the CHECK constraint should fail")
	}
	if got := take(); len(got) != 0 {
		t.Fatalf("a failed statement must not be reported, got %+v", got)
	}
	// Harder: the deferred UNIQUE check runs at the implicit commit, after
	// PostgreSQL already sent the RETURNING rows. The commit failed, so the
	// write never happened and must not be reported.
	if _, err := db.Exec(rctx, fmt.Sprintf("UPDATE %s SET code = 1 WHERE id = 2", acct)); err == nil {
		t.Fatal("the deferred UNIQUE constraint should fail at commit")
	}
	if got := take(); len(got) != 0 {
		t.Fatalf("a statement whose commit failed must not be reported, got %+v", got)
	}

	// Rows closed early: what was read is reported.
	rs, err := db.Query(rctx, fmt.Sprintf("SELECT id FROM %s ORDER BY id", acct))
	if err != nil {
		t.Fatal(err)
	}
	rs.Next()
	rs.Close()
	if got := take(); len(got) != 1 || len(got[0].Reads) != 1 {
		t.Fatalf("want the one row that was read, got %+v", got)
	}

	// Untracked statements are not reported.
	if _, err := db.Exec(rctx, fmt.Sprintf("SELECT balance, count(*) FROM %s GROUP BY balance", acct)); err != nil {
		t.Fatal(err)
	}
	if got := take(); len(got) != 0 {
		t.Fatalf("untracked statements are not reported, got %+v", got)
	}

	// Begin, BeginTx and Do on the wrapped pool are tracked transactions
	// that take their name from ctx unless WithName says otherwise.
	tx, err := db.Begin(rctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = balance + 1 WHERE id = 2", acct)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(rctx); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(rctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, WithName("explicit"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = balance + 1 WHERE id = 3", acct)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(rctx); err != nil {
		t.Fatal(err)
	}
	got = take()
	if len(got) != 2 || got[0].Name != "pay" || got[1].Name != "explicit" || got[1].Isolation != "repeatable read" {
		t.Fatalf("Begin/BeginTx names: %+v", got)
	}
	m.Protect("pay")
	err = db.Do(rctx, pgx.TxOptions{}, func(tx *Tx) error {
		_, err := tx.Exec(rctx, fmt.Sprintf("UPDATE %s SET balance = balance + 1 WHERE id = 2", acct))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := take(); len(got) != 1 || got[0].Name != "pay" || got[0].Isolation != "serializable" {
		t.Fatalf("Do must use the name from ctx, also for protection: %+v", got)
	}
	noLeakedConns(t, pool)
}

// The wrapped pool also satisfies the interface that sqlc generates, so
// generated code can use it directly.
var _ interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
} = (*DB)(nil)

// A write without RETURNING returns no rows from Query, with or without
// IsoWatch, even though IsoWatch adds hidden RETURNING columns to it.
func TestTxQueryOnWriteWithoutReturning(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, balance int")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0), (2, 0)", acct)); err != nil {
		t.Fatal(err)
	}
	var got []*history.Txn
	m, err := New(ctx, pool, Config{Tables: []string{acct}, Sink: SinkFunc(func(_ context.Context, x *history.Txn) { got = append(got, x) })})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := m.Begin(ctx, pool, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	rs, err := tx.Query(ctx, fmt.Sprintf("UPDATE %s SET balance = 1", acct))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rs.Next() {
		n++
	}
	if rs.Err() != nil || n != 0 || rs.CommandTag().String() != "UPDATE 2" {
		t.Fatalf("want no rows and tag UPDATE 2, got %d rows, tag %q, err %v", n, rs.CommandTag(), rs.Err())
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Writes) != 2 {
		t.Fatalf("both writes must still be recorded, got %+v", got)
	}
}
