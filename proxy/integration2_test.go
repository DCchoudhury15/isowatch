package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type result struct {
	fields []pgconn.FieldDescription
	rows   [][][]byte
	tag    string
	err    string
}

func run(ctx context.Context, tx pgx.Tx, sql string, args ...any) result {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return result{err: err.Error()}
	}
	var r result
	r.fields = append(r.fields, rows.FieldDescriptions()...)
	for rows.Next() {
		var row [][]byte
		for _, v := range rows.RawValues() {
			if v == nil {
				row = append(row, nil)
			} else {
				row = append(row, bytes.Clone(v))
			}
		}
		r.rows = append(r.rows, row)
	}
	if err := rows.Err(); err != nil {
		r.err = err.Error()
	}
	r.tag = rows.CommandTag().String()
	return r
}

// TestResultsAreIdentical runs the same statements directly and through the
// proxy, in every mode, and compares every column description and every
// byte of every value. Tracked and untracked statements, binary and text
// formats, NULLs, many rows, and writes with and without RETURNING.
func TestResultsAreIdentical(t *testing.T) {
	tb, other := tableName("px_types"), tableName("px_other")
	h := newHarness(t, map[string]string{
		tb: `id int8 PRIMARY KEY, txt text, num numeric, raw bytea, ts timestamptz, doc jsonb,
		     arr int4[], flag bool, u uuid, f float8, d date`,
		other: "id int PRIMARY KEY, x text",
	})
	h.exec(fmt.Sprintf(`INSERT INTO %s
		SELECT g, CASE WHEN g %% 7 = 0 THEN NULL ELSE 'row ' || g END,
		       CASE WHEN g %% 5 = 0 THEN NULL ELSE g * 1.25 END,
		       CASE WHEN g %% 3 = 0 THEN NULL ELSE decode(md5(g::text), 'hex') END,
		       CASE WHEN g %% 11 = 0 THEN NULL ELSE '2026-01-01T00:00:00Z'::timestamptz + g * interval '1 minute' END,
		       CASE WHEN g %% 13 = 0 THEN NULL ELSE jsonb_build_object('g', g, 's', 'x') END,
		       CASE WHEN g %% 17 = 0 THEN NULL ELSE ARRAY[g::int, g::int + 1] END,
		       CASE WHEN g %% 2 = 0 THEN NULL ELSE g %% 4 = 1 END,
		       CASE WHEN g %% 19 = 0 THEN NULL ELSE md5(g::text)::uuid END,
		       CASE WHEN g %% 23 = 0 THEN NULL ELSE g / 3.0 END,
		       CASE WHEN g %% 29 = 0 THEN NULL ELSE date '2026-01-01' + g::int END
		FROM generate_series(1, 5000) g`, tb))
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'x' || g FROM generate_series(1, 100) g", other))
	unwatched := tableName("px_unwatched")
	h.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text); INSERT INTO %s VALUES (1, 'a'), (2, NULL)", unwatched, unwatched))
	t.Cleanup(func() { _, _ = h.direct.Exec(context.Background(), "DROP TABLE IF EXISTS "+unwatched) })

	type q struct {
		sql  string
		args []any
	}
	queries := []q{
		{sql: fmt.Sprintf("SELECT * FROM %s ORDER BY id", tb)},
		{sql: fmt.Sprintf("SELECT id, txt, raw FROM %s WHERE id = $1", tb), args: []any{int64(42)}},
		{sql: fmt.Sprintf("SELECT * FROM %s WHERE id > $1 ORDER BY id LIMIT 10", tb), args: []any{int64(4990)}},
		{sql: fmt.Sprintf("SELECT * FROM %s WHERE false", tb)},
		{sql: fmt.Sprintf("SELECT count(*), sum(num), max(ts) FROM %s WHERE flag", tb)},
		{sql: fmt.Sprintf("SELECT a.id, a.txt, b.x FROM %s a JOIN %s b ON b.id = a.id ORDER BY a.id", tb, other)},
		{sql: fmt.Sprintf("WITH w AS (SELECT id FROM %s WHERE id < 5) SELECT * FROM w ORDER BY id", tb)},
		{sql: fmt.Sprintf("SELECT * FROM %s ORDER BY id", unwatched)},
		{sql: "SELECT 1 AS one, NULL::text AS nothing, 'a'::text AS a"},
		{sql: fmt.Sprintf("UPDATE %s SET num = num + 1 WHERE id <= $1 RETURNING id, num, txt", tb), args: []any{int64(5)}},
		{sql: fmt.Sprintf("UPDATE %s SET num = num + 1 WHERE id <= $1", tb), args: []any{int64(50)}},
		{sql: fmt.Sprintf("INSERT INTO %s (id, txt) VALUES ($1, $2) RETURNING *", tb), args: []any{int64(9001), "new"}},
		{sql: fmt.Sprintf("INSERT INTO %s (id, txt) VALUES ($1, $2)", tb), args: []any{int64(9002), "new"}},
		{sql: fmt.Sprintf("INSERT INTO %s (id, txt) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET txt = 'upd' RETURNING id, txt", tb), args: []any{int64(9001), "again"}},
		{sql: fmt.Sprintf("DELETE FROM %s WHERE id = $1 RETURNING id, txt", tb), args: []any{int64(9002)}},
		{sql: fmt.Sprintf("DELETE FROM %s WHERE id > $1", tb), args: []any{int64(4995)}},
		{sql: fmt.Sprintf("SELECT * FROM %s WHERE id > $1 ORDER BY id", tb), args: []any{int64(4990)}},
		{sql: fmt.Sprintf("SELECT no_such_column FROM %s", unwatched)},
	}
	ctx := context.Background()
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			collect := func(pool *pgxpool.Pool) []result {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				var out []result
				for i, q := range queries {
					if i == len(queries)-1 {
						// The last one fails, which aborts the transaction.
						out = append(out, run(ctx, tx, q.sql, q.args...))
						break
					}
					out = append(out, run(ctx, tx, q.sql, q.args...))
				}
				return out
			}
			want := collect(h.directPool(mode))
			got := collect(h.pool(mode))
			for i := range queries {
				w, g := want[i], got[i]
				if w.err != g.err {
					t.Fatalf("%s\nerror through the proxy %q, directly %q", queries[i].sql, g.err, w.err)
				}
				if !reflect.DeepEqual(w.fields, g.fields) {
					t.Fatalf("%s\ncolumns through the proxy:\n%+v\ndirectly:\n%+v", queries[i].sql, g.fields, w.fields)
				}
				if w.tag != g.tag {
					t.Fatalf("%s\ncommand tag %q, directly %q", queries[i].sql, g.tag, w.tag)
				}
				if !reflect.DeepEqual(w.rows, g.rows) {
					t.Fatalf("%s\n%d rows through the proxy, %d directly, or values differ", queries[i].sql, len(g.rows), len(w.rows))
				}
			}
			if len(want[0].rows) != 5000 {
				t.Fatalf("the first query should return 5000 rows, got %d", len(want[0].rows))
			}
		})
	}
}

// TestIsolationLevels: the level of each reported transaction comes from
// BEGIN, SET TRANSACTION, the session setting or the startup parameters.
func TestIsolationLevels(t *testing.T) {
	tb := tableName("px_iso")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", tb))
	ctx := context.Background()
	upd := fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = 1", tb)
	last := func() string {
		t.Helper()
		txns, _ := h.reported()
		if len(txns) == 0 {
			t.Fatal("nothing reported")
		}
		return txns[len(txns)-1].Isolation
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			pool := h.pool(mode)
			c, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Release()
			conn := c.Conn()
			check := func(what, want string) {
				t.Helper()
				if got := last(); got != want {
					t.Fatalf("%s: isolation %q, want %q", what, got, want)
				}
			}
			for _, lvl := range []pgx.TxIsoLevel{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted} {
				if err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: lvl}, func(tx pgx.Tx) error {
					_, err := tx.Exec(ctx, upd)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				check("BEGIN ISOLATION LEVEL", strings.ToLower(string(lvl)))
			}
			if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, upd)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			check("SET TRANSACTION", "repeatable read")
			if _, err := conn.Exec(ctx, upd); err != nil {
				t.Fatal(err)
			}
			check("statement without BEGIN", "read committed")
			if _, err := conn.Exec(ctx, "SET default_transaction_isolation = 'serializable'"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, upd); err != nil {
				t.Fatal(err)
			}
			check("after SET default_transaction_isolation", "serializable")
			if _, err := conn.Exec(ctx, "RESET default_transaction_isolation"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, upd); err != nil {
				t.Fatal(err)
			}
			check("after RESET", "read committed")
		})
	}

	t.Run("startup parameter", func(t *testing.T) {
		cfg, _ := pgx.ParseConfig(testDSN(t))
		cfg.Host, cfg.Port, cfg.Fallbacks, cfg.TLSConfig = "127.0.0.1", h.port, nil, nil
		cfg.RuntimeParams["default_transaction_isolation"] = "repeatable read"
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var real string
		conn.QueryRow(ctx, "SHOW transaction_isolation").Scan(&real)
		if _, err := conn.Exec(ctx, upd); err != nil {
			t.Fatal(err)
		}
		if got := last(); got != real || got != "repeatable read" {
			t.Fatalf("isolation %q, the server ran %q", got, real)
		}
	})
}

// TestCancelRequest: pgx cancels a query by connecting to the address it
// knows, which is the proxy. The proxy passes the request on, so
// PostgreSQL stops the query at once. (Without it, pgx would still return
// quickly, but the query would keep running on the server.)
func TestCancelRequest(t *testing.T) {
	h := newHarness(t, map[string]string{tableName("px_cancel"): "id int PRIMARY KEY"})
	pool := h.pool(pgx.QueryExecModeCacheStatement)
	marker := tableName("cancel_marker")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := pool.Exec(ctx, "SELECT pg_sleep(10) /* "+marker+" */"); err == nil {
		t.Fatal("the query should have been canceled")
	}
	time.Sleep(500 * time.Millisecond)
	var running int
	err := h.direct.QueryRow(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%' || $1 || '%' AND pid <> pg_backend_pid()",
		marker).Scan(&running)
	if err != nil {
		t.Fatal(err)
	}
	if running != 0 {
		t.Fatal("the query is still running on the server: the cancel request did not reach it")
	}
	var one int
	if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("the pool should still work: %v", err)
	}
}

// TestConnectionSettings: TLS is refused, so sslmode=require fails and the
// default (prefer) falls back to a plain connection. A wrong password fails
// as it does without the proxy (SCRAM is passed through untouched).
func TestConnectionSettings(t *testing.T) {
	h := newHarness(t, map[string]string{tableName("px_conn"): "id int PRIMARY KEY"})
	ctx := context.Background()
	base, _ := pgx.ParseConfig(testDSN(t))
	dsn := func(extra string) string {
		return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?%s", base.User, base.Password, h.port, base.Database, extra)
	}
	if _, err := pgx.Connect(ctx, dsn("sslmode=require")); err == nil {
		t.Fatal("sslmode=require must fail: the proxy does not offer TLS")
	}
	conn, err := pgx.Connect(ctx, dsn("sslmode=prefer"))
	if err != nil {
		t.Fatalf("sslmode=prefer should fall back to a plain connection: %v", err)
	}
	conn.Close(ctx)
	bad, _ := pgx.ParseConfig(dsn("sslmode=disable"))
	bad.Password = "not-the-password"
	_, err = pgx.ConnectConfig(ctx, bad)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
		t.Fatalf("want invalid password (28P01), got %v", err)
	}
}

// TestOtherSearchPath: on a connection with another search_path, a table
// name without a schema can mean another table. The proxy must not rewrite
// it (here the other table has no id column, so a rewrite would break the
// query), and still track names with a schema.
func TestOtherSearchPath(t *testing.T) {
	tb := tableName("px_path")
	schema := tableName("px_schema")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, v int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 10)", tb))
	h.exec(fmt.Sprintf("CREATE SCHEMA %s; CREATE TABLE %s.%s (k int PRIMARY KEY, v int); INSERT INTO %s.%s VALUES (1, 20)", schema, schema, tb, schema, tb))
	t.Cleanup(func() { _, _ = h.direct.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	ctx := context.Background()
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			pool := h.pool(mode)
			c, _ := pool.Acquire(ctx)
			defer c.Release()
			conn := c.Conn()
			var v int
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE v > 0", tb)).Scan(&v); err != nil || v != 10 {
				t.Fatalf("default path: v=%d err=%v", v, err)
			}
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET search_path = %s, public", schema)); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE v > 0", tb)).Scan(&v); err != nil || v != 20 {
				t.Fatalf("other path, no schema: v=%d err=%v", v, err)
			}
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM public.%s WHERE id = 1", tb)).Scan(&v); err != nil || v != 10 {
				t.Fatalf("other path, with schema: v=%d err=%v", v, err)
			}
			if _, err := conn.Exec(ctx, "RESET search_path"); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE v > 5", tb)).Scan(&v); err != nil || v != 10 {
				t.Fatalf("after RESET: v=%d err=%v", v, err)
			}
			txns, _ := h.reported()
			if len(txns) != 3 {
				t.Fatalf("want 3 reads reported (before, with schema, after RESET), got %d", len(txns))
			}
		})
	}
}

// TestStatementCacheEviction: a small pgx statement cache closes prepared
// statements all the time. Names are reused for new SQL.
func TestStatementCacheEviction(t *testing.T) {
	tb := tableName("px_evict")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, g FROM generate_series(1, 30) g", tb))
	ctx := context.Background()
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe} {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			cfg, _ := pgx.ParseConfig(testDSN(t))
			cfg.Host, cfg.Port, cfg.Fallbacks, cfg.TLSConfig = "127.0.0.1", h.port, nil, nil
			cfg.DefaultQueryExecMode = mode
			cfg.StatementCacheCapacity = 2
			cfg.DescriptionCacheCapacity = 2
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			for round := 0; round < 3; round++ {
				for id := 1; id <= 30; id++ {
					var n int
					// A different SQL text per id, so the cache keeps evicting.
					sql := fmt.Sprintf("SELECT n FROM %s WHERE id = $1 AND %d = %d", tb, id, id)
					if err := conn.QueryRow(ctx, sql, id).Scan(&n); err != nil || n != id {
						t.Fatalf("id %d: n=%d err=%v", id, n, err)
					}
				}
			}
			txns, _ := h.reported()
			if len(txns) != 90 {
				t.Fatalf("want 90 reads reported, got %d", len(txns))
			}
		})
	}
}

// TestParseErrorInABatch: a statement with a syntax error in the middle of
// a pipeline. In the modes that send Parse inside the pipeline, PostgreSQL
// rejects the Parse and skips everything up to the Sync.
func TestParseErrorInABatch(t *testing.T) {
	tb := tableName("px_perr")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 0), (2, 0)", tb))
	ctx := context.Background()
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			h.reset()
			pool := h.pool(mode)
			b := &pgx.Batch{}
			b.Queue(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = $1", tb), 1)
			b.Queue("SELEC broken")
			b.Queue(fmt.Sprintf("SELECT n FROM %s WHERE id = $1", tb), 2)
			if err := pool.SendBatch(ctx, b).Close(); err == nil {
				t.Fatal("the batch should fail")
			}
			for i := 0; i < 3; i++ {
				var n int
				if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT n FROM %s WHERE id = $1", tb), 2).Scan(&n); err != nil {
					t.Fatal(err)
				}
			}
			txns, _ := h.reported()
			if len(txns) != 3 {
				t.Fatalf("want only the 3 later reads reported, got %d", len(txns))
			}
		})
	}
}

// TestMultiStatementStringInATransaction: statements sent together in one
// string are forwarded untracked, and the transaction they ran in is not
// reported, because the proxy could not follow it.
func TestMultiStatementStringInATransaction(t *testing.T) {
	tb := tableName("px_multi")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 0), (2, 0)", tb))
	ctx := context.Background()
	pool := h.pool(pgx.QueryExecModeSimpleProtocol)
	c, _ := pool.Acquire(ctx)
	defer c.Release()
	conn := c.Conn().PgConn()
	if _, err := conn.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = 1", tb)).ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = 2; SELECT 1", tb)).ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = 1", tb)).ReadAll(); err != nil {
		t.Fatal(err)
	}
	txns, _ := h.reported()
	if len(txns) != 1 || len(txns[0].Writes) != 1 {
		t.Fatalf("want only the last statement reported, got %+v", txns)
	}
}

// TestSkippedCloseKeepsTheStatement: after an error, PostgreSQL skips a
// Close in the same pipeline, so the prepared statement still exists with
// its hidden columns. The proxy must still remove them when it runs.
func TestSkippedCloseKeepsTheStatement(t *testing.T) {
	tb := tableName("px_close")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 5)", tb))
	ctx := context.Background()
	pool := h.pool(pgx.QueryExecModeExec)
	c, _ := pool.Acquire(ctx)
	defer c.Release()
	pc := c.Conn().PgConn()
	if _, err := pc.Prepare(ctx, "x", fmt.Sprintf("SELECT n FROM %s WHERE id = 1", tb), nil); err != nil {
		t.Fatal(err)
	}
	pl := pc.StartPipeline(ctx)
	pl.SendQueryParams("SELECT 1/0", nil, nil, nil, nil)
	pl.SendDeallocate("x")
	if err := pl.Sync(); err != nil {
		t.Fatal(err)
	}
	for {
		res, err := pl.GetResults()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				continue
			}
			t.Fatal(err)
		}
		if _, ok := res.(*pgconn.PipelineSync); ok {
			break
		}
		if rr, ok := res.(*pgconn.ResultReader); ok {
			rr.Close()
		}
	}
	if err := pl.Close(); err != nil {
		t.Fatal(err)
	}
	r := pc.ExecPrepared(ctx, "x", nil, nil, nil).Read()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if len(r.FieldDescriptions) != 1 || len(r.Rows) != 1 || len(r.Rows[0]) != 1 || string(r.Rows[0][0]) != "5" {
		t.Fatalf("want one column with 5, got %d columns, rows %q", len(r.FieldDescriptions), r.Rows)
	}
}

// TestDeallocateThenPrepareInSQL: DEALLOCATE removes a statement the proxy
// rewrote. A statement made later with PREPARE in SQL gets the same name
// but is not rewritten, so nothing must be added to its Bind or removed
// from its rows.
func TestDeallocateThenPrepareInSQL(t *testing.T) {
	tb := tableName("px_dealloc")
	h := newHarness(t, map[string]string{tb: "id int PRIMARY KEY, n int"})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 5)", tb))
	ctx := context.Background()
	for _, sql := range []string{"DEALLOCATE x", "DISCARD ALL"} {
		t.Run(sql, func(t *testing.T) {
			pool := h.pool(pgx.QueryExecModeExec)
			c, _ := pool.Acquire(ctx)
			defer c.Release()
			pc := c.Conn().PgConn()
			if _, err := pc.Prepare(ctx, "x", fmt.Sprintf("SELECT n, id FROM %s WHERE id = 1", tb), nil); err != nil {
				t.Fatal(err)
			}
			if _, err := pc.Exec(ctx, sql).ReadAll(); err != nil {
				t.Fatal(err)
			}
			if _, err := pc.Exec(ctx, fmt.Sprintf("PREPARE x AS SELECT n, id FROM %s WHERE id = 1", tb)).ReadAll(); err != nil {
				t.Fatal(err)
			}
			r := pc.ExecPrepared(ctx, "x", nil, nil, []int16{1, 1}).Read()
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			if len(r.FieldDescriptions) != 2 || len(r.Rows) != 1 {
				t.Fatalf("want 2 columns and 1 row, got %d and %d", len(r.FieldDescriptions), len(r.Rows))
			}
		})
	}
}
