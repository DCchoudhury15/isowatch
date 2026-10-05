package capture

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/history"
)

// The same transaction (read one row, update it) with and without IsoWatch,
// against a real PostgreSQL 18. Needs ISOWATCH_TEST_DSN.
func benchSetup(b *testing.B) (*pgxpool.Pool, string) {
	dsn := envDSN(b)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)
	name := "iw_bench"
	ctx := context.Background()
	pool.Exec(ctx, "DROP TABLE IF EXISTS "+name)
	pool.Exec(ctx, "CREATE TABLE "+name+" (id int PRIMARY KEY, n int)")
	pool.Exec(ctx, "INSERT INTO "+name+" SELECT g, 0 FROM generate_series(1, 1000) g")
	b.Cleanup(func() { pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
	return pool, name
}

func envDSN(b *testing.B) string {
	b.Helper()
	dsn := testDSN()
	if dsn == "" {
		b.Skip("ISOWATCH_TEST_DSN not set")
	}
	return dsn
}

func BenchmarkPlainPgx(b *testing.B) {
	pool, tbl := benchSetup(b)
	ctx := context.Background()
	sel := fmt.Sprintf("SELECT n FROM %s WHERE id = $1", tbl)
	upd := fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = $2", tbl)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := 1 + i%1000
		tx, _ := pool.Begin(ctx)
		var n int
		tx.QueryRow(ctx, sel, id).Scan(&n)
		tx.Exec(ctx, upd, n+1, id)
		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWithIsoWatch(b *testing.B) {
	pool, tbl := benchSetup(b)
	ctx := context.Background()
	m, err := New(ctx, pool, Config{Tables: []string{tbl}, Sink: SinkFunc(func(context.Context, *history.Txn) {})})
	if err != nil {
		b.Fatal(err)
	}
	sel := fmt.Sprintf("SELECT n FROM %s WHERE id = $1", tbl)
	upd := fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = $2", tbl)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := 1 + i%1000
		tx, _ := m.Begin(ctx, pool, pgx.TxOptions{})
		var n int
		tx.QueryRow(ctx, sel, id).Scan(&n)
		tx.Exec(ctx, upd, n+1, id)
		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
