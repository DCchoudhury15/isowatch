package protection

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeDB struct{}

func (fakeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("no database")
}

func (fakeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("no database")
}

// These tests need a real PostgreSQL server, for example:
//
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./protection/
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ISOWATCH_TEST_DSN")
	if dsn == "" {
		t.Skip("ISOWATCH_TEST_DSN not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func freshTable(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := fmt.Sprintf("iw_prot_%d", rand.Int64())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
	return name
}

func TestValidTable(t *testing.T) {
	good := []string{"isowatch_protected", "isowatch.protected", "_x", "t$1"}
	for _, n := range good {
		if err := ValidTable(n); err != nil {
			t.Errorf("%q should be valid: %v", n, err)
		}
	}
	bad := []string{"", "Protected", `"x"`, "a.b.c", "1abc", "x; DROP TABLE accounts", "x y", "a..b", "x.", "x-y",
		strings.Repeat("a", 64), "x." + strings.Repeat("a", 64)}
	for _, n := range bad {
		if err := ValidTable(n); err == nil {
			t.Errorf("%q should be rejected", n)
		}
	}
	if _, err := NewPostgres(nil, ""); err == nil {
		t.Error("a store without a database should be refused")
	}
}

func TestDefaultTableName(t *testing.T) {
	p, err := NewPostgres(fakeDB{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.Table() != DefaultTable || DefaultTable != "isowatch_protected" {
		t.Fatalf("table = %q", p.Table())
	}
}

func TestPostgresStore(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	table := freshTable(t, pool)
	s, err := NewPostgres(pool, table)
	if err != nil {
		t.Fatal(err)
	}

	// No table yet: nothing is protected, and that is not an error.
	names, err := s.List(ctx)
	if err != nil || len(names) != 0 {
		t.Fatalf("missing table: %v %v", names, err)
	}
	if err := s.Remove(ctx, "deposit"); err != nil {
		t.Fatalf("remove from a missing table: %v", err)
	}

	// Add creates the table on first use.
	if err := s.Add(ctx, "deposit", "lost-update: Lost update on public.accounts(1)"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(ctx, "deposit", "a second reason is ignored"); err != nil {
		t.Fatalf("adding twice must not fail: %v", err)
	}
	if err := s.Add(ctx, "withdraw", ""); err != nil {
		t.Fatal(err)
	}
	names, err = s.List(ctx)
	slices.Sort(names)
	if err != nil || !slices.Equal(names, []string{"deposit", "withdraw"}) {
		t.Fatalf("list = %v %v", names, err)
	}
	var reason string
	if err := pool.QueryRow(ctx, "SELECT reason FROM "+table+" WHERE name = 'deposit'").Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason != "lost-update: Lost update on public.accounts(1)" {
		t.Fatalf("the first reason should be kept, got %q", reason)
	}

	if err := s.Remove(ctx, "deposit"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, "deposit"); err != nil {
		t.Fatalf("removing twice must not fail: %v", err)
	}
	names, err = s.List(ctx)
	if err != nil || !slices.Equal(names, []string{"withdraw"}) {
		t.Fatalf("after remove: %v %v", names, err)
	}
}

// A row written by another program (like the collector) with only the
// columns from the contract is read by the store.
func TestReadsRowsWrittenByOthers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	schema := fmt.Sprintf("iw_prot_schema_%d", rand.Int64())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	table := schema + ".protected"
	s, err := NewPostgres(pool, table)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("creating twice must not fail: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+table+" (name, reason) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING", "transfer", "from the collector"); err != nil {
		t.Fatal(err)
	}
	names, err := s.List(ctx)
	if err != nil || !slices.Equal(names, []string{"transfer"}) {
		t.Fatalf("list = %v %v", names, err)
	}
}

func TestStoreErrorsAreReturned(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	table := freshTable(t, pool)
	// A table with the right name but the wrong shape: every call fails.
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (id int)"); err != nil {
		t.Fatal(err)
	}
	s, _ := NewPostgres(pool, table)
	if err := s.Add(ctx, "deposit", ""); err == nil {
		t.Error("add should fail")
	}
	if _, err := s.List(ctx); err == nil {
		t.Error("list should fail")
	}
	if err := s.Remove(ctx, "deposit"); err == nil {
		t.Error("remove should fail")
	}
}
