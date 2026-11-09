package main

// These tests show how to use IsoWatch in "go test". They call the real
// deposit endpoints from serve.go and let isowatchtest check every
// transaction they run.
//
// They need PostgreSQL 18 and skip without it:
//
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test .

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/isowatchtest"
)

// testServer returns the demo server on a fresh schema with the demo data,
// so tests do not touch each other's tables or a running demo.
func testServer(t *testing.T) *server {
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
	name := fmt.Sprintf("bank_test_%d", rand.Int64())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+name+" CASCADE")
		_ = admin.Close(context.Background())
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 32 // at least as many as calls at once, so they race
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, schema+seed); err != nil {
		t.Fatal(err)
	}
	return &server{pool: pool, window: 5 * time.Millisecond}
}

// deposits sends n deposits of 1 to account 1 at the same moment, through
// an endpoint handler from serve.go, and returns the balance afterwards.
func deposits(t *testing.T, s *server, n int, endpoint func(context.Context, *http.Request) error) int {
	t.Helper()
	isowatchtest.Concurrently(t, n, func(int) error {
		return endpoint(t.Context(), httptest.NewRequest("POST", "/deposit?account=1&amount=1", nil))
	})
	var balance int
	if err := s.pool.QueryRow(context.Background(), "SELECT balance FROM accounts WHERE id = 1").Scan(&balance); err != nil {
		t.Fatal(err)
	}
	return balance
}

// The buggy endpoint reads the balance, adds in Go and writes it back.
// Concurrent deposits overwrite each other, and IsoWatch says exactly that.
func TestDepositLosesMoney(t *testing.T) {
	s := testServer(t)
	m := isowatchtest.New(t, s.pool, isowatchtest.Tables("accounts"))
	s.mon = m.Monitor
	m.ExpectAnomaly(isowatchtest.LostUpdate)

	const n = 20
	balance := deposits(t, s, n, s.deposit("deposit", ""))

	lost := n - balance
	if lost == 0 {
		t.Fatal("no deposit was lost this time; the race did not happen")
	}
	if got := m.LostWrites(); got != lost {
		t.Errorf("IsoWatch counted %d lost deposits, the database lost %d", got, lost)
	}
	t.Logf("%d deposits, balance %d: %d lost, and IsoWatch counted %d", n, balance, lost, m.LostWrites())
}

// The safe endpoint reads with SELECT ... FOR UPDATE. Every deposit counts,
// and IsoWatch sees no anomaly.
func TestSafeDepositIsClean(t *testing.T) {
	s := testServer(t)
	m := isowatchtest.New(t, s.pool, isowatchtest.Tables("accounts"))
	s.mon = m.Monitor

	const n = 20
	if balance := deposits(t, s, n, s.deposit("deposit-safe", "FOR UPDATE")); balance != n {
		t.Fatalf("balance is %d after %d deposits", balance, n)
	}
	m.ExpectNoAnomalies()
}
