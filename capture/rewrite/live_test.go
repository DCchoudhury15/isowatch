package rewrite

import (
	"context"
	"os"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestRewrittenSQLRunsOnPostgres executes every rewritten statement from
// TestPlans on a real PostgreSQL 18 server. Set ISOWATCH_TEST_DSN to run it.
func TestRewrittenSQLRunsOnPostgres(t *testing.T) {
	dsn := os.Getenv("ISOWATCH_TEST_DSN")
	if dsn == "" {
		t.Skip("ISOWATCH_TEST_DSN not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS accounts(id int PRIMARY KEY, balance int)`,
		`CREATE TABLE IF NOT EXISTS seats(show_id int, seat_no int, taken bool, PRIMARY KEY (show_id, seat_no))`,
		`CREATE TABLE IF NOT EXISTS users(id int PRIMARY KEY, name text)`,
		`CREATE SCHEMA IF NOT EXISTS billing`,
		`CREATE TABLE IF NOT EXISTS billing.ledger(id int PRIMARY KEY)`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	params := regexp.MustCompile(`\$\d+`)
	queries := []string{
		"SELECT balance FROM accounts WHERE id = $1",
		"SELECT * FROM accounts ORDER BY id",
		"SELECT a.balance FROM accounts a WHERE a.id = $1 FOR UPDATE",
		"SELECT * FROM accounts",
		"SELECT taken FROM seats WHERE show_id = 1",
		"SELECT a.id FROM accounts a JOIN seats s ON s.show_id = a.id",
		"SELECT a.id FROM accounts a LEFT JOIN users u ON u.id = a.id",
		"SELECT id FROM billing.ledger",
		"SELECT a.id FROM accounts a JOIN users u USING (id)",
		"UPDATE accounts SET balance = $1 WHERE id = $2",
		"UPDATE accounts a SET balance = a.balance + 1 FROM users u WHERE u.id = a.id RETURNING a.balance",
		"DELETE FROM accounts WHERE id = 1",
		"INSERT INTO accounts (id, balance) VALUES (77, 2)",
		"INSERT INTO accounts (id, balance) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET balance = excluded.balance",
		"INSERT INTO accounts (id, balance) VALUES (1, 2) ON CONFLICT DO NOTHING",
		"DELETE FROM seats WHERE show_id = 1",
	}
	for _, q := range queries {
		p := Make(q, cat)
		if p.Kind != Read && p.Kind != Write {
			continue
		}
		sql := params.ReplaceAllString(p.SQL, "1")
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, sql)
		if err == nil {
			for rows.Next() {
				if got := len(rows.FieldDescriptions()); got < p.Hidden {
					t.Errorf("%s: only %d columns, %d hidden", q, got, p.Hidden)
				}
			}
			err = rows.Err()
		}
		if err != nil {
			t.Errorf("rewritten SQL failed on PostgreSQL:\n  original:  %s\n  rewritten: %s\n  error: %v", q, sql, err)
		}
		_ = tx.Rollback(ctx)
	}
}
