package isowatchprocessor

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
	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
	"github.com/DCchoudhury15/isowatch/protection"
)

// These tests need a real PostgreSQL 18 server, like the capture tests:
//
//	ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./...
func testPool(t *testing.T) (string, *pgxpool.Pool) {
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
	return dsn, pool
}

// rows returns name -> reason from a protection table.
func rows(t *testing.T, pool *pgxpool.Pool, table string) map[string]string {
	t.Helper()
	r, err := pool.Query(context.Background(), "SELECT name, reason FROM "+table)
	if err != nil {
		return nil
	}
	defer r.Close()
	out := map[string]string{}
	for r.Next() {
		var n, why string
		if err := r.Scan(&n, &why); err != nil {
			t.Fatal(err)
		}
		out[n] = why
	}
	return out
}

// eventually waits until ok returns true.
func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProtectionWritesNamesToTheSharedTable(t *testing.T) {
	dsn, pool := testPool(t)
	ctx := context.Background()
	table := fmt.Sprintf("iw_protected_%d", rand.Int64())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: configopaque.String(dsn), Table: table}
	p, sink, _ := startProcessor(t, nopSettings(), cfg)

	// The processor creates the table with exactly the agreed columns.
	race := depositRace
	race.nameA, race.nameB = "deposit", "withdraw"
	sendRace(t, p, 0, race)
	eventually(t, "both names", func() bool { return len(rows(t, pool, table)) == 2 })

	var cols []string
	r, err := pool.Query(ctx, `SELECT column_name || ' ' || data_type || ' ' || is_nullable || ' ' || coalesce(column_default, '')
		FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var c string
		_ = r.Scan(&c)
		cols = append(cols, c)
	}
	r.Close()
	want := []string{"name text NO ", "reason text NO ''::text", "added_at timestamp with time zone NO now()"}
	if strings.Join(cols, "|") != strings.Join(want, "|") {
		t.Fatalf("columns = %q, want %q", cols, want)
	}
	var pk string
	if err := pool.QueryRow(ctx, `SELECT a.attname FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary`, table).Scan(&pk); err != nil || pk != "name" {
		t.Fatalf("primary key = %q (%v), want name", pk, err)
	}

	got := rows(t, pool, table)
	issueID := strAttr(t, spansNamed(sink, history.AnomalySpan)[0], "isowatch.issue.id")
	for _, n := range []string{"deposit", "withdraw"} {
		if !strings.Contains(got[n], "lost-update") || !strings.Contains(got[n], issueID) {
			t.Fatalf("reason for %s = %q, want the kind and issue %s", n, got[n], issueID)
		}
	}

	// A name is written once per process: if someone removes it, the same
	// bug happening again does not put it back.
	if _, err := pool.Exec(ctx, "DELETE FROM "+table+" WHERE name = 'deposit'"); err != nil {
		t.Fatal(err)
	}
	for n := 1; n < 20; n++ {
		sendRace(t, p, n, race)
	}
	// A new name in a later anomaly is still written, and it shows that the
	// queue has moved past the repeats.
	transfer := depositRace
	transfer.nameA, transfer.nameB = "transfer", "transfer"
	sendRace(t, p, 50, transfer)
	eventually(t, "transfer", func() bool { _, ok := rows(t, pool, table)["transfer"]; return ok })
	if _, ok := rows(t, pool, table)["deposit"]; ok {
		t.Fatal("deposit was written twice")
	}
}

func TestProtectionSkipsAnomaliesItCannotFix(t *testing.T) {
	dsn, pool := testPool(t)
	table := fmt.Sprintf("iw_protected_%d", rand.Int64())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: configopaque.String(dsn), Table: table}
	p, _, _ := startProcessor(t, nopSettings(), cfg)

	// One side has no name: protecting only the other side would not stop
	// the race, so nothing is written.
	half := depositRace
	half.nameA, half.nameB = "deposit", ""
	sendRace(t, p, 0, half)
	// Then a fully named one, so we know the writer has run.
	marker := depositRace
	marker.nameA, marker.nameB = "marker", "marker"
	sendRace(t, p, 1, marker)
	eventually(t, "marker", func() bool { _, ok := rows(t, pool, table)["marker"]; return ok })
	if got := rows(t, pool, table); len(got) != 1 {
		t.Fatalf("only marker should be protected, got %v", got)
	}
}

func TestProtectableRules(t *testing.T) {
	named := func(names ...string) []*history.Txn {
		var out []*history.Txn
		for _, n := range names {
			out = append(out, &history.Txn{Name: n})
		}
		return out
	}
	cases := []struct {
		a    checker.Anomaly
		want string
	}{
		{checker.Anomaly{Kind: checker.LostUpdate, Txns: named("a", "b")}, "a,b"},
		{checker.Anomaly{Kind: checker.WriteSkew, Txns: named("a", "a")}, "a"},
		{checker.Anomaly{Kind: checker.LostUpdate, Txns: named("a", "")}, ""},
		{checker.Anomaly{Kind: checker.LostUpdate, Txns: named("a", "b"), Unexpected: true}, ""},
		{checker.Anomaly{Kind: checker.G1c, Txns: named("a", "b")}, ""},
		{checker.Anomaly{Kind: checker.G0, Txns: named("a", "b")}, ""},
	}
	for _, c := range cases {
		if got := strings.Join(protectable(c.a), ","); got != c.want {
			t.Errorf("%s unexpected=%v names=%d: got %q, want %q", c.a.Kind, c.a.Unexpected, len(c.a.Txns), got, c.want)
		}
	}
}

// A failed write is not remembered, so the next anomaly tries again. Here
// the schema does not exist at first.
func TestProtectionRetriesAfterAFailedWrite(t *testing.T) {
	dsn, pool := testPool(t)
	ctx := context.Background()
	schema := fmt.Sprintf("iw_schema_%d", rand.Int64())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	table := schema + ".protected"

	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: configopaque.String(dsn), Table: table}
	p, _, _ := startProcessor(t, nopSettings(), cfg)

	sendRace(t, p, 0, depositRace)
	eventually(t, "the failed write to be forgotten", func() bool {
		p.prot.mu.Lock()
		defer p.prot.mu.Unlock()
		return !p.prot.names["deposit"]
	})
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	sendRace(t, p, 1, depositRace)
	eventually(t, "deposit after the retry", func() bool { _, ok := rows(t, pool, table)["deposit"]; return ok })
}

// A database that is down must not stop the collector or slow the pipeline.
func TestProtectionWithDatabaseDown(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: "postgres://nobody@127.0.0.1:1/none?connect_timeout=1", Table: "isowatch_protected"}
	p, _, _ := startProcessor(t, nopSettings(), cfg)
	start := time.Now()
	for n := 0; n < 50; n++ {
		r := depositRace
		r.nameA = fmt.Sprintf("op-%d", n)
		r.nameB = r.nameA
		sendRace(t, p, n, r)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the pipeline waited for the database: %v", d)
	}
}

func TestProtectionConfigIsChecked(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: "postgres://localhost/db", Table: "bad; DROP TABLE x"}
	if cfg.Validate() == nil {
		t.Fatal("table name must be checked")
	}
	cfg.Protection.Table = "app.isowatch_protected"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	// Upper case would be quoted here but folded to lower case in the apps'
	// unquoted SQL: two different tables. The apps refuse it, so must we.
	cfg.Protection.Table = "Protected"
	if cfg.Validate() == nil {
		t.Fatal("an upper case table name must be refused, like protection.NewPostgres does")
	}
}

// The point of the protection table: a name the collector writes makes an
// app replica that reads the same table run that transaction at
// SERIALIZABLE.
func TestProtectionReachesTheApps(t *testing.T) {
	dsn, pool := testPool(t)
	ctx := context.Background()
	table := fmt.Sprintf("iw_protected_%d", rand.Int64())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	store, err := protection.NewPostgres(pool, table)
	if err != nil {
		t.Fatal(err)
	}
	accounts := fmt.Sprintf("iw_accounts_%d", rand.Int64())
	if _, err := pool.Exec(ctx, "CREATE TABLE "+accounts+" (id int PRIMARY KEY, balance int)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+accounts) })
	app, err := capture.New(ctx, pool, capture.Config{
		Tables:          []string{accounts},
		Sink:            capture.SinkFunc(func(context.Context, *history.Txn) {}),
		ProtectionStore: store,
		ProtectionPoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Close)
	if app.IsProtected("deposit") {
		t.Fatal("test setup: deposit is protected before any anomaly")
	}

	cfg := createDefaultConfig()
	cfg.Protection = ProtectionConfig{DSN: configopaque.String(dsn), Table: table}
	p, _, _ := startProcessor(t, nopSettings(), cfg)
	sendRace(t, p, 0, depositRace)

	eventually(t, "the app to protect deposit", func() bool { return app.IsProtected("deposit") })
	err = app.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
		var iso string
		if err := tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&iso); err != nil {
			return err
		}
		if iso != "serializable" {
			return fmt.Errorf("deposit ran at %q, want serializable", iso)
		}
		return nil
	}, capture.WithName("deposit"))
	if err != nil {
		t.Fatal(err)
	}
}
