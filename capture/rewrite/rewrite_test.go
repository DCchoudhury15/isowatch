package rewrite

import (
	"regexp"
	"strings"
	"testing"
)

var hiddenAlias = regexp.MustCompile(` AS __isowatch_\d+`)

type fakeCatalog map[string]*Table

func (f fakeCatalog) Lookup(schema, name string) (*Table, bool) {
	if schema == "" {
		for _, t := range f {
			if t.Name == name && t.Unqualified {
				return t, true
			}
		}
		return nil, false
	}
	t, ok := f[schema+"."+name]
	return t, ok
}

var cat = fakeCatalog{
	"public.accounts": {Schema: "public", Name: "accounts", PK: []string{"id"}, Unqualified: true},
	"public.seats":    {Schema: "public", Name: "seats", PK: []string{"show_id", "seat_no"}, Unqualified: true},
	"billing.ledger":  {Schema: "billing", Name: "ledger", PK: []string{"id"}},
}

func TestPlans(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		kind   Kind
		want   string // expected rewritten SQL ("" to skip the check)
		hidden int
	}{
		{"simple select", "SELECT balance FROM accounts WHERE id = $1", Read,
			"SELECT balance, accounts.xmin::text, accounts.id::text FROM accounts WHERE id = $1", 2},
		{"alias and lock", "SELECT a.balance FROM accounts a WHERE a.id = $1 FOR UPDATE", Read,
			"SELECT a.balance, a.xmin::text, a.id::text FROM accounts a WHERE a.id = $1 FOR UPDATE", 2},
		{"star", "SELECT * FROM accounts", Read,
			"SELECT *, accounts.xmin::text, accounts.id::text FROM accounts", 2},
		{"composite key", "SELECT taken FROM seats WHERE show_id = 1", Read,
			"SELECT taken, seats.xmin::text, seats.show_id::text, seats.seat_no::text FROM seats WHERE show_id = 1", 3},
		{"join two watched", "SELECT a.id FROM accounts a JOIN seats s ON s.show_id = a.id", Read, "", 5},
		{"join with unwatched", "SELECT a.id FROM accounts a JOIN users u ON u.id = a.id", Read, "", 2},
		{"schema qualified", "SELECT id FROM billing.ledger", Read,
			"SELECT id, ledger.xmin::text, ledger.id::text FROM billing.ledger", 2},
		{"unqualified not allowed", "SELECT id FROM ledger", Untracked, "", 0},
		{"aggregate", "SELECT count(*) FROM accounts", AggRead,
			"SELECT count(*), (SELECT COALESCE(json_agg(json_build_array(accounts.xmin::text, accounts.id::text)), '[]'::pg_catalog.json) FROM accounts)::text FROM accounts", 1},
		{"aggregate with where and alias", "SELECT sum(balance) FROM accounts a WHERE a.id > $1", AggRead,
			"SELECT sum(balance), (SELECT COALESCE(json_agg(json_build_array(a.xmin::text, a.id::text)), '[]'::pg_catalog.json) FROM accounts a WHERE a.id > $1)::text FROM accounts a WHERE a.id > $1", 1},
		{"aggregate composite key", "SELECT count(*) FROM seats WHERE show_id = 1", AggRead,
			"SELECT count(*), (SELECT COALESCE(json_agg(json_build_array(seats.xmin::text, seats.show_id::text, seats.seat_no::text)), '[]'::pg_catalog.json) FROM seats WHERE show_id = 1)::text FROM seats WHERE show_id = 1", 1},
		{"aggregate with filter", "SELECT count(*) FILTER (WHERE balance > 0) FROM accounts", Untracked, "", 0},
		{"aggregate over a join", "SELECT count(*) FROM accounts a JOIN seats s ON s.show_id = a.id", Untracked, "", 0},
		{"aggregate with volatile where", "SELECT count(*) FROM accounts WHERE random() < 0.5", Untracked, "", 0},
		{"aggregate not watched", "SELECT count(*) FROM users", Untracked, "", 0},
		{"aggregate grouped", "SELECT count(*) FROM accounts GROUP BY id", Untracked, "", 0},
		{"window", "SELECT id, rank() OVER (ORDER BY balance) FROM accounts", Untracked, "", 0},
		{"group by", "SELECT id FROM accounts GROUP BY id", Untracked, "", 0},
		{"distinct", "SELECT DISTINCT id FROM accounts", Untracked, "", 0},
		{"union", "SELECT id FROM accounts UNION SELECT id FROM accounts", Untracked, "", 0},
		{"with", "WITH x AS (SELECT 1) SELECT id FROM accounts", Untracked, "", 0},
		{"not watched", "SELECT id FROM users", Untracked, "", 0},
		{"no from", "SELECT 1", Untracked, "", 0},
		{"same table twice", "SELECT 1 FROM accounts a, accounts a", Untracked, "", 0},
		{"subquery only", "SELECT * FROM (SELECT id FROM accounts) s", Untracked, "", 0},

		{"update", "UPDATE accounts SET balance = $1 WHERE id = $2", Write,
			"UPDATE accounts SET balance = $1 WHERE id = $2 RETURNING old.xmin::text, old.id::text, new.xmin::text, new.id::text", 4},
		{"update keeps user returning", "UPDATE accounts SET balance = balance + 1 WHERE id = 1 RETURNING balance", Write,
			"UPDATE accounts SET balance = balance + 1 WHERE id = 1 RETURNING balance, old.xmin::text, old.id::text, new.xmin::text, new.id::text", 4},
		{"delete", "DELETE FROM accounts WHERE id = 1", Write,
			"DELETE FROM accounts WHERE id = 1 RETURNING old.xmin::text, old.id::text, new.xmin::text, new.id::text", 4},
		{"insert", "INSERT INTO accounts (id, balance) VALUES (1, 2)", Write,
			"INSERT INTO accounts (id, balance) VALUES (1, 2) RETURNING old.xmin::text, old.id::text, new.xmin::text, new.id::text", 4},
		{"upsert", "INSERT INTO accounts (id, balance) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET balance = excluded.balance", Write, "", 4},
		{"update alias old", "UPDATE accounts old SET balance = 1", Untracked, "", 0},
		{"update not watched", "UPDATE users SET name = 'x'", Untracked, "", 0},
		{"merge", "MERGE INTO accounts a USING users u ON a.id = u.id WHEN MATCHED THEN UPDATE SET balance = 0", Untracked, "", 0},

		{"savepoint", "SAVEPOINT sp1", Savepoint, "", 0},
		{"release", "RELEASE SAVEPOINT sp1", Release, "", 0},
		{"rollback to", "ROLLBACK TO SAVEPOINT sp1", RollbackTo, "", 0},
		{"commit forbidden", "COMMIT", Forbidden, "", 0},
		{"begin forbidden", "BEGIN", Forbidden, "", 0},

		{"two statements", "SELECT 1; SELECT 2", Untracked, "", 0},
		// "@" is the absolute value operator, so this is valid SQL. The capture
		// layer expands pgx named arguments before planning anyway.
		{"at sign is an operator", "SELECT * FROM accounts WHERE id = @id", Read, "", 2},
		{"ddl", "CREATE INDEX ON accounts (balance)", Untracked, "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Make(c.sql, cat)
			if p.Kind != c.kind {
				t.Fatalf("kind = %v (reason %q), want %v", p.Kind, p.Reason, c.kind)
			}
			if p.Hidden != c.hidden {
				t.Fatalf("hidden = %d, want %d", p.Hidden, c.hidden)
			}
			if got := hiddenAlias.ReplaceAllString(p.SQL, ""); c.want != "" && got != c.want {
				t.Fatalf("sql:\n got  %s\n want %s", got, c.want)
			}
			if p.Kind == Untracked && p.SQL != c.sql {
				t.Fatalf("untracked statements must run unchanged, got %q", p.SQL)
			}
		})
	}
}

func TestSavepointNameIsLowercased(t *testing.T) {
	if p := Make(`SAVEPOINT Sp_One`, cat); p.Name != "sp_one" {
		t.Fatalf("got %q", p.Name)
	}
}

func TestNormalizeRemovesLiterals(t *testing.T) {
	n := Normalize("SELECT * FROM accounts WHERE email = 'a@b.c' AND id = 42")
	if strings.Contains(n, "a@b.c") || strings.Contains(n, "42") {
		t.Fatalf("literals leaked: %s", n)
	}
}

func TestSetTransactionIsDetected(t *testing.T) {
	for _, q := range []string{
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET transaction_isolation = 'serializable'",
		"SET LOCAL transaction_isolation TO 'repeatable read'",
	} {
		if p := Make(q, cat); !p.SetsIsolation {
			t.Errorf("%q: SetsIsolation should be true", q)
		}
	}
	if p := Make("SET search_path = public", cat); p.SetsIsolation {
		t.Error("search_path is not isolation")
	}
}

func TestHiddenColumnsHaveUniqueNames(t *testing.T) {
	p := Make("SELECT * FROM accounts ORDER BY id", cat)
	want := "SELECT *, accounts.xmin::text AS __isowatch_0, accounts.id::text AS __isowatch_1 FROM accounts ORDER BY id"
	if p.SQL != want {
		t.Fatalf("got  %s\nwant %s", p.SQL, want)
	}
}
