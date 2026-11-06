package rewrite

import (
	"strings"
	"testing"
)

// Coverage counts depend on Plan.Watched: statements that use a watched
// table anywhere count for coverage, everything else does not.
func TestWatchedFlag(t *testing.T) {
	cases := []struct {
		sql     string
		watched bool
		reason  string // expected reason for untracked statements ("" to skip)
	}{
		{"SELECT balance FROM accounts WHERE id = $1", true, ""},
		{"UPDATE accounts SET balance = 1 WHERE id = 2", true, ""},
		{"SELECT owner, sum(balance) FROM accounts GROUP BY owner", true, "GROUP BY or HAVING"},
		{"WITH x AS (SELECT id FROM accounts) SELECT * FROM x", true, "WITH query"},
		{"SELECT * FROM (SELECT id FROM accounts) s", true, "watched table only in a subquery or an aliased join"},
		{"SELECT * FROM users WHERE id IN (SELECT id FROM accounts)", true, "watched table only in a subquery or an aliased join"},
		{"INSERT INTO users SELECT id FROM accounts", true, "watched table only read by a write to another table"},
		{"UPDATE users u SET n = 1 FROM accounts a WHERE a.id = u.id", true, "watched table only read by a write to another table"},
		{"MERGE INTO accounts a USING users u ON a.id = u.id WHEN MATCHED THEN DELETE", true, "MERGE is not supported yet"},
		{"COPY accounts TO STDOUT", true, "COPY"},
		{"COPY accounts (id, balance) FROM STDIN", true, "COPY"},
		{"TRUNCATE accounts", true, "TRUNCATE"},
		{"EXPLAIN ANALYZE SELECT * FROM accounts", true, "EXPLAIN ANALYZE"},
		{"SELECT 1 FROM accounts; SELECT 2", true, "multiple statements"},
		{"SELECT id FROM billing.ledger", true, ""},

		{"SELECT id FROM users", false, "no watched table"},
		{"UPDATE users SET name = 'x'", false, "no watched table"},
		{"SELECT 1", false, ""},
		{"SELECT id FROM ledger", false, ""}, // not visible without the schema
		{"EXPLAIN SELECT * FROM accounts", false, ""},
		{"CREATE INDEX ON accounts (balance)", false, ""},
		{"LOCK TABLE accounts IN SHARE MODE", false, ""},
		{"SET search_path = public", false, ""},
		{"SAVEPOINT a", false, ""},
		{"COMMIT", false, ""},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			p := Make(c.sql, cat)
			if p.Watched != c.watched {
				t.Fatalf("Watched = %v, want %v (kind %v, reason %q)", p.Watched, c.watched, p.Kind, p.Reason)
			}
			if c.reason != "" && p.Reason != c.reason {
				t.Fatalf("reason = %q, want %q", p.Reason, c.reason)
			}
			if p.Watched && p.Normalized == "" {
				t.Fatal("watched statements must carry normalized SQL")
			}
			if !p.Watched && p.Normalized != "" {
				t.Fatalf("normalized SQL is only kept for watched statements, got %q", p.Normalized)
			}
			if p.Kind == Untracked && p.SQL != c.sql {
				t.Fatalf("untracked statements must run unchanged, got %q", p.SQL)
			}
		})
	}
}

func TestNormalizedHasNoLiterals(t *testing.T) {
	p := Make("SELECT owner FROM accounts WHERE owner = 'ann@example.com' GROUP BY owner", cat)
	if !p.Watched || strings.Contains(p.Normalized, "ann@example.com") {
		t.Fatalf("normalized SQL leaked a literal: %+v", p)
	}
}

func TestParseErrorCountsAsWatched(t *testing.T) {
	// IsoWatch cannot tell which tables this uses, so coverage must show it.
	if p := Make("SELEC balance FROM accounts", cat); !p.Watched || p.Kind != Untracked {
		t.Fatalf("got %+v", p)
	}
}
