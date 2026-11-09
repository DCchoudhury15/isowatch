# Testing with IsoWatch

The `isowatchtest` package turns IsoWatch into a check for `go test` and CI. You write a test that runs your code a few times at once. If two of those calls race and lose data, the test fails and tells you which SQL raced and how to fix it.

This finds lost updates and write skew before they reach production, using the tests you already have.

## What you need

- PostgreSQL 18 **for the test database only**. IsoWatch uses `RETURNING old.xmin`, which is new in 18. Your production database can stay on an older version: the races in your code are the same.
- Code that starts its transactions with `m.Do` or `m.Begin` (the same calls as `capture.Monitor`). Transactions started on the pool directly are not seen.

## A first test

```go
import (
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/DCchoudhury15/isowatch/capture"
	"github.com/DCchoudhury15/isowatch/isowatchtest"
)

func TestDeposit(t *testing.T) {
	pool := testPool(t) // your pool on PostgreSQL 18, with the tables created

	m := isowatchtest.New(t, pool, isowatchtest.Tables("accounts"))

	isowatchtest.Concurrently(t, 20, func(i int) error {
		return m.Do(t.Context(), pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
			var balance int
			if err := tx.QueryRow(t.Context(), "SELECT balance FROM accounts WHERE id = 1").Scan(&balance); err != nil {
				return err
			}
			_, err := tx.Exec(t.Context(), "UPDATE accounts SET balance = $1 WHERE id = 1", balance+1)
			return err
		}, capture.WithName("deposit"))
	})
}
```

That is all. When the test ends, it fails if IsoWatch saw any anomaly. You do not need to check the balance yourself.

If your code takes a `*capture.Monitor` (for example a server struct), pass `m.Monitor`.

## What a failure looks like

This is real output from the bank example (`examples/bank`), run with the buggy deposit and no `ExpectAnomaly`. `bank_test_...` is the test's own schema.

```text
--- FAIL: TestDeposit (0.10s)
    deposit_test.go:11: IsoWatch saw 15 isolation anomalies (15 lost-update) that this test did not expect.
        Each one is a race that really happened in this test: PostgreSQL returned the row versions that prove it.

        1) lost-update, 15 times, between transactions named "deposit"
           Lost update on bank_test_7921241099130129244.accounts(1): a transaction wrote this row based
           on an old read and replaced 1 newer version(s) it never saw.
           Rows: bank_test_7921241099130129244.accounts(1)
           What happened (one example):
             deposit (txn 1e2ab115)  read      bank_test_7921241099130129244.accounts(1) version 69935
               SELECT balance FROM accounts WHERE id = $1
             deposit (txn 28006b22)  replaced  bank_test_7921241099130129244.accounts(1) version 69935
               UPDATE accounts SET balance = $1 WHERE id = $2
             deposit (txn 28006b22)  wrote     bank_test_7921241099130129244.accounts(1) version 69937
               UPDATE accounts SET balance = $1 WHERE id = $2
             deposit (txn 1e2ab115)  replaced  bank_test_7921241099130129244.accounts(1) version 69937
               UPDATE accounts SET balance = $1 WHERE id = $2
           Fix: Lock the rows when you read them: add FOR UPDATE to "SELECT balance FROM accounts WHERE
           id = $1". Or write it as one relative update (for example SET balance = balance + $1) instead
           of reading and then writing. Or run these transactions at REPEATABLE READ or SERIALIZABLE and
           retry when PostgreSQL returns error 40001.
           14 more like this one (same kind, transaction names and SQL).

        IsoWatch counted 19 lost write(s): changes that were overwritten by a transaction that never saw them.

        IsoWatch checked 20 committed transaction(s) and tracked 40 statement(s).

        Next: apply the fix above and run the test again.
        If this test is meant to show the race, call m.ExpectAnomaly(isowatchtest.LostUpdate).
        If the race is acceptable, pass isowatchtest.IgnoreNames("deposit") or isowatchtest.IgnoreTables("accounts") to isowatchtest.New.
```

The same bug happening many times is shown once, with a count. Each different bug gets its own numbered group.

## Making races happen

A race only shows up if the calls really overlap. `Concurrently(t, n, fn)` starts `n` goroutines, holds them at a barrier until all are ready, then lets them go together. If a call returns an error or panics, the test fails with that error.

Some tips:

- Give the pool at least `n` connections (`MaxConns`), or calls wait for a connection and stop overlapping. `New` opens all the pool's connections up front for the same reason.
- If the race only happens sometimes, add rounds: `Concurrently(t, 20, fn, isowatchtest.Repeat(10))`.
- A test that never sees a race proves little. If your fast code path finishes before the next call starts, the race window may be too small to hit. A short sleep between the read and the write in a test build makes it reliable.

## Checking results

| Call | What it does |
|---|---|
| `m.ExpectAnomaly(isowatchtest.LostUpdate)` | The test is meant to show this race. It passes only if IsoWatch saw it. Other kinds still fail the test. |
| `m.ExpectNoAnomalies()` | Fails right away if IsoWatch saw any anomaly so far, even an expected one. Also fails if IsoWatch checked no transaction at all, because then "no anomalies" proves nothing. |
| `m.Anomalies()` | Every anomaly seen so far, for your own checks. |
| `m.LostWrites()` | How many writes were overwritten by a transaction that never saw them. In the bank example this equals the money the database lost. |
| `m.Stats()` | How many statements IsoWatch tracked, and how many it could not. |

Kinds: `LostUpdate`, `WriteSkew`, `GSingle`, `G2Item`.

## Options for `New`

| Option | Use it when |
|---|---|
| `Tables("accounts", "billing.ledger")` | You want to watch only some tables. Without it, `New` watches every plain table with a primary key on the pool's `search_path`, as they exist when `New` is called. So create your tables first. |
| `IgnoreTables(...)` | Races on these tables are fine (for example a view counter). |
| `IgnoreNames(...)` | Transactions with these names (`capture.WithName`) are allowed to race. |
| `GroupByTrace()` | You want to check whole requests instead of single transactions. See below. |
| `CheckerOptions(...)` | You need a checker setting not listed here. |

### Whole requests: `GroupByTrace`

By default each transaction is checked on its own. Some bugs need a whole request: it reads in one transaction and writes in a later one. `GroupByTrace()` finds those.

It is off by default because:

- Every concurrent call needs its own trace id. Wrap the context with `isowatchtest.NewTrace(ctx)` inside each call. If calls share one trace, they count as one request and the races between them are hidden.
- A request that reads a row and later changes it with a relative update (`SET n = n + 1`) in another transaction is reported, even though no data was lost.

## Running in CI

Add PostgreSQL 18 as a service and point your tests at it. For GitHub Actions:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: postgres
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 2s
          --health-timeout 3s
          --health-retries 30
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26"
      - run: go test -race -count=1 ./...
        env:
          ISOWATCH_TEST_DSN: postgres://postgres:postgres@localhost:5432/postgres
```

On your laptop:

```sh
docker run -d --name pg18 -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:18
ISOWATCH_TEST_DSN=postgres://postgres:postgres@localhost:55432/postgres go test ./...
```

Give each test its own schema (set `search_path` on the pool) so tests do not see each other's tables. `examples/bank/bank_test.go` shows one way.

## A full example

`examples/bank/bank_test.go` runs the real deposit handlers of the demo bank:

- `TestDepositLosesMoney` runs the buggy handler, expects the lost update, and checks that IsoWatch counted exactly the money the database lost.
- `TestSafeDepositIsClean` runs the `FOR UPDATE` handler and expects no anomalies.
