# IsoWatch

**IsoWatch tells you when concurrent requests silently corrupt your data in PostgreSQL, and exactly which requests did it.**

## The problem in one example

Two people deposit money into the same account at the same moment:

```
Request A (deposit +50)            Request B (deposit +30)
  SELECT balance  -> 100             SELECT balance  -> 100
  UPDATE balance = 150
  COMMIT                             UPDATE balance = 130
                                     COMMIT
```

The balance ends at **130**. It should be **180**. One deposit is gone.

PostgreSQL reports no error. Both requests return success. Your logs and your monitoring show nothing wrong. This is called a **lost update**, and it is allowed by PostgreSQL's default isolation level (READ COMMITTED). Its cousin, **write skew**, is how two doctors can both go off call at once and leave nobody on call.

These bugs have bankrupted a bitcoin exchange (Flexcoin), created free money on gift cards (Starbucks), and appear in most web apps that researchers check. Today nothing tells you when one happens in production.

## What IsoWatch does

IsoWatch watches the transactions your app runs and raises an alert when one of these anomalies **actually happens**:

```
ANOMALY lost-update: Lost update on public.accounts(1): a transaction wrote this row
based on an old read and replaced 1 newer version(s) it never saw.
    deposit (txn 75d0a9c6) in trace 77c63e5e read public.accounts(1) version 3449: SELECT balance FROM accounts WHERE id = $1
    deposit (txn 95392cf0) in trace 3eed3a5c replaced public.accounts(1) version 3449: UPDATE accounts SET balance = $1 WHERE id = $2
    deposit (txn 95392cf0) in trace 3eed3a5c wrote public.accounts(1) version 3450: UPDATE accounts SET balance = $1 WHERE id = $2
    deposit (txn 75d0a9c6) in trace 77c63e5e replaced public.accounts(1) version 3450: UPDATE accounts SET balance = $1 WHERE id = $2
    fix: Lock the rows when you read them: add FOR UPDATE to "SELECT balance FROM accounts WHERE id = $1". Or write it as one
    relative update (for example SET balance = balance + $1) instead of reading and then writing. Or run these transactions
    at REPEATABLE READ or SERIALIZABLE and retry when PostgreSQL returns error 40001.
```

Each alert tells you:

* **What happened**, in plain English.
* **Exactly which requests** were involved (their trace ids), so you can open them in Jaeger, SigNoz, Grafana Tempo or any OpenTelemetry backend.
* **Which SQL statements** read and wrote the row.
* **How to fix it.**

It does not guess. It reports facts. And if you want, it can **protect** those transactions automatically from then on (see below).

## How it knows (the short version)

Every row version in PostgreSQL carries `xmin`, the id of the transaction that wrote it. IsoWatch quietly reads that number:

* When your app runs `SELECT`, IsoWatch adds the row's `xmin` to the query, so it knows **which version** you read.
* When your app runs `UPDATE`, `INSERT` or `DELETE`, IsoWatch adds `RETURNING old.xmin, new.xmin` (new in PostgreSQL 18), so it knows **which version** you replaced.

If request B read version 100 but replaced version 150, B overwrote a write it never saw. That is proof of a lost update, not a guess. IsoWatch builds a graph of these facts and looks for cycles, using the standard model from database research (Adya's thesis, the same model the Jepsen tool Elle uses).

The extra columns are hidden. Your code gets exactly the rows and columns it asked for.

Read more: [How it works](docs/how-it-works.md)

## See it in 2 minutes (Docker only)

```bash
make demo-up                 # PostgreSQL 18, Jaeger, the IsoWatch collector and a demo bank
make demo-load               # 200 concurrent deposits through the buggy code
make demo-load MODE=-safe    # the same through the fixed code
make demo-load MODE=-auto    # the buggy code, protected by IsoWatch once it sees a problem
```

Then open **http://localhost:16686** (Jaeger), pick the service `bank-demo` and click **Find Traces**. Requests with an anomaly are marked red. Inside each one is an `isowatch.anomaly` span with the kind, the SQL, the fix, and links to the other requests involved. `make demo-down` stops everything.

## Quick start without Docker for the app (5 minutes)

You need Go 1.26+ and Docker (for PostgreSQL).

```bash
# 1. Start PostgreSQL 18
make db

# 2. Run the demo service (a tiny bank with the bug above)
cd examples/bank
go run . serve

# 3. In another terminal, send 200 concurrent deposits
go run . load
```

You will see something like:

```
200 deposits of 1 succeeded in 2.134s
  balance should be: 200
  balance is:        17
  deposits lost:     183

doctors still on call: [] (should never be empty)

IsoWatch saw: map[lost-update:199 write-skew:1]
IsoWatch counted lost writes: 183
```

Now run the fixed version of the same endpoints:

```bash
go run . load -safe
```

```
  deposits lost:     0
doctors still on call: [alice]
IsoWatch saw: map[]
```

The database lost 183 deposits, and IsoWatch counted exactly 183. The fixed code loses nothing, and IsoWatch stays quiet.

## Using it in your app

```go
import "github.com/DCchoudhury15/isowatch/capture"

mon, err := capture.New(ctx, pool, capture.Config{
    Tables: []string{"accounts", "orders"},      // tables to watch
    Sink:   capture.NewOTelSink(nil),            // send to the collector as spans
})

// Instead of pool.Begin(ctx):
tx, err := mon.Begin(ctx, pool, pgx.TxOptions{})
defer tx.Rollback(ctx)

var balance int
err = tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1", id).Scan(&balance)
_, err = tx.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", balance+amount, id)
err = tx.Commit(ctx)
```

`tx` has the same `Exec`, `Query` and `QueryRow` methods as a pgx transaction, so most code changes only where the transaction starts.

There is also `Do`, which commits for you, and you can **name** each kind of transaction:

```go
err := mon.Do(ctx, pool, pgx.TxOptions{}, func(tx *capture.Tx) error {
    // read and write as usual
    return nil
}, capture.WithName("deposit"))
```

Names appear in alerts, can be ignored, and are what protection works on.

**Two ways to check:**

| Mode | How | Good for |
|---|---|---|
| In your app | `capture.NewCheckerSink(...)` | Tests, staging, small services |
| In the collector | `capture.NewOTelSink(nil)` + the IsoWatch collector | Production, many services, alerts in your tracing tool |

## From finding to fixing: protection

PostgreSQL's SERIALIZABLE level prevents all of these anomalies, but it costs retries, so most apps do not use it everywhere. IsoWatch can apply it **only where it has seen a problem**:

```go
var mon *capture.Monitor
sink := capture.NewCheckerSink(checker.Options{}, func(a checker.Anomaly) {
    mon.ProtectFrom(a) // the named transactions in this anomaly now run at SERIALIZABLE
})
mon, _ = capture.New(ctx, pool, capture.Config{Tables: []string{"accounts"}, Sink: sink})
```

After that, `Do` runs those named transactions at SERIALIZABLE and retries them when PostgreSQL asks (error 40001). Everything else keeps running at full speed. You can also call `mon.Protect("deposit")` yourself.

In the tests, the same buggy counter lost 16 increments before IsoWatch protected it, then **0 out of the next 640**. Be aware: the first anomaly still happens (IsoWatch has to see it), retries cost time on very hot rows, and `fn` may run more than once, so it must not send emails or call other services before it commits.

## Ignoring what you accept

Some races are fine for you (for example "last seen" timestamps). Ignore them by table or by transaction name:

```go
checker.Options{IgnoreTables: []string{"sessions"}, IgnoreNames: []string{"touch-last-seen"}}
```

The collector has the same settings: `ignore_tables` and `ignore_names`.

## The collector

`processor/isowatchprocessor` is an OpenTelemetry Collector processor. It reads the transaction spans, finds anomalies, and adds an `isowatch.anomaly` span to the trace of the request that completed each one, linked to the other requests.

```bash
make collector                                   # builds collector/build/isowatch-collector
./collector/build/isowatch-collector --config collector/config.yaml
cd examples/bank && go run . serve -mode otlp    # the demo, sending spans to it
```

## Results

All of these were measured against a real PostgreSQL 18. See [Results](docs/results.md) for details and how to repeat them.

| Check | Result |
|---|---|
| Lost writes counted vs. actually lost (16 workers, 640 increments, 5 runs) | **Exact match every run** (582, 582, 594, 596, 592) |
| False alarms with `SELECT ... FOR UPDATE` | **0** |
| False alarms at SERIALIZABLE (random transfers, every run) | **0** |
| Write skew caught at READ COMMITTED and REPEATABLE READ | **Yes** |
| Protection: increments lost after IsoWatch protected the transaction | **0 of 640** |
| Extra latency per transaction (read + update, local Docker) | **About 20 to 55 microseconds** |
| Checker speed (one CPU core) | **About 300,000 transactions per second** |

## What it can and cannot see

IsoWatch is careful: when it is not sure it can rewrite a query safely, it runs the query unchanged and simply does not track it. That can cause a missed anomaly, but never a false alarm and never a broken query.

Simple aggregates over one table, like `SELECT count(*) FROM doctors WHERE on_call`, are tracked. It does not yet track aggregates over joins, `GROUP BY`, `DISTINCT`, `UNION`, `WITH` queries, subqueries, `MERGE`, batches or `COPY`. It needs PostgreSQL 18 or newer and the pgx v5 driver. Read the full list: [Limitations](docs/limitations.md)

## Is this new?

Checking for isolation anomalies is a well studied research area. IsoWatch does not invent a new checking algorithm. What it adds, as far as our research found, is doing it **continuously, on real production traffic, for normal applications, using exact row versions, with every alert linked to request traces**. Existing tools work offline on captured logs, predict anomalies that might happen, or test database engines rather than applications.

* [Who has built what](docs/prior-art.md)
* [Is this a real problem?](docs/market-validation.md)
* [How people fix it today, and where IsoWatch fits](docs/solutions-landscape.md)

## Project layout

| Folder | What is in it |
|---|---|
| `history/` | The record of one transaction: rows read, rows written, versions |
| `checker/` | Builds the dependency graph and finds anomalies (no outside dependencies) |
| `capture/` | The pgx wrapper your app uses |
| `capture/rewrite/` | Adds the hidden version columns to SQL |
| `processor/isowatchprocessor/` | The OpenTelemetry Collector processor |
| `collector/` | Config to build and run the collector |
| `examples/bank/` | The demo service and load generator |
| `docker/`, `docker-compose.yml` | Run everything in Docker |
| `.github/workflows/` | Tests on every push, with a real PostgreSQL 18 |
| `docs/` | How it works, limitations, results, research |

## Running the tests

```bash
make test       # unit tests, no database needed
make db         # start PostgreSQL 18 in Docker
make test-db    # everything, including the real database tests
```

## License

Apache 2.0. See [LICENSE](LICENSE).
