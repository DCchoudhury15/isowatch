# Limitations

IsoWatch follows one rule: **if it is not sure it can rewrite a query safely, it runs the query exactly as written and does not track it.** A wrong rewrite could make a query fail and abort your transaction, which would be worse than missing an anomaly.

Because of that rule, everything below can make IsoWatch **miss** an anomaly. None of it can make IsoWatch report one that did not happen, because every link in the graph comes from versions PostgreSQL itself reported.

`mon.Stats()` shows how many statements were tracked and why the others were not.

## Requirements

* **PostgreSQL 18 or newer.** IsoWatch needs `RETURNING old.xmin`, which arrived in PostgreSQL 18. `capture.New` returns an error on older servers.
* **Go with pgx v5.** Other drivers are not supported yet.
* **Watched tables must be plain tables with a primary key.** Views and partitioned tables are refused at startup.

## Statements that are run unchanged (not tracked)

| Statement | Why |
|---|---|
| `SELECT` with aggregates (`count`, `sum`, ...), `GROUP BY`, `HAVING`, `DISTINCT`, window functions | Adding a plain column would break the query |
| `SELECT` with `WITH`, `UNION`, `INTERSECT`, `EXCEPT`, `VALUES`, `SELECT INTO` | Table names may refer to something else; kept simple on purpose |
| Watched tables inside subqueries, or behind a join that has its own alias | Only tables directly in `FROM` (including plain joins) are tracked |
| `MERGE` | Not supported yet |
| Rows read by `INSERT ... SELECT` or by `UPDATE ... FROM` | The write is tracked, the extra read is not |
| Several statements in one string, `SendBatch`, `CopyFrom` | Not supported yet |
| Statements sent through `tx.Raw()` | By design |
| A table alias named `old` or `new` on a write | It would clash with `RETURNING old.x` |
| Queries that use custom result formats (`pgx.QueryResultFormats`) | The formats would not cover the hidden columns |
| Unqualified table names that resolve to a different table on your `search_path` | Only names that resolve to the watched table are matched; otherwise use `schema.table` |

## Anomalies it cannot see yet

* **Predicate reads.** "Count the doctors on call" with `SELECT count(*)` is not tracked. Selecting the rows themselves (`SELECT id FROM doctors WHERE on_call`) is tracked, and that is enough for the classic write skew. A row **inserted** by someone else that would have matched your `WHERE` (a phantom) is not seen.
* **Anomalies across separate transactions** of one request are only found with `group_by: trace` in the collector, or `checker.ByTrace` in your app. By default each database transaction is checked on its own.
* **Very long transactions.** Transactions are forgotten after the window (2 minutes by default). A cycle with a transaction older than that is missed. Set the window longer than your longest transaction.
* **The first occurrence.** IsoWatch has to see an anomaly before it can report it or protect against it, so the first one always happens. Run it in staging and load tests too, so most problems are found before production.

## Protection

* It only protects **named** transactions (`capture.WithName`), and only the names that took part in an anomaly IsoWatch saw. Other code that could race is not protected.
* It only works when checking inside the app (`CheckerSink`). The collector cannot change how your app runs.
* Only `Do` retries. If you use `Begin` with a protected name, the transaction runs at SERIALIZABLE but you must handle error 40001 yourself.
* Retried transactions run your function again. Do not send emails, charge cards or call other services inside it before the commit.
* On very hot rows, protection means waiting and retries. In the tests (16 workers on one row) it was about 1 to 2 retries per transaction.
* Protection lives in memory. After a restart, call `mon.Protect(...)` again (for example from a config file) for names you know need it.

## Things to know

* **"Anomaly" means the history was not serializable.** Some apps accept that on purpose (for example "last write wins" for a profile picture). The alert is still correct, but you may decide it is fine.
* **Apps that lock with their own mechanism** (Redis locks, advisory locks, version checks in code) may look anomalous at the SQL level even when the app is safe. Research shows this is common. Use `IgnoreNames` or `IgnoreTables` for those.
* **Read-only checks of your own writes** are ignored, which is correct: reading your own write is not a dependency.
* **The collector logs** only the first few identical messages per second by default. The example config turns that off. The `isowatch.anomaly` spans are never sampled.
* **Overhead.** Each tracked statement returns a few extra small text columns. In local tests this added about 20 to 55 microseconds per transaction. Measure it in your own setup.
