# The IsoWatch proxy: any language, no code change

The Go library needs your app to use pgx. Most apps with these bugs are written in Ruby, Python, Java or Node. For them, IsoWatch has a proxy: a small server that speaks the PostgreSQL protocol. Your app connects to the proxy instead of to PostgreSQL. The only change is the connection string.

```text
app (any language)  --->  isowatch-proxy  --->  PostgreSQL 18
                               |
                               +--> anomalies in its log, or isowatch.txn spans to the IsoWatch collector
```

The proxy does what the Go library does: it rewrites statements on watched tables to also return the row versions (`xmin`), removes those extra columns before your app sees the results, and checks or sends each committed transaction. Your app gets exactly the same bytes it would get from PostgreSQL. The tests check that, message by message.

## Run it

Build it (needs Go 1.26 and a C compiler, because it uses the PostgreSQL SQL parser):

```bash
make proxy                     # build/isowatch-proxy
# or
go install github.com/DCchoudhury15/isowatch/cmd/isowatch-proxy@latest
```

Start it next to your database:

```bash
export PGPASSWORD=...          # the password of the -dsn user, read by the driver
isowatch-proxy \
  -listen :6432 \
  -upstream db.internal:5432 \
  -dsn 'postgres://isowatch@db.internal:5432/app' \
  -tables accounts,orders \
  -http :9187
```

Then point your app at the proxy, for example `postgres://app@proxy-host:6432/app` with the same password as before. Users, passwords and SCRAM authentication pass through unchanged: the proxy never sees or needs your app's password.

| Flag | Environment variable | What it does |
|---|---|---|
| `-listen` | `ISOWATCH_LISTEN` | Where apps connect. Default `:6432`. |
| `-upstream` | `ISOWATCH_UPSTREAM` | Your PostgreSQL server, `host:port`. Required. |
| `-dsn` | `ISOWATCH_DSN` | A connection string the proxy uses once at start, to read the watched tables and the default isolation level. Required. |
| `-tables` | `ISOWATCH_TABLES` | Tables to watch, comma separated: `accounts,billing.ledger`. |
| `-watch-all` | `ISOWATCH_WATCH_ALL` | Watch every table with a primary key instead. |
| `-exclude` | `ISOWATCH_EXCLUDE` | Never watch these: `schema.table`, `table` or `schema.*`. |
| `-sink` | `ISOWATCH_SINK` | `log` (default): check inside the proxy and log each anomaly. `otlp`: send every transaction to an [IsoWatch collector](collector.md). |
| `-otlp-endpoint` | `ISOWATCH_OTLP_ENDPOINT` | The collector's OTLP/HTTP `host:port`, for `-sink otlp`. |
| `-otlp-insecure` | `ISOWATCH_OTLP_INSECURE` | Plain HTTP to the collector. |
| `-group-by` | `ISOWATCH_GROUP_BY` | For `-sink log`: `transaction` (default) or `trace`. |
| `-keys` | `ISOWATCH_KEYS` | `hash` (default) stores a hash of primary keys, `raw` the values. |
| `-http` | `ISOWATCH_HTTP` | Serves `/stats` and `/coverage` (see below). Off when empty. |

With `-sink log`, one proxy checks the transactions of every app that connects to it. If you run several proxies, or also use the Go library somewhere, use `-sink otlp` and one collector, so one checker sees everything.

Stop it with SIGTERM: it stops accepting connections and waits up to 10 seconds for open ones to end.

## Link anomalies to your traces and name your transactions

The proxy reads [sqlcommenter](https://google.github.io/sqlcommenter/) comments from your SQL:

```sql
SELECT balance FROM accounts WHERE id = $1 /*traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',route='%2Fdeposit'*/
```

* `traceparent` puts the transaction into your request's trace, so anomaly spans link to your requests, and `group_by: trace` works.
* The transaction's **name** comes from `isowatch_name`, else `route`, else `controller` and `action` (as `controller#action`), else `controller` or `action` alone. Names are what you see in alerts, what `ignore_names` matches, and what protection works on. Use one name per kind of transaction, not per request.
* Both the sqlcommenter form `key='value'` and the `key:value` form of Rails' query log tags are read.
* Comments are removed before a statement is planned and put back after it is rewritten, so they still reach your database logs. They are never stored in the transaction history.

How to turn the comments on:

| Stack | How |
|---|---|
| Rails 7+ | `config.active_record.query_log_tags_enabled = true`, with tags like `:controller, :action`. For trace ids, add a tag for the current `traceparent`. |
| Django | The [sqlcommenter middleware for Django](https://google.github.io/sqlcommenter/python/django/), or the OpenTelemetry instrumentation for Django or psycopg with its SQL commenter option turned on. |
| Spring / Hibernate | The [sqlcommenter for Java](https://google.github.io/sqlcommenter/java/). |
| Node | [`@opentelemetry/instrumentation-pg`](https://www.npmjs.com/package/@opentelemetry/instrumentation-pg) with `addSqlCommenterCommentToQueries: true`, or the sqlcommenter packages for Knex and Sequelize. |
| Go | Use the Go library instead of the proxy, or any sqlcommenter for database/sql. |

Without comments, everything still works. Transactions just have no name and no trace.

## What it follows

* **Both protocols.** The simple query protocol (psql, many drivers for plain statements) and the extended protocol (prepared statements, with named and unnamed statements and portals, `Describe`, row limits, binary and text result formats, and pipelining).
* **Transactions.** Statements inside `BEGIN` ... `COMMIT`, statements on their own (each is its own transaction), and pipelines without `BEGIN` (one implicit transaction up to `Sync`). A transaction is reported only when PostgreSQL confirmed the commit: a `ROLLBACK`, a failed `COMMIT`, an error in a statement without `BEGIN`, and `PREPARE TRANSACTION` are not reported. `ROLLBACK TO SAVEPOINT` forgets the writes made after the savepoint.
* **Isolation level.** From `BEGIN ISOLATION LEVEL ...`, `SET TRANSACTION`, `SET default_transaction_isolation`, `SET SESSION CHARACTERISTICS`, the connection's startup parameters (`options=-c default_transaction_isolation=...`), and otherwise the database default read with `-dsn`.
* **search_path.** PostgreSQL 18 tells the client its `search_path` whenever it changes, and the proxy listens. On a connection whose `search_path` is not the one the proxy read the tables with, a table name without a schema may mean another table (for example one schema per tenant). There, only statements that name the schema (`public.accounts`) are tracked. Prepared statements made earlier are prepared again for the new path, without the app noticing.
* **Cancel requests** are passed on to PostgreSQL.
* **Prepared statements** closed with `Close`, `DEALLOCATE` or `DISCARD ALL`.

## What is not supported yet

* **TLS between the app and the proxy.** The proxy answers "no" to the TLS request, so a client with `sslmode=prefer` (the default of libpq and pgx) continues without TLS, and one with `sslmode=require` fails to connect. Run the proxy next to your app or database on a private network. The proxy connects to PostgreSQL without TLS too.
* **Connection pooling.** Each app connection gets its own PostgreSQL connection. Put the proxy between your app and PgBouncer, or between PgBouncer and PostgreSQL.
* **Several statements in one string** (like `UPDATE a ...; UPDATE b ...`) are forwarded untracked, and the transaction they run in is not reported, because the proxy cannot follow it statement by statement. Some drivers send batches this way, for example pgx in simple protocol mode.
* **Protection.** The proxy does not switch transactions to SERIALIZABLE or retry them. The Go library and the collector do. Use the anomaly reports to fix the code.
* **Settings changed in ways the proxy cannot see**, like `set_config('default_transaction_isolation', ...)` inside a query, or a default set per role or database with `ALTER ROLE ... SET`. The isolation level shown in a report may then be wrong. Anomalies are still real; only the "unexpected" flag can be wrong.
* **Replication connections and COPY data.** `COPY` works, but is not tracked.
* Everything in [Limitations](limitations.md) about which statements are tracked applies to the proxy too.

## Is it safe to put in front of my database?

The proxy follows the IsoWatch rule: when it is not sure, it forwards the message unchanged and does not track it. It removes a column only when the column is one of its own (named `__isowatch_N`) at the end of a row description. If a reply from PostgreSQL ever does not match what the proxy expects, it closes that connection rather than send your app wrong data, logs an error, and counts it in `/stats` as `Desyncs` (it should stay 0). The tests run every scenario above in all five pgx query modes, compare every byte of every reply with a direct connection, and fail if any connection was closed this way.

It adds one network hop and some parsing. Rewritten statements are cached by their SQL text.

## Check that it works

`/stats` shows the counters (shortened):

```json
{"capture":{"Committed":200,"Tracked":400,"Untracked":200,"Reasons":{"no watched table":200}},
 "lost_writes":136,
 "proxy":{"Connections":200,"Active":0,"Desyncs":0}}
```

This is real output from 20 `psql` sessions running a read-then-write deposit 10 times each through the proxy. The database lost 136 of the 200 deposits, and IsoWatch counted 136 lost writes. (`lost_writes` is only there with `-sink log`.)

`/coverage` shows how many statements on watched tables were tracked, and lists the ones that were not, with the reason (`?format=text` for a readable report). See [Adding IsoWatch to an existing app](adopting.md#4-check-what-isowatch-can-see).
