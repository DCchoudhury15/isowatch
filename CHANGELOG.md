# Changelog

All notable changes to IsoWatch are listed here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0.0, a minor version (0.x.0) may change the Go API.

The root module (`github.com/DCchoudhury15/isowatch`), the processor module
(`github.com/DCchoudhury15/isowatch/processor/isowatchprocessor`) and the
collector image are released together with the same version. See
[RELEASING.md](RELEASING.md).

## [Unreleased]

### Added

#### Library (`github.com/DCchoudhury15/isowatch`)

- `capture`: a wrapper around pgx v5 transactions (`Monitor.Begin`, `Monitor.Do`,
  `WithName`). `Tx` has the same `Exec`, `Query` and `QueryRow` methods as a pgx
  transaction.
- `capture`: `Monitor.Wrap(pool)` returns a `DB` with the pool's `Exec`, `Query`,
  `QueryRow`, `Begin`, `BeginTx`, `SendBatch` and `CopyFrom`, so an existing app
  changes one line. Statements outside a transaction are recorded as
  one-statement transactions. `WithNameContext` names everything a request runs.
- `capture`: `Config.WatchAll` watches every table with a primary key, and
  `ExcludeTables` leaves some out.
- `capture`: the coverage report (`Coverage`, `CoverageReport`, `Handler`,
  `ReportEvery`) shows how many statements on watched tables were tracked, and
  lists the ones that were not, with the reason.
- `capture/rewrite`: adds the row's `xmin` to `SELECT` on watched tables, and
  `RETURNING old.xmin, new.xmin` (PostgreSQL 18) to `UPDATE`, `INSERT` and
  `DELETE`. The extra columns are hidden from your code. Queries it cannot
  rewrite safely run unchanged and are not tracked.
- Simple aggregate reads over one watched table, like
  `SELECT count(*) FROM doctors WHERE on_call`, are tracked, so write skew
  through a count is found.
- `history`: the record of one transaction (rows read, rows written, row
  versions, isolation level, name, trace id).
- `checker`: builds the dependency graph from exact row versions and finds
  cycles. Reports `lost-update`, `write-skew`, `G-single`, `G2-item`, `G1c`
  and `G0`, each with the SQL involved, a plain English summary and a fix.
  Anomalies at an isolation level that should prevent them are marked
  unexpected. Can group by transaction or by trace. Grouped by trace, lost
  writes are counted exactly for a request that reads in one transaction and
  writes in a later one.
- Ignoring accepted races by table or by transaction name (`IgnoreTables`,
  `IgnoreNames`).
- Sinks: `NewCheckerSink` checks inside the app, `NewOTelSink` sends
  `isowatch.txn` spans through the app's tracer provider, `MultiSink` does
  both. `NewOTLPSink` and `NewExporterSink` send every transaction to the
  collector whatever the app samples, and drop spans instead of slowing the
  app when the collector is slow.
- Protection: `Monitor.ProtectFrom(anomaly)` and `Monitor.Protect(name)` run the
  named transactions at SERIALIZABLE from then on, and `Do` retries them on
  error 40001.
- `protection`: a shared list of protected names in a PostgreSQL table
  (`protection.NewPostgres`). With `Config.ProtectionStore`, a name protected
  on one replica reaches every replica within `ProtectionPoll` (5s by
  default). A store that is down never breaks the app.
- `isowatchtest`: fails `go test` with a readable report when IsoWatch sees an
  anomaly, unless the test expects it (`ExpectAnomaly`). `Concurrently` runs a
  function many times at once so races really happen.

#### Collector processor (`processor/isowatchprocessor`)

- An OpenTelemetry Collector processor that checks `isowatch.txn` spans and
  adds an `isowatch.anomaly` span, linked to the other requests, to the trace
  of the request that completed each anomaly. Settings: `window`, `group_by`,
  `max_depth`, `evict_interval`, `drop_txn_spans`, `ignore_tables`,
  `ignore_names`.
- Issues: one bug that happens many times is one issue, with one anomaly span
  and then a running count (`report`, `issue_interval`, `max_issues`).
- Metrics on the collector's own endpoint: `isowatch_anomalies`,
  `isowatch_lost_writes`, `isowatch_transactions`, `isowatch_decode_errors`,
  `isowatch_notifications_dropped`, `isowatch_issues` and
  `isowatch_window_transactions`.
- `webhook`: posts each new issue as JSON. Slack and similar chat webhooks
  work as is. The URL is never logged.
- `protection`: writes the names in each anomaly to the same table the apps
  read, so the collector protects transactions for every replica.
- Prometheus alert rules in `collector/alerts.yaml`, with tests
  (`make test-alerts`).

#### Proxy (`cmd/isowatch-proxy`)

- A PostgreSQL wire protocol proxy, so apps in any language can use IsoWatch
  by changing only their connection string. It follows the simple and the
  extended protocol (prepared statements, portals, row limits, binary
  results, pipelining), transactions with and without `BEGIN`, savepoints,
  isolation levels and `search_path`, and passes authentication and cancel
  requests through. Replies reach the app byte for byte as PostgreSQL sent
  them. sqlcommenter comments (`traceparent`, `route`, `controller`,
  `action`, `isowatch_name`) link transactions to traces and name them.
  Anomalies go to its log, or every transaction to the collector
  (`-sink otlp`). `/stats` and `/coverage` show what it saw.
- `capture.Recorder` (with `Monitor.Plan` and `Monitor.CountUntracked`)
  records transactions for programs that run the SQL themselves, like the
  proxy.

#### Running it

- The collector image `ghcr.io/dcchoudhury15/isowatch-collector` for
  linux/amd64 and linux/arm64. It runs as a non-root user, serves a health
  check on port 13133 and the metrics on port 8888.
- Collector binaries for linux and darwin (amd64, arm64) on each GitHub
  release.
- `examples/bank`: a demo service with a lost update and a write skew bug,
  fixed versions of both, a load generator, and `go test` examples.
- Docker Compose demo with PostgreSQL 18, Jaeger and the collector.
- CI with real PostgreSQL 18 tests, and a release workflow.
- Docs: [installing](docs/install.md), [the proxy](docs/proxy.md), [adding IsoWatch to an existing app](docs/adopting.md),
  [testing](docs/testing.md), [production](docs/production.md),
  [the collector](docs/collector.md), [metrics and alerts](docs/alerts.md).

[Unreleased]: https://github.com/DCchoudhury15/isowatch/commits/main
