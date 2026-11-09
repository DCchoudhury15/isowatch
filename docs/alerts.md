# Metrics and alerts

The IsoWatch processor adds `isowatch_*` metrics to the collector's own metrics endpoint. By default that is `http://localhost:8888/metrics`. You set the address under `service.telemetry.metrics` in the collector config.

Point Prometheus at it and load [`collector/alerts.yaml`](../collector/alerts.yaml) to get alerts.

## The metrics

Real output from the bank demo after 200 concurrent deposits, of which 183 were lost:

```text
isowatch_anomalies{kind="lost-update",table="public.accounts",txn_name="deposit"} 199
isowatch_anomalies{kind="write-skew",table="public.doctors",txn_name="leave"} 1
isowatch_issues 2
isowatch_lost_writes{table="several"} 183
isowatch_transactions 202
isowatch_window_transactions 202
```

(The collector also adds `otel_scope_*` labels, left out here.)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `isowatch_anomalies` | counter | `kind`, `table`, `txn_name` | Anomalies found. Counts every occurrence, also with `report: per_issue`. |
| `isowatch_lost_writes` | counter | `table` | Writes overwritten by a transaction that never saw them. In the demo this equals the lost money exactly. |
| `isowatch_transactions` | counter | | Transactions received and checked. |
| `isowatch_decode_errors` | counter | | `isowatch.txn` spans that could not be read. |
| `isowatch_notifications_dropped` | counter | `target` (`webhook` or `protection`) | Messages dropped because the queue was full. |
| `isowatch_issues` | gauge | | Issues kept in memory (at most `max_issues`). |
| `isowatch_window_transactions` | gauge | | Transactions kept in the checking window (requests, with `group_by: trace`). This is what uses memory. |

The names have **no `_total` suffix** with the collector's built-in endpoint (checked with collector v0.162). If you export the metrics another way, check your `/metrics` page for the exact names.

Some notes on the labels:

- `table` and `txn_name` list the tables and transaction names in the anomaly, sorted and joined with `,`. A transaction without a name shows as `unnamed`.
- To keep the number of series small, `isowatch_anomalies` keeps at most 500 label combinations. More than that (for example if an app puts ids into transaction names) are counted with `table="other"` and `txn_name="other"`.
- `isowatch_lost_writes` gets the table name only when all the issues seen in that interval were on one table. Otherwise it says `several`, like in the demo above, where a write skew on `doctors` happened at the same time. The total is always right, so alert on `sum(...)`.

## Alerts

[`collector/alerts.yaml`](../collector/alerts.yaml) has five rules:

| Alert | Fires when | Severity |
|---|---|---|
| `IsoWatchAnomaly` | An anomaly happened in the last 10 minutes. One alert per kind, tables and names. | warning |
| `IsoWatchLostWrites` | Writes were lost in the last 15 minutes. | critical |
| `IsoWatchSeesNoTransactions` | No transaction arrived for 15 minutes, or the metric does not exist. IsoWatch is blind. | warning |
| `IsoWatchDecodeErrors` | Spans could not be read. | warning |
| `IsoWatchNotificationsDropped` | The webhook or the protection database could not keep up. | warning |

The main one:

```yaml
- alert: IsoWatchAnomaly
  expr: sum by (kind, table, txn_name) (increase(isowatch_anomalies[10m])) > 0
  labels:
    severity: warning
  annotations:
    summary: "{{ $labels.kind }} on {{ $labels.table }} ({{ $labels.txn_name }})"
```

An alert tells you **that** it happened. The `isowatch.anomaly` span in your tracing backend tells you **what** happened: search for spans named `isowatch.anomaly` with that `isowatch.anomaly.kind`. They hold the SQL steps, the requests involved and the fix.

`IsoWatchSeesNoTransactions` matters more than it looks. If an app stops sending its IsoWatch spans (a config change, a sampler put before the processor), IsoWatch finds nothing, and "no anomalies" looks like good news. Leave it on.

The rules are tested with `promtool` (`collector/alerts_test.yaml`, run with `make test-alerts`): each alert fires on the input it is meant for and stays quiet otherwise.

## Without Prometheus

- **Webhook:** `webhook.url` in the processor config posts each new issue to Slack or any HTTP endpoint. See [the collector](collector.md#webhook).
- **Your tracing backend:** many backends (Grafana Tempo with TraceQL, SigNoz, Honeycomb, Datadog) can alert on spans. Alert on spans named `isowatch.anomaly` with `isowatch.issue.new = true` to get one alert per new issue.
