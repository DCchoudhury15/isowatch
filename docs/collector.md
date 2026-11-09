# Running the IsoWatch collector

The IsoWatch processor runs inside an OpenTelemetry Collector. It reads the `isowatch.txn` spans your apps send, finds anomalies, and adds an `isowatch.anomaly` span to the trace of the request that completed each one. It can also count them as metrics, post new ones to a webhook, and protect the transactions involved.

## Build it

```bash
make collector                              # collector/build/isowatch-collector
docker build -f docker/collector.Dockerfile -t isowatch-collector .
```

`collector/config.yaml` is a commented example with every option. `docker-compose.yml` runs it with the demo app.

## Run one checker, see every transaction

IsoWatch finds an anomaly by joining transactions from **different requests**, often from different replicas of your app. So one processor has to see every `isowatch.txn` span for the tables it checks:

- **Run the IsoWatch collector as one instance.** Several replicas would each see part of the traffic and miss the anomalies between them. Load balancing by trace id does not help, because the two sides of an anomaly are different traces.
- **Point every app at it.** The simplest setup is a separate IsoWatch collector next to the collectors you already have. Apps send their IsoWatch spans straight to it with `capture.NewOTLPSink` (see [Running in production](production.md)), and it sends the anomaly spans on to your tracing backend.
- **Put `isowatch` first in the pipeline**, before `batch`, filters and any sampling processor, so nothing is dropped before it is checked.

```yaml
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [isowatch, batch]
      exporters: [otlp/backend]
```

The checker keeps recent transactions in memory for `window`. Memory grows with your transaction rate times the window: watch `isowatch_window_transactions` (see [Alerts](alerts.md)). A restart forgets the window, so an anomaly that spans the restart is missed.

## Options

| Option | Default | What it does |
|---|---|---|
| `window` | `2m` | How long transactions are kept. Must be longer than your longest transaction. |
| `group_by` | `transaction` | `trace` also finds anomalies across one whole request, for example a read and a write in separate statements. |
| `max_depth` | `16` | Longest cycle to look for. |
| `evict_interval` | `5s` | How often old transactions are forgotten and lost writes are counted. |
| `drop_txn_spans` | `true` | Remove the `isowatch.txn` spans after checking. Set `false` to keep them in your backend for debugging. |
| `ignore_tables` | none | Anomalies only on these tables are acceptable. |
| `ignore_names` | none | Transactions with these names may race. |
| `report` | `per_issue` | `per_issue` or `every`. See below. |
| `issue_interval` | `1m` | With `per_issue`, at most one more span per issue in this time. |
| `max_issues` | `1000` | Issues kept in memory. The least recently seen is forgotten first. |
| `webhook.url` | none | Post new issues here. Off when empty. |
| `webhook.timeout` | `5s` | Per request. |
| `webhook.repeat` | `false` | Also post the running count, at most once per `issue_interval`. |
| `protection.dsn` | none | Write protected names to this database. Off when empty. |
| `protection.table` | `isowatch_protected` | The table, also as `schema.table`. Lower case only, the same rule the apps use. |

Use `${env:NAME}` for secrets, like the webhook URL and the database password.

## Issues: one bug, one report

One bug in one endpoint can happen thousands of times an hour. The processor groups occurrences into **issues**. Two anomalies are the same issue when they have the same kind, the same tables, the same transaction names and the same SQL (with values and spacing removed).

With `report: per_issue` (the default), an issue gets an anomaly span the first time, then at most one more per `issue_interval` with the running count. With `report: every`, each occurrence gets its own span. **Metrics count every occurrence either way.**

The anomaly span carries these attributes:

| Attribute | Meaning |
|---|---|
| `isowatch.issue.id` | Stable id of the issue (16 hex characters) |
| `isowatch.issue.new` | True on the first span of an issue |
| `isowatch.issue.count` | Occurrences so far |
| `isowatch.issue.unreported` | Occurrences since the last span that got no span of their own (only when above 0) |
| `isowatch.issue.first_seen` | When the issue was first seen |
| `isowatch.issue.lost_writes` | Lost writes counted for this issue so far (only when above 0) |
| `isowatch.anomaly.kind`, `.summary`, `.fix`, `.names`, `.steps`, `.traces`, ... | This occurrence: what happened, which SQL, how to fix it, and the traces of the requests involved |

The processor reads the `isowatch.txn` spans of each batch, so a batch that held only those spans sends nothing on.

## Webhook

```yaml
webhook:
  url: ${env:ISOWATCH_WEBHOOK_URL}
```

The processor posts one JSON message per new issue. It has a `text` field, so a Slack, Mattermost or Rocket.Chat incoming webhook works as is. The other fields are for programs:

```json
{
  "text": "...",
  "event": "new_issue",
  "issue": {"id": "cf503ce7e34c128b", "kind": "lost-update", "tables": ["public.accounts"],
            "names": ["deposit"], "statements": ["..."], "first_seen": "...", "last_seen": "...",
            "count": 1, "lost_writes": 0},
  "anomaly": {"kind": "lost-update", "unexpected": false, "summary": "...", "fix": "...",
              "traces": ["..."], "names": ["deposit"]}
}
```

`event` is `new_issue`, or `repeat` for the counts sent with `repeat: true`.

Posting never slows the pipeline. Messages wait in a queue of 100. If the webhook is down long enough to fill it, new messages are dropped and counted in `isowatch_notifications_dropped{target="webhook"}`. Slack webhook URLs contain a secret, so the URL is never written to the logs.

## Protection from the collector

```yaml
protection:
  dsn: ${env:ISOWATCH_PROTECTION_DSN}
  table: isowatch_protected
```

The collector sees the traffic of every replica, so it is the best place to decide what to protect. For each anomaly it writes the names of the transactions involved to the protection table. Apps that read the same table (`Config.ProtectionStore`, see [Running in production](production.md#2-share-protection-across-replicas)) then run those names at SERIALIZABLE and retry them. Both sides use the same `protection` package, so they agree on the table.

- Each name is written once per collector process. If you delete it from the table (because you fixed the code), the same bug happening again does not put it back until the collector restarts.
- A failed write is forgotten, so the next occurrence tries again. A database that is down never slows the pipeline: writes wait in a queue of 256, and dropped ones are counted in `isowatch_notifications_dropped{target="protection"}`.
- Only anomalies where every transaction has a name are acted on. G0 and G1c (which PostgreSQL should never allow) and anomalies that already ran at a level that forbids them are not, because SERIALIZABLE would not help.
- The database user needs `INSERT` on the table, and `CREATE` if the table does not exist yet.

The reason column says what happened, for example:

```text
isowatch: lost-update on public.accounts (issue cf503ce7e34c128b, trace c5590a0cc81ef1ee2939d6efdbfb734a)
```

## Metrics

The processor adds `isowatch_*` metrics to the collector's own metrics endpoint. See [Alerts](alerts.md) for the list and ready-made alert rules.
