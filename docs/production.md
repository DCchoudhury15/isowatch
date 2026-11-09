# Running IsoWatch in production

In tests and staging, one process with `capture.NewCheckerSink` is enough. In production you usually have:

- **Trace sampling**, so most requests never reach your tracing backend.
- **Many replicas** of the same app, each seeing only part of the traffic.

Both of these can hide anomalies. This page shows how to set IsoWatch up so they do not.

## 1. Send every transaction, even when you sample

`capture.NewOTelSink` sends each transaction as a span through your app's tracer provider. Your sampler then decides whether the span is kept. With head sampling at rate `p`, an anomaly between two transactions is only seen when **both** were kept: about `p * p` of the time. At 10% sampling that is 1 anomaly in 100.

Use `capture.NewOTLPSink` instead. It has its own tracer provider that keeps every `isowatch.txn` span, and sends them straight to the collector that runs the IsoWatch processor:

```go
sink, err := capture.NewOTLPSink(ctx, capture.OTLPOptions{
    Endpoint: "otel-collector:4318", // OTLP/HTTP receiver of the IsoWatch collector
    Insecure: true,                  // plain HTTP inside the cluster
    Gzip:     true,
})
if err != nil {
    return err
}
defer sink.Shutdown(context.Background()) // sends what is still queued

mon, err := capture.New(ctx, pool, capture.Config{
    Tables: []string{"accounts"},
    Sink:   sink,
})
```

Your app's own tracing does not change. The IsoWatch span still has your request's trace id and the request span as its parent, so an anomaly still links to the requests that caused it. If the request itself was not sampled, its trace in your backend holds only the IsoWatch spans.

| Option | Default | Notes |
|---|---|---|
| `Endpoint` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT`, then `localhost:4318` | host:port |
| `URLPath` | `/v1/traces` | |
| `Insecure` | false (HTTPS) | |
| `Headers` | none | for example an auth header your collector checks |
| `Gzip` | false | the history is repetitive JSON, so this saves a lot |
| `Timeout` | 10s | per export request |
| `MaxQueueSize` | 8192 | spans waiting to be sent |
| `BatchTimeout` | 1s | longest a span waits before it is sent |
| `Resource` | `resource.Default()` | reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` |

What it does on purpose:

- **It ignores `OTEL_TRACES_SAMPLER`.** A dropped span hides anomalies.
- **It ignores the attribute length limits** (`OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT` and friends). A cut history cannot be read by the collector.
- **It drops spans when the queue is full** (collector slow or down) instead of slowing your app. That can only hide an anomaly, never invent one.

To use another exporter (for example OTLP over gRPC), build it yourself and pass it to `capture.NewExporterSink(exporter, capture.ExporterOptions{...})`.

**Cost:** one span per committed transaction on a watched table. Watch only the tables where races matter.

## 2. Share protection across replicas

Protection (see the README) makes a transaction name run at SERIALIZABLE once IsoWatch has seen it race. By default the list of protected names lives in the memory of one process. So with many replicas:

- a replica only protects what **its own** checker saw, and
- a restart forgets everything.

Give the Monitor a shared store and every replica reads the same list:

```go
import "github.com/DCchoudhury15/isowatch/protection"

store, err := protection.NewPostgres(plainPool, "") // table isowatch_protected
if err != nil {
    return err
}
mon, err := capture.New(ctx, pool, capture.Config{
    Tables:          []string{"accounts"},
    Sink:            sink,
    ProtectionStore: store,
    Logger:          slog.Default(), // store problems are logged here
})
defer mon.Close() // writes names that are still pending
```

How it behaves:

- The Monitor loads the names when it starts and reads them again every `ProtectionPoll` (default 5s). A name protected on one replica reaches the others within one poll.
- `Protect`, `ProtectFrom` and `Unprotect` change the store **in the background**, never on the goroutine that called them.
- Names survive restarts, because they are in the database.
- **If the store cannot be reached**, nothing fails. The Monitor keeps the names it last knew, keeps retrying its writes, and logs the first failure and the recovery to `Config.Logger`. This also holds when the store is down at startup.
- Give the store a **plain pool**. IsoWatch refuses to watch the protection table itself (it is an error to list it in `Config.Tables`).

In the tests, two replicas share one table. After replica A sees a lost update, replica B runs the same name at SERIALIZABLE within one poll, and a counter run on both loses 0 of 640 increments.

### The table

```sql
CREATE TABLE IF NOT EXISTS isowatch_protected (
  name     text PRIMARY KEY,
  reason   text NOT NULL DEFAULT '',
  added_at timestamptz NOT NULL DEFAULT now()
);
```

The store creates it on the first `Add`. You can pass another name, also with a schema (`"isowatch.protected"`). Only lower case plain names are allowed, so the name means the same table with or without quotes.

The table is the whole contract. Anything that can write to it can protect a name for every replica, for example by hand:

```sql
INSERT INTO isowatch_protected (name, reason) VALUES ('deposit', 'protected by hand');
DELETE FROM isowatch_protected WHERE name = 'deposit';  -- back to normal
```

Only **named** transactions can be protected. Give each kind of transaction a name with `capture.WithName`.

## Checklist

- PostgreSQL 18 or newer, pgx v5.
- Transactions started with `mon.Do` or `mon.Begin`, each kind named with `capture.WithName`.
- `capture.NewOTLPSink` (not `NewOTelSink`) if you sample traces. Call `Shutdown` when the app stops.
- A collector with the IsoWatch processor that every replica sends to. See [the collector](../README.md#the-collector).
- `Config.ProtectionStore` if you use protection with more than one replica. Call `mon.Close` when the app stops.
- `Config.Logger` set, so you hear about a store that cannot be reached.
