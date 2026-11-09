# Adding IsoWatch to an existing app

This page takes an app that already uses a `pgxpool.Pool` and adds IsoWatch with as few code changes as possible. Then it shows how to check what IsoWatch can and cannot see in your SQL.

## 1. Pick the tables

List the tables you care about:

```go
mon, err := capture.New(ctx, pool, capture.Config{
    Tables: []string{"accounts", "orders"},
    Sink:   sink,
})
```

Or watch everything and leave some out:

```go
mon, err := capture.New(ctx, pool, capture.Config{
    WatchAll:      true,
    ExcludeTables: []string{"audit.*", "sessions", "public.events"},
    Sink:          sink,
    Logger:        slog.Default(),
})
```

- `WatchAll` watches every plain table with a primary key, outside `pg_catalog` and `information_schema`, **as it exists when `New` runs**. Tables created later are not watched.
- Tables it cannot watch are skipped, not an error: tables without a primary key, and partitioned tables. `mon.SkippedTables()` lists them with the reason, and `Logger` gets one line about them.
- `ExcludeTables` takes `"schema.table"`, `"table"` (that name in any schema) or `"schema.*"` (a whole schema). It also removes tables listed in `Tables`.
- `WatchAll` leaves out the table of a `ProtectionStore`. Listing that table in `Tables` is an error.

## 2. Wrap the pool

```go
db := mon.Wrap(pool)
```

`db` has the same `Exec`, `Query`, `QueryRow`, `SendBatch`, `CopyFrom`, `Ping` and `Close` as the pool, so it fits any interface made of those, and the code that runs SQL does not change. Two things differ: code that takes a `*pgxpool.Pool` needs its type changed, and `db.Begin` returns a `*capture.Tx` (same `Exec`, `Query`, `QueryRow`, `Commit`, `Rollback`) instead of a `pgx.Tx`. `db.Pool()` gives the plain pool back for anything IsoWatch should not see.

**Statements outside a transaction are tracked too.** In PostgreSQL, a statement run straight on the pool is its own transaction. IsoWatch records it as a one-statement transaction, with the trace of the request.

This matters because the most common lost update in web apps looks like this:

```go
var balance int
db.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1", id).Scan(&balance)
db.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", balance+amount, id)
```

Each statement on its own is fine, so checking one transaction at a time finds nothing. Check **whole requests** instead: `checker.Options{GroupBy: checker.ByTrace}` in the app, or `group_by: trace` in the collector. Then two requests that do this at the same time are a lost update, and `LostWrites` counts exactly the lost changes. Each request needs its own trace id, which normal HTTP tracing middleware gives you.

`Begin`, `BeginTx` and `Do` on `db` return a tracked `*capture.Tx`, the same as `mon.Begin` and `mon.Do`.

## 3. Name what runs

Names show up in reports and are what protection works on. The easiest place to set them is your router or middleware, once per route:

```go
func named(name string, h http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        h.ServeHTTP(w, r.WithContext(capture.WithNameContext(r.Context(), name)))
    })
}

mux.Handle("POST /deposit", named("deposit", depositHandler))
```

Every statement and transaction the request runs with that context carries the name. `capture.WithName("...")` on one `Begin` or `Do` still wins over the context.

## 4. Check what IsoWatch can see

IsoWatch adds hidden columns to the SQL it understands. A statement it cannot rewrite safely runs **unchanged** and is "untracked": an anomaly that involves it can be missed. The coverage report shows how much of your SQL that is, and which statements.

```go
fmt.Print(mon.CoverageReport())
```

Real output:

```text
isowatch: tracked 87% of statements on watched tables (80 of 91)
untracked: 10 GROUP BY or HAVING, 1 WITH query
top untracked statements:
    10  GROUP BY or HAVING: SELECT owner, sum(balance) FROM accounts WHERE balance > $1 GROUP BY owner
     1  WITH query: WITH rich AS (SELECT id FROM accounts WHERE balance > $1) SELECT count(*) FROM rich
not watched: public.events (no primary key)
```

- Only statements that touch a watched table count. `SELECT 1`, `SET` and statements on other tables are left out of the percentage.
- Statements are grouped by their SQL without literal values, so the same query with different numbers is one line.
- The list keeps at most 200 different statements. When it is full, a new statement replaces the least frequent one, so a frequent query that shows up late still gets in. Its count is then shown as `~N` (it may be a little high).
- `SendBatch` and `CopyFrom` are not tracked yet. They are counted as untracked so you can see them.

Three ways to get the report in production:

| How | Code |
|---|---|
| In your logs | `Config{Logger: slog.Default(), ReportEvery: 10 * time.Minute}`. Call `mon.Close()` when the app stops. |
| On an internal HTTP port | `mux.Handle("/debug/isowatch", mon.Handler())`. JSON with the coverage and `Stats`; add `?format=text` for the report above. |
| From code | `mon.Coverage()` returns the numbers. |

The report shows your SQL without literal values, but it does show your queries. Do not put the handler on a public port.

### What to do about untracked statements

For each line in the report, decide:

1. **Is it on a path that can race?** A report query or an admin page usually does not matter.
2. **Can it be written in a form IsoWatch tracks?** For example a plain `SELECT ... WHERE id = $1` instead of a `WITH` query, or a lock (`FOR UPDATE`) that makes the race impossible anyway.
3. **Otherwise, accept it.** The rest of the app is still checked.

See [Limitations](limitations.md) for the full list of what is tracked.

## Checklist

- [ ] `Tables` or `WatchAll`, and `SkippedTables()` checked.
- [ ] Pool wrapped with `mon.Wrap(pool)`, or transactions started with `mon.Do` and `mon.Begin`.
- [ ] Checking by trace (`checker.ByTrace` or `group_by: trace`) if your app reads and writes in separate statements.
- [ ] Names set per route with `WithNameContext`.
- [ ] Coverage report looked at, untracked lines decided on.
- [ ] For production: [Running in production](production.md).
