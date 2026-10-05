# How IsoWatch works

This page explains IsoWatch from the ground up, with one running example.

## 1. Row versions and xmin

PostgreSQL never changes a row in place. An `UPDATE` writes a **new version** of the row and keeps the old one around until nobody needs it.

Every version has a hidden column called `xmin`. It holds the id of the transaction that wrote that version. You can see it yourself:

```sql
SELECT xmin, id, balance FROM accounts WHERE id = 1;
--  xmin | id | balance
--   760 |  1 |     100
```

So `xmin` answers the question "which write produced the data I am looking at?" That is all IsoWatch needs.

## 2. What IsoWatch records

IsoWatch wraps your pgx transaction. For each statement on a watched table it adds a few hidden columns:

| Your SQL | What IsoWatch actually sends |
|---|---|
| `SELECT balance FROM accounts WHERE id = $1` | `SELECT balance, accounts.xmin::text AS __isowatch_0, accounts.id::text AS __isowatch_1 FROM accounts WHERE id = $1` |
| `UPDATE accounts SET balance = $1 WHERE id = $2` | `UPDATE ... RETURNING old.xmin::text, old.id::text, new.xmin::text, new.id::text` |

`RETURNING old.xmin` is new in PostgreSQL 18. It tells us exactly which version an update or delete replaced.

The hidden columns are removed before your code sees the rows. `Scan`, `Values`, `FieldDescriptions`, `CollectRows` and command tags all look exactly like plain pgx.

When the transaction commits, IsoWatch has a small record:

```
Transaction B
  read  accounts(1) at version 760
  wrote accounts(1): replaced version 761, created version 762
```

If the transaction rolls back, nothing is recorded. If a savepoint is rolled back, the writes made after it are forgotten.

## 3. Turning records into a graph

Between two transactions there are three kinds of links (these come from Atul Adya's 1999 thesis, the standard model for this):

| Link | Meaning |
|---|---|
| `A -wr-> B` | B **read** a version that A wrote |
| `A -ww-> B` | B **replaced** a version that A wrote |
| `A -rw-> B` | A read a version, and B later **replaced** it. A did not see B's write |

If you could line up all transactions one after another (which is what "serializable" means), every link would point forward in that line. So **a cycle means the transactions could not have run in any order**. That cycle is the anomaly.

## 4. The lost update, step by step

This is a real history recorded from PostgreSQL 18:

```
A: read  accounts(1) at 760     B: read  accounts(1) at 760
A: write 760 -> 761
A: commit
                                B: write 761 -> 762   (PostgreSQL waited for A, then B wrote on top of A's version)
                                B: commit
```

The links:

* B replaced version 761, which A wrote: `A -ww-> B`
* B read version 760, and A replaced it: `B -rw-> A`

That is a cycle: A, then B, then back to A. It is all on one row, so IsoWatch calls it a **lost update**. B overwrote A's deposit without ever seeing it.

Notice that nothing here is a guess. B really read 760 and really replaced 761. PostgreSQL told us both numbers.

## 5. Write skew

Two doctors, both on call. Each one checks "is someone else on call?", sees yes, and goes off call:

```
A: read alice at 100, read bob at 100, write alice
B: read alice at 100, read bob at 100, write bob
```

* A read bob's version, and B replaced it: `A -rw-> B`
* B read alice's version, and A replaced it: `B -rw-> A`

Cycle again, this time across two different rows. That is **write skew**.

## 6. Kinds of anomalies

| Kind | What it means | Can PostgreSQL allow it? |
|---|---|---|
| `lost-update` | Someone overwrote a row based on an old read | At READ COMMITTED, yes |
| `write-skew` | Two transactions each acted on data the other was changing | At READ COMMITTED and REPEATABLE READ, yes |
| `G-single` | Other cycles with one `rw` link (for example read skew) | At READ COMMITTED, yes |
| `G2-item` | Other cycles with two or more `rw` links | Below SERIALIZABLE, yes |
| `G1c`, `G0` | Cycles with no `rw` link at all | No, never |

If every transaction in a cycle ran at a level that should have prevented it, IsoWatch marks the alert **unexpected**. That would point to a database bug or to something IsoWatch could not see.

## 7. Arrival order does not matter

Transactions can reach the checker in any order, for example from different services or in different batches. Every link is found from both sides: when a writer arrives, IsoWatch checks who already read or replaced its versions, and the other way around. A cycle is found when its last transaction arrives. The tests check every possible arrival order.

## 8. Counting lost writes

For each row, IsoWatch starts at the newest version and follows "this write was based on that read" backwards. Versions on that path made it into the current value. A version that is not on the path, and that some later writer replaced without seeing, was **lost**.

For a counter where every request does `n = n + 1`, this number equals exactly how many increments went missing. The integration tests check that it matches the database every time.

## 9. Memory

A cycle can only form between transactions that overlap in time. So IsoWatch forgets transactions after a window (2 minutes by default). Set the window longer than your longest transaction.

## 10. Where the checking runs

* **In your app** (`capture.NewCheckerSink`): simplest, good for tests and staging.
* **In the OpenTelemetry Collector** (`capture.NewOTelSink` plus the IsoWatch processor): each transaction travels as an `isowatch.txn` span inside your request's trace. The processor checks them centrally and adds an `isowatch.anomaly` span to the trace that completed the cycle, with links to every other trace involved.

## 11. Privacy

* SQL is stored **normalized**: literal values are replaced by `$1`, `$2`, ...
* Primary key values are stored as a short **hash** by default, so equal keys still match but the real values never leave your app. Set `KeyMode: capture.RawKeys` if your keys are not sensitive and you want to read them in alerts.
* Column values are never recorded. Only versions and keys are.

## 12. What an alert contains

Every anomaly comes with:

* **A summary** in one sentence.
* **Steps**: two per link in the cycle, saying which transaction read, wrote or replaced which row version, and with which SQL. For example:

  ```
  deposit (txn 75d0a9c6) in trace 77c63e5e read public.accounts(1) version 3449: SELECT balance FROM accounts WHERE id = $1
  deposit (txn 95392cf0) in trace 3eed3a5c replaced public.accounts(1) version 3449: UPDATE accounts SET balance = $1 WHERE id = $2
  ```

* **A fix**. IsoWatch knows which read was stale (the read side of each `rw` link), so it can say exactly which statement needs `FOR UPDATE`, or suggest a relative update, a constraint, or SERIALIZABLE with retries.

In the collector these become attributes on the `isowatch.anomaly` span: `isowatch.anomaly.kind`, `.summary`, `.fix`, `.steps`, `.names`, `.traces`, `.edges`, plus span links to every transaction involved.

## 13. Protection: from finding to fixing

PostgreSQL's SERIALIZABLE level prevents every anomaly in this list. Most apps do not use it everywhere because it can reject transactions (error 40001), which then have to be retried.

IsoWatch can use SERIALIZABLE **only where it has seen a problem**:

1. You give each kind of transaction a name with `capture.WithName("deposit")`.
2. When an anomaly happens, `mon.ProtectFrom(anomaly)` marks every named transaction in it as protected.
3. From then on, `mon.Do` runs protected names at SERIALIZABLE and retries them (with a short, random back off) when PostgreSQL asks.

**Why every transaction in the cycle, not just one?** PostgreSQL only guarantees SERIALIZABLE between transactions that all use it. If a "deposit" ran at SERIALIZABLE but the "withdraw" it raced with did not, the race could still happen. IsoWatch sees both sides of every cycle, so it protects both.

**What this does and does not promise:**

* It stops the races IsoWatch has seen, between the kinds of transaction it has seen.
* A kind of transaction that never took part in an observed anomaly is not protected, even if it could race. This is based on evidence, not a proof.
* A transaction without a name cannot be protected. `ProtectFrom` tells you if the cycle had one.
* The first anomaly still happens, because IsoWatch has to see it.
* On a very hot row, protected transactions take turns, so you pay in retries (in the tests, about 1 to 2 retries per transaction with 16 workers on one row).

Protection works in the app (`CheckerSink`). The collector only reports; it cannot change how your app runs.

## 14. Ignoring what you accept

Some races are acceptable (for example, a "last seen" timestamp where the newest write winning is fine). `checker.Options{IgnoreTables: ..., IgnoreNames: ...}` (or `ignore_tables` and `ignore_names` in the collector) drops those anomalies. An anomaly is ignored if any transaction in it has an ignored name, or if every row in it belongs to an ignored table. Ignored anomalies are still counted in `Stats().Suppressed`.
