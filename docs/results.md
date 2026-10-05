# Results

Every number on this page was measured on a laptop (12 cores, Linux) against **PostgreSQL 18.6 in Docker**, with the commands shown, so you can repeat them.

```bash
make db        # PostgreSQL 18 on port 55432
make test-db   # runs everything below
```

## 1. Does it count lost writes correctly?

Test: `TestLostWritesMatchTheMoney` in `capture/integration_test.go`.

16 workers each add 1 to the same counter 40 times (640 increments), using the buggy pattern: read the value, then write value + 1, at READ COMMITTED. Every lost update makes the final value smaller. We compare what the database lost with what IsoWatch counted.

| Run | Increments | Final value | Lost in database | Lost counted by IsoWatch |
|---|---|---|---|---|
| 1 | 640 | 58 | 582 | **582** |
| 2 | 640 | 58 | 582 | **582** |
| 3 | 640 | 46 | 594 | **594** |
| 4 | 640 | 44 | 596 | **596** |
| 5 | 640 | 48 | 592 | **592** |

The same test with `SELECT ... FOR UPDATE` (the fix): 640 increments, final value 640, **0 lost, 0 counted, 0 alerts**, every run.

## 2. Does it raise false alarms?

| Test | What it does | Alerts |
|---|---|---|
| `TestNoFalsePositivesUnderSerializable` | 12 workers make random transfers between 5 accounts at SERIALIZABLE. PostgreSQL guarantees no anomalies, so any alert would be a false alarm | **0** in every run (about 100 commits and 380 rejected by PostgreSQL per run) |
| `TestNoFalseAlarmWithForUpdate` | The lost update interleaving, fixed with `FOR UPDATE` | **0** |
| `TestSerializablePreventsAndStaysQuiet` | The lost update interleaving at SERIALIZABLE. PostgreSQL rejects one transaction (error 40001) | **0** |

These tests found two real bugs during development, both now fixed and covered by unit tests:

* A transaction that updated the same row twice was wrongly treated as overwriting someone else's version (it was its own). This caused false alarms at SERIALIZABLE.
* When transactions arrived out of order, one kind of link was missed.

## 3. Does it catch write skew?

`TestDetectsWriteSkew`: two doctors on call, both leave at once.

| Isolation level | Both commit? | IsoWatch |
|---|---|---|
| READ COMMITTED | yes, nobody is left on call | **write-skew** |
| REPEATABLE READ | yes, nobody is left on call | **write-skew** |
| SERIALIZABLE | no, PostgreSQL rejects one | nothing (correct) |

## 4. Does protection stop the losses?

Test: `TestProtectionStopsLosses`. The same buggy counter as in section 1 (16 workers, 640 increments), with transactions named `incr` and `ProtectFrom` called on every anomaly. Then the same code runs again for another 640 increments.

| Phase | Increments | Lost | Anomalies | Retries |
|---|---|---|---|---|
| 1: until IsoWatch notices | 640 | 16 (IsoWatch counted 16) | 16 | |
| 2: protected | 640 | **0** | **0** | 933 |

The first losses still happen, because IsoWatch has to see a problem before it can act. After that, nothing is lost. The cost is retries: the 16 workers now take turns on the one hot row.

## 5. The demo, end to end

`examples/bank`, 200 concurrent deposits plus two doctors leaving at once:

| | Deposits lost | Doctors on call | IsoWatch |
|---|---|---|---|
| Buggy endpoints, checked in the app | 183 | none | 199 lost-update alerts, 1 write-skew, 183 lost writes counted |
| Buggy endpoints, through the collector (100 deposits) | 91 | none | 99 lost-update spans, 1 write-skew span |
| Buggy endpoints, everything in Docker (`make demo-up`, 200 deposits) | 182 | none | 198 lost-update and 1 write-skew spans in Jaeger, 182 lost writes counted |
| Fixed endpoints | 0 | one stays | nothing |
| Buggy endpoints with protection (`-auto`) | 19 | none (the one leave happens before protection) | 20 lost-update alerts, 1 write-skew, then protected, about 750 to 830 retries |

There are more alerts than lost writes because each alert is one cycle, and one stale write can form a cycle with several others.

## 6. Speed

**Checker** (`go test -bench . ./checker/`), one CPU core:

| Workload | Time per transaction | About |
|---|---|---|
| Every transaction reads and writes the same hot row | 3.0 microseconds | 330,000 per second |
| Transactions spread over 10,000 rows | 3.3 microseconds | 300,000 per second |

**Capture overhead** (`go test -bench . ./capture/`): one transaction that reads one row and updates it, against local Docker PostgreSQL, 3,000 transactions, three runs:

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Plain pgx | 439 us | 449 us | 446 us |
| With IsoWatch | 496 us | 479 us | 464 us |
| Extra | 57 us | 30 us | 18 us |

So about 20 to 55 microseconds per transaction here. Most of that is the extra columns on each statement and parsing SQL the first time it is seen (plans are cached afterwards). Over a real network the database round trip is larger, so the percentage would be smaller. Measure it in your own setup before using it in production.

## 7. SQL rewriting is checked on a real server

`TestRewrittenSQLRunsOnPostgres` sends every rewritten statement (selects with aliases, joins, `USING`, `FOR UPDATE`, `ORDER BY`, composite keys, updates with `FROM`, deletes, inserts, upserts, `ON CONFLICT DO NOTHING`) to PostgreSQL 18 and checks that it runs.
