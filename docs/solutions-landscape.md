# How people fix this today, and where IsoWatch fits

Checked in October 2026.

**Short answer:** a real fix has existed since 2012 (SERIALIZABLE isolation), but most apps do not use it, and some databases have recently moved away from making it the default. What teams actually use are workarounds, applied one place at a time, that only work where a developer remembered them. Research has better ideas that have not reached industry yet. **Nothing tells a team when a workaround was missed in production.** That is the gap IsoWatch fills.

## 1. The real fix: SERIALIZABLE

| Approach | Real fix or workaround? | How well it works | Why it has not ended the problem |
|---|---|---|---|
| [SERIALIZABLE in PostgreSQL](https://dl.acm.org/doi/10.14778/2367502.2367523) (2012) | Real fix | Only slightly slower than the next level down | Apps must catch error 40001 and retry the whole transaction ([docs](https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html)). Most apps do not |
| Its track record | | | Not perfect: [duplicates found under it in 2015](https://www.bailis.org/papers/feral-sigmod2015.pdf), [a real bug found by Jepsen in PostgreSQL 12.3](https://jepsen.io/analyses/postgresql-12.3) (fixed since), and [AWS RDS MySQL often broke SERIALIZABLE](https://jepsen.io/analyses/mysql-8.0.34) in 2023 |
| Databases that default to SERIALIZABLE (CockroachDB, Spanner) | Real fix | Works for apps written for it | CockroachDB added READ COMMITTED in 2023 because retry logic is ["often infeasible for existing applications"](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20230122_read_committed_isolation.md). Spanner added a [weaker level in 2025](https://docs.cloud.google.com/spanner/docs/use-repeatable-read-isolation) to cut latency and aborts |
| [TigerBeetle](https://jepsen.io/analyses/tigerbeetle-0.16.11) | Real fix, for one kind of data | Strictly serial and Jepsen tested | Only for ledgers (debits and credits) |

## 2. What teams actually do

| Pattern | Real fix or workaround? | What it covers | What it misses |
|---|---|---|---|
| `SELECT ... FOR UPDATE`, advisory locks | Workaround, one place at a time | The rows you lock | Easy to forget in one code path. Does not cover rows you did not lock (write skew) |
| Version columns (Rails `lock_version`, JPA `@Version`) | Workaround, per table | Lost updates on one row | Write skew across rows |
| Atomic SQL (`balance = balance - 30`) and database constraints | **The strongest workaround** | Any rule the database can check | Many rules cannot be written as constraints ("someone must stay on call"). The [Rails docs](https://api.rubyonrails.org/classes/ActiveRecord/Validations/ClassMethods.html) admit their uniqueness check can fail and say to add a unique index |
| Idempotency keys ([Stripe style](https://brandur.org/idempotency-keys)) | Fixes a different problem | The same request sent twice | Two different requests changing the same data |
| Rate limits, firewalls | Not a fix | Slows brute force | The [single packet attack](https://portswigger.net/research/smashing-the-state-machine) still fits many requests in one window |
| Reconciliation jobs | Finds damage later | Wrong balances | Hours late, with no cause |

## 3. Research that tries to fix it automatically

| Work | Result | Used in industry? |
|---|---|---|
| [Proving when READ COMMITTED is safe](https://dl.acm.org/doi/10.14778/3476249.3476268) (VLDB 2021) | First check with no false positives and no false negatives | Not that we found |
| [Choosing the isolation level for each transaction](https://dl.acm.org/doi/10.1145/3716374) (PODS 2023, Distinguished Paper) | Computes the cheapest safe level for each transaction type | Theory, needs a model of the workload |
| [TxnSails](https://arxiv.org/abs/2502.00991) (VLDB 2025) and [read promotion](https://doi.org/10.14778/3746405.3746412) (VLDB 2025) | Serializable results at lower cost, up to 4.8 times faster than PostgreSQL SERIALIZABLE | Research prototypes |
| [Atropos](https://arxiv.org/pdf/2103.05573) (PLDI 2021) | Changes the schema so weaker isolation is safe; fixed at least half the anomalies in every test | Research only |
| Offline detectors (see [prior art](prior-art.md)) | Found real bugs | Developers still fix by hand |
| [Static analysis](https://codeql.github.com/codeql-query-help/java/java-toctou-race-condition/) (CodeQL, Semgrep) | Has rules for file races | No common rules for database read then write races |

## 4. Where IsoWatch fits

| Part of IsoWatch | Closest existing work | Difference |
|---|---|---|
| Detect in production | None for applications | |
| Explain and suggest a fix | IsoDiff, RaceDB | Both offline |
| Protect only where needed (built: `ProtectFrom`) | PODS 2023 allocation, TxnSails | Neither uses real production evidence |
| Check that a fix held | None found | |

**Is IsoWatch a fix or a workaround?** Its core is a **detector**, like a smoke alarm. Every existing fix, real or workaround, depends on knowing where to apply it, and IsoWatch tells you where.

IsoWatch also uses the real fix in a targeted way: it can run only the transactions that conflicted at SERIALIZABLE (with retries) and leave the rest fast (see "Protection" in [how it works](how-it-works.md)). One technical point matters here: PostgreSQL only guarantees SERIALIZABLE between transactions that all use it, so **every** transaction type in a conflict must be raised, not just one. IsoWatch sees both sides of every conflict, so it knows which ones. A code path it has never seen can still race, so this would be based on evidence, not a proof. Combining that evidence with the PODS 2023 allocation method is a possible research contribution.

**Honest positioning:** IsoWatch does not "solve isolation anomalies" (SERIALIZABLE does that). It **shows you where your chosen trade off is costing you, with proof, and lets you check that a fix worked.**
