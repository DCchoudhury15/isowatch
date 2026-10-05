# Who has built what (prior art)

Research done in October 2026, before writing any code. The goal was to find out what already exists, so the claims about IsoWatch stay honest.

## Summary

* Checking transactions for isolation anomalies is a mature research area. **IsoWatch does not claim a new checking algorithm.** It uses the standard model (Adya) and cycle search, like Elle does.
* Every tool we found that looks at **applications** works **offline**: it reads a captured query log or a test run, and mostly reports anomalies that **could** happen. Those reports include false positives that a developer has to sort through.
* Every tool we found that checks **databases** tests the database engine itself with a controlled workload. None of them watches a normal application in production.
* The only checker that works **online** (Cobra) needs special extra transactions added to the workload, which later papers call impractical for production.
* Database monitoring products (Datadog, pganalyze, CockroachDB) focus on speed and locking, not on correctness.
* **OpenTelemetry has no standard fields for a transaction id, its start and end, its isolation level, or row versions.** So today nobody can do this from normal traces.

**What is new in IsoWatch, as far as we found:** finding anomalies that actually happened, continuously, in production traffic of normal applications, using exact row versions, with each alert linked to the request traces involved. We did not find anything that does this. That is not proof that nothing exists.

## 1. Tools that look at applications (closest to IsoWatch)

| Tool | Where published | Input | Finds real or possible anomalies? | Online? |
|---|---|---|---|---|
| [ACIDRain / 2AD](https://www.bailis.org/papers/acidrain-sigmod2017.pdf) | SIGMOD 2017 | Database query logs captured while using the app | Possible ones | No |
| [IsoDiff](http://www.vldb.org/pvldb/vol13/p2773-gan.pdf) | VLDB 2020 | MySQL query log | Possible ones, with tricks and developer hints to cut false positives | No |
| [IsoPredict](https://arxiv.org/pdf/2404.04621) ([code](https://github.com/PLaSSticity/IsoPredict-implementation)) | PLDI 2024 | Trace of an app on a key value store | Predicts them | No |
| [Augur](https://arxiv.org/abs/2609.05288) | 2026 | Execution traces of apps with SQL | Predicts them | No |
| [RaceDB](https://par.nsf.gov/servlets/purl/10617180) | IEEE S&P 2025 | App code and queries, replayed through a proxy | Possible ones, then confirmed by replay | No (security testing) |
| [ReqRacer](https://dl.acm.org/doi/10.1145/3468264.3468594) | FSE 2021 | Request traces | Possible ones | No |
| [MonkeyDB](https://arxiv.org/pdf/2103.02830) | OOPSLA 2021 | A fake database that behaves weakly on purpose | Testing | No |
| [isolator](https://github.com/palkan/isolator) (Ruby gem) | Open source | Rails hooks | A different problem: side effects inside a transaction | Development only |

Notes:

* ACIDRain found 22 serious bugs in 12 popular shop apps used on more than 2 million websites.
* The IsoDiff paper says checking a real concurrent execution would be possible, but leaves it as future work.
* IsoPredict and Augur come from the same research group. Augur (2026) is the most recent and the closest to IsoWatch, but it is still offline prediction.
* A related study ([Ad hoc transactions, SIGMOD 2022](https://ipads.se.sjtu.edu.cn/_media/publications/concerto-sigmod22.pdf)) found more than 90 places in 8 popular apps where developers built their own locking in code. Those can look like anomalies at the SQL level even when the app is safe.
* [Quantifying isolation anomalies (VLDB 2009)](http://www.vldb.org/pvldb/vol2/vldb09-185.pdf) showed anomaly rates can be measured, and are sometimes surprising.

## 2. Tools that check databases (their ideas are reused)

| Tool | Where published | What IsoWatch takes from it |
|---|---|---|
| [Elle](https://people.ucsc.edu/~palvaro/elle_vldb21.pdf) ([code](https://github.com/jepsen-io/elle)) | VLDB 2021 | The anomaly names and fast cycle search. Elle needs special workloads where versions can be inferred; IsoWatch gets real versions from PostgreSQL instead |
| [Cobra](https://www.usenix.org/system/files/osdi20-tan.pdf) | OSDI 2020 | The only online checker, but it needs extra "fence" transactions |
| [Version certificate recovery](https://www.doc.ic.ac.uk/~afd/papers/2024/EuroSys.pdf) | EuroSys 2024, Best Paper | **The most important idea:** read row versions straight from the database, implemented for PostgreSQL. They use it to test databases; IsoWatch uses it to watch applications |
| [Chronos and Aion](https://arxiv.org/pdf/2504.01477) | 2025 | Aion checks online at about 12,000 transactions per second, for database testing |
| [PolySI](https://www.vldb.org/pvldb/vol16/p1264-wei.pdf), [Plume](https://dl.acm.org/doi/10.1145/3689742), [AWDIT](https://dl.acm.org/doi/10.1145/3742465), [Vbox](https://arxiv.org/pdf/2503.05163), [Boomslang](https://arxiv.org/pdf/2604.20587) | 2023 to 2026 | Faster or more complete checking algorithms, all for testing databases |

## 3. Products

| Product | What it does | Why it is different |
|---|---|---|
| [Jepsen](https://github.com/jepsen-io/elle) | Tests databases before release | Not for application monitoring |
| [Antithesis](https://antithesis.com/) | Deterministic testing before release | Not production |
| [Datadog Database Monitoring](https://www.datadoghq.com/product/database-monitoring/), [pganalyze](https://pganalyze.com/blog/postgresql-log-monitoring-101-deadlocks-checkpoints-blocked-queries) | Slow queries, locks, deadlocks | Speed, not correctness |
| [CockroachDB Insights](https://www.cockroachlabs.com/docs/stable/monitor-and-analyze-transaction-contention) | Shows which transaction conflicted when SERIALIZABLE rejected one | Shows conflicts that were prevented, not anomalies that got through |
| Data observability tools (Monte Carlo, Bigeye, Anomalo) | Spot odd patterns in data pipelines | A different meaning of "anomaly" |

## 4. Gaps IsoWatch fills

1. **Real, not predicted.** It reports anomalies that happened, proven by row versions.
2. **Continuous, in production.** It runs all the time, not as a one off analysis.
3. **Exact versions for normal apps.** No special workloads and no fence transactions.
4. **Linked to traces.** Each alert names the requests involved, inside the tracing tools teams already use.
5. **A possible standard.** IsoWatch defines fields that [OpenTelemetry's database conventions](https://opentelemetry.io/docs/specs/semconv/db/database-spans/) do not have yet.

## Caveats

* Some papers were read from their abstracts only (Augur, Chronos and Aion, Boomslang).
* "Nothing found" is based on web and GitHub searches in October 2026. Check again before claiming "first" in public.
* The name "IsoWatch" is also used by a few small unrelated GitHub projects (smartwatch apps). Harmless, but worth knowing.
