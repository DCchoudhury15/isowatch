# Is this a real problem?

Checked in October 2026.

## Evidence that it is

**The conditions are almost everywhere.**

* In a survey of database administrators, 86% said most or all of their transactions run at READ COMMITTED (Pavlo 2017, as cited in a [PLDI 2023 paper](https://dl.acm.org/doi/pdf/10.1145/3591243)).
* The default isolation levels of PostgreSQL, Oracle, SQL Server (READ COMMITTED) and MySQL (REPEATABLE READ) do not prevent lost updates or write skew ([explanation](https://vladmihalcea.com/race-condition/), [PostgreSQL docs](https://www.postgresql.org/docs/current/transaction-iso.html)).

**Researchers find these bugs wherever they look.**

* [ACIDRain](https://www.bailis.org/papers/acidrain-sigmod2017.pdf): 22 serious bugs in 12 shop apps used on more than 2 million websites.
* [IsoDiff](http://www.vldb.org/pvldb/vol13/p2773-gan.pdf): anomalies in 7 real apps.
* [Ad hoc transactions](https://ipads.se.sjtu.edu.cn/_media/publications/concerto-sigmod22.pdf): home made locking in all 8 apps studied.
* [RaceDB](https://par.nsf.gov/servlets/purl/10617180): 18 new bugs and 7 CVEs in 14 apps.

**They have cost real money.**

* [Flexcoin](https://vladmihalcea.com/race-condition/) (2014): 896 bitcoins stolen by sending many transfers at once that all read the same balance. The company [shut down](https://www.welivesecurity.com/2014/03/05/bitcoin-bank-flexcoin-shuts-down-after-attackers-loot-570000-from-hot-wallet/).
* [Poloniex](http://globaleconomicanalysis.blogspot.com/2014/03/more-bitcoin-exchanges-hacked-flexcoin.html): lost 12.3% of its assets.
* [Starbucks gift cards](https://www.geekwire.com/2015/security-researcher-exploits-flaw-to-load-starbucks-gift-cards-with-free-money/) (2015): free money from moving a balance twice at once.
* GitLab account takeover (2023), double coupon use at Instacart, and [customers charged twice](https://dev.to/michaelajayi_dev/customers-charged-twice-for-the-same-order-a-race-condition-post-mortem-ojl).

**Attackers got better at triggering them.** PortSwigger's [single packet attack](https://portswigger.net/research/smashing-the-state-machine) (2023), built into Burp Suite, sends 20 to 30 requests inside one millisecond. Race conditions are still [under 0.3% of HackerOne reports](https://www.hackerone.com/vulnerability-management/sherrets-race-condition), but interest has grown since then.

**AI written code makes more of these mistakes.** CodeRabbit found 1.75 times more logic errors in AI written pull requests ([470 PRs](https://www.theregister.com/2025/12/17/ai_code_bugs/)). The Stack Overflow blog reports about [twice as many concurrency mistakes](https://stackoverflow.blog/2026/01/28/are-bugs-and-incidents-inevitable-with-ai-coding-agents/) (we did not check their method).

**People pay for correctness tools.** Antithesis raised a [$105M Series A](https://www.finsmes.com/2025/12/antithesis-raises-105m-in-series-a-funding.html) in December 2025. Stripe and Uber built their own systems to check money movement.

## Evidence against

1. **Most apps rarely hit it.** It needs real concurrency on the same rows.
2. **The damage is often found another way.** Finance teams run reconciliation jobs that notice a wrong balance, hours later and without the cause. IsoWatch's edge is naming the exact requests, within a second.
3. **It detects, it does not prevent.** An attack finishes in milliseconds.
4. **Nobody is publicly asking for this tool.** The advice people give is "use FOR UPDATE" or "test with barriers". Teams usually do not know they have the bug until it costs money.
5. **Setup takes some effort**: a driver change, PostgreSQL 18, and a collector.

## Verdict

* **The problem is real, well documented, and expensive when it hits.** The conditions for it are the default almost everywhere.
* **As a portfolio and research project: strong.**
* **As a product: demand is not proven yet.** The likely users are teams where one row means money: payments and wallets, shops (stock, coupons, gift cards), ticketing, marketplaces, game economies.

## Cheap ways to learn more

* Ask 5 to 10 engineers at payments or shop companies: "Have you had a double spend, oversell or lost update in production? How did you find out, and how long did it take?"
* Publish the demo as a blog post ("Replaying the Flexcoin bug and catching it live") and see how people react.
