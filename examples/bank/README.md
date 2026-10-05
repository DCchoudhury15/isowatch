# Bank demo

A tiny service with two classic concurrency bugs, and fixed versions of both.

| Endpoint | What it does |
|---|---|
| `POST /deposit?account=1&amount=1` | Reads the balance, waits a moment, writes balance + amount. **Lost update bug.** |
| `POST /deposit-safe` | Same, but reads with `SELECT ... FOR UPDATE`. Fixed. |
| `POST /deposit-auto` | The buggy code, protected by IsoWatch once it sees a problem. |
| `POST /leave?doctor=alice` | A doctor goes off call if someone else is still on call. **Write skew bug.** |
| `POST /leave-safe` | Same, at SERIALIZABLE with retries. Fixed. |
| `POST /leave-auto` | The buggy code, protected by IsoWatch once it sees a problem. |
| `GET /state` | Balances, doctors on call, and IsoWatch counters |
| `POST /reset` | Puts the demo data back and clears IsoWatch |

## Run it

```bash
make db                       # from the repository root: PostgreSQL 18 on port 55432
go run . serve                # checks inside the app and prints alerts
go run . load                 # buggy traffic
go run . load -safe           # fixed traffic
go run . load -auto           # buggy traffic, protected after the first problems
```

`serve -mode otlp` sends spans to a collector instead, and `-mode both` does both. Or run everything in Docker with `make demo-up` from the repository root.

The pause between read and write (`-race-window`, default 5ms) stands in for real work, like calling another service. It makes the races easy to see.
