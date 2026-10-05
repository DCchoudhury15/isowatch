package capture

import (
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DCchoudhury15/isowatch/checker"
)

// Protection is how IsoWatch goes from finding a problem to stopping it.
//
// PostgreSQL's SERIALIZABLE level prevents every anomaly, but only between
// transactions that all use it, and it costs retries. So IsoWatch applies it
// only where it has seen a problem: when an anomaly involves transactions
// named "deposit" and "withdraw", both names are protected and run at
// SERIALIZABLE from then on. Everything else keeps running fast at your
// normal level.
//
// This is based on evidence, not proof. A kind of transaction that never
// took part in an observed anomaly is not protected, even if it could race.

// Protect makes transactions with these names run at SERIALIZABLE (and be
// retried by Do). It returns the names that were not protected before.
func (m *Monitor) Protect(names ...string) (added []string) {
	for _, n := range names {
		if n == "" {
			continue
		}
		if _, existed := m.protected.LoadOrStore(n, struct{}{}); !existed {
			added = append(added, n)
		}
	}
	return added
}

// Unprotect returns a name to the normal isolation level.
func (m *Monitor) Unprotect(name string) { m.protected.Delete(name) }

// IsProtected reports whether a name is protected.
func (m *Monitor) IsProtected(name string) bool {
	_, ok := m.protected.Load(name)
	return ok
}

// Protected lists the protected names, sorted.
func (m *Monitor) Protected() []string {
	var out []string
	m.protected.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	sort.Strings(out)
	return out
}

// ProtectFrom protects every named transaction in an anomaly. It returns
// the names that became protected now (not the ones that already were) and
// whether every transaction in the cycle had a name. If one had no name, it
// cannot be protected, and the anomaly may happen again: give it a name with
// WithName.
//
// Anomalies that PostgreSQL should never allow (G0, G1c) or that already ran
// at the levels that forbid them are not acted on, because SERIALIZABLE
// would not help.
//
// A typical setup, checking inside the app:
//
//	var mon *capture.Monitor
//	sink := capture.NewCheckerSink(checker.Options{}, func(a checker.Anomaly) {
//		mon.ProtectFrom(a)
//	})
//	mon, _ = capture.New(ctx, pool, capture.Config{Tables: ..., Sink: sink})
func (m *Monitor) ProtectFrom(a checker.Anomaly) (names []string, complete bool) {
	if a.Unexpected || a.Kind == checker.G0 || a.Kind == checker.G1c {
		return nil, false
	}
	complete = true
	for _, t := range a.Txns {
		if t.Name == "" {
			complete = false
		}
	}
	return m.Protect(a.Names()...), complete
}

func (m *Monitor) maxRetries() int {
	if m.cfg.MaxRetries > 0 {
		return m.cfg.MaxRetries
	}
	return 20
}

// retryable reports whether PostgreSQL asked for the transaction to be run
// again: serialization failure (40001) or deadlock (40P01).
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}

// sleepBackoff waits a little longer after each failed attempt, with jitter
// so retries from many requests do not collide again.
func sleepBackoff(ctx context.Context, attempt int) error {
	max := time.Millisecond << min(attempt, 6) // 1ms, 2ms, ... 64ms
	d := time.Duration(rand.Int64N(int64(max))) + 100*time.Microsecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
