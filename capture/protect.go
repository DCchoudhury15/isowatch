package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/protection"
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
//
// With Config.ProtectionStore, the protected names are shared: the Monitor
// loads them when it starts, reads them again every Config.ProtectionPoll,
// and writes the names given to Protect and ProtectFrom (Unprotect deletes
// them). So a name protected by one replica, or by the IsoWatch collector,
// reaches every replica and survives restarts. Writes happen in the
// background, never on the goroutine that called Protect. If the store
// cannot be reached, the Monitor keeps the names it last knew, keeps
// retrying its writes, and logs the problem to Config.Logger.

// Protect makes transactions with these names run at SERIALIZABLE (and be
// retried by Do). It returns the names that were not protected before.
// With a ProtectionStore, the new names are also written to the store.
func (m *Monitor) Protect(names ...string) (added []string) {
	return m.protect("", names)
}

func (m *Monitor) protect(reason string, names []string) (added []string) {
	p := m.prot
	if p != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
	}
	for _, n := range names {
		if n == "" {
			continue
		}
		if _, existed := m.protected.LoadOrStore(n, struct{}{}); !existed {
			added = append(added, n)
			if p != nil {
				p.adds[n] = reason
				delete(p.removes, n)
			}
		}
	}
	if p != nil && len(added) > 0 {
		p.wake()
	}
	return added
}

// Unprotect returns a name to the normal isolation level. With a
// ProtectionStore, the name is also deleted from the store, so every
// replica stops protecting it.
func (m *Monitor) Unprotect(name string) {
	p := m.prot
	if p == nil {
		m.protected.Delete(name)
		return
	}
	p.mu.Lock()
	m.protected.Delete(name)
	delete(p.adds, name)
	p.removes[name] = struct{}{}
	p.mu.Unlock()
	p.wake()
}

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
	return m.protect(string(a.Kind)+": "+a.Summary, a.Names()), complete
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

// storeTimeout bounds every round of calls to the protection store.
const storeTimeout = 5 * time.Second

// protector keeps the protected names of a Monitor in step with a shared
// store.
type protector struct {
	store    protection.Store
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	adds    map[string]string   // names to write to the store, with reasons
	removes map[string]struct{} // names to delete from the store
	failing bool                // the last round of calls to the store failed

	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// startProtection loads the protected names from the store and starts
// following it. A store that cannot be reached is logged, not returned: it
// must not stop the app from starting.
func (m *Monitor) startProtection(ctx context.Context) error {
	store := m.cfg.ProtectionStore
	if store == nil {
		return nil
	}
	// The store's own statements must never be tracked, even if the store
	// is given a connection that IsoWatch wraps.
	if t, ok := store.(interface{ Table() string }); ok {
		schema, name := "", t.Table()
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			schema, name = name[:i], name[i+1:]
		}
		if _, watched := m.Lookup(schema, name); watched {
			return fmt.Errorf("isowatch: the protection table %q must not be in Config.Tables", t.Table())
		}
	}
	interval := m.cfg.ProtectionPoll
	if interval <= 0 {
		interval = 5 * time.Second
	}
	log := m.cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p := &protector{
		store:    store,
		interval: interval,
		log:      log,
		adds:     map[string]string{},
		removes:  map[string]struct{}{},
		kick:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	m.prot = p
	lctx, cancel := context.WithTimeout(ctx, storeTimeout)
	m.syncProtection(lctx)
	cancel()
	go m.followStore()
	return nil
}

// stopProtection stops following the protection store, after writing the
// names that are still pending (waiting at most storeTimeout).
func (m *Monitor) stopProtection() {
	p := m.prot
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.stop)
		<-p.done
	})
}

func (p *protector) wake() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// followStore writes pending changes and reads the store, every interval
// and soon after each Protect or Unprotect.
func (m *Monitor) followStore() {
	p := m.prot
	defer close(p.done)
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	for {
		select {
		case <-p.stop:
			ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
			p.report(m.writeProtection(ctx))
			cancel()
			return
		case <-p.kick:
		case <-tick.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
		m.syncProtection(ctx)
		cancel()
	}
}

// syncProtection writes pending changes to the store, then makes the local
// set equal to the store's list. Names whose writes are still pending keep
// the state this process gave them, so a read can never undo a write that
// has not reached the store yet.
func (m *Monitor) syncProtection(ctx context.Context) {
	p := m.prot
	writeErr := m.writeProtection(ctx)
	names, err := p.store.List(ctx)
	if err != nil {
		p.report(err) // keep the names we last knew
		return
	}
	p.mu.Lock()
	want := make(map[string]struct{}, len(names)+len(p.adds))
	for _, n := range names {
		if _, removing := p.removes[n]; !removing && n != "" {
			want[n] = struct{}{}
		}
	}
	for n := range p.adds {
		want[n] = struct{}{}
	}
	m.protected.Range(func(k, _ any) bool {
		if _, ok := want[k.(string)]; !ok {
			m.protected.Delete(k)
		}
		return true
	})
	for n := range want {
		m.protected.Store(n, struct{}{})
	}
	p.mu.Unlock()
	p.report(writeErr)
}

// writeProtection sends pending adds and removes to the store. A change
// that fails stays pending and is tried again next time.
func (m *Monitor) writeProtection(ctx context.Context) error {
	p := m.prot
	p.mu.Lock()
	adds, removes := maps.Clone(p.adds), maps.Clone(p.removes)
	p.mu.Unlock()
	var firstErr error
	for n, reason := range adds {
		if err := p.store.Add(ctx, n, reason); err != nil {
			firstErr = errors.Join(firstErr, err)
			continue
		}
		p.mu.Lock()
		if r, ok := p.adds[n]; ok && r == reason {
			delete(p.adds, n)
		}
		p.mu.Unlock()
	}
	for n := range removes {
		if err := p.store.Remove(ctx, n); err != nil {
			firstErr = errors.Join(firstErr, err)
			continue
		}
		p.mu.Lock()
		delete(p.removes, n)
		p.mu.Unlock()
	}
	return firstErr
}

// report logs the first failure and the recovery, so a store that is down
// for an hour does not fill the log.
func (p *protector) report(err error) {
	p.mu.Lock()
	was := p.failing
	p.failing = err != nil
	p.mu.Unlock()
	switch {
	case err != nil && !was:
		p.log.Warn("isowatch: protection store failed; keeping the protected names it last returned and retrying", "error", err)
	case err != nil:
		p.log.Debug("isowatch: protection store still failing", "error", err)
	case was:
		p.log.Info("isowatch: protection store works again")
	}
}
