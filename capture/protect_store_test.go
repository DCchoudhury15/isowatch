package capture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
	"github.com/DCchoudhury15/isowatch/protection"
)

// fakeServer answers the two queries New sends, so the store logic can be
// tested without a database.
type fakeServer struct{}

type fakeRow []any

func (r fakeRow) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r[i]))
	}
	return nil
}

func (fakeServer) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "server_version_num") {
		return fakeRow{180000, "read committed"}
	}
	return fakeRow{"public", args[0].(string), "r", []string{"id"}, true}
}

// fakeStore is a Store in memory that can be made to fail or to block.
type fakeStore struct {
	mu       sync.Mutex
	names    map[string]string
	err      error         // every call fails
	writeErr error         // Add and Remove fail, List works
	block    chan struct{} // if set, Add waits for it to be closed
}

func newFakeStore(names ...string) *fakeStore {
	s := &fakeStore{names: map[string]string{}}
	for _, n := range names {
		s.names[n] = ""
	}
	return s
}

func (s *fakeStore) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *fakeStore) Add(ctx context.Context, name, reason string) error {
	s.mu.Lock()
	block := s.block
	s.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := cmpErr(s.err, s.writeErr); err != nil {
		return err
	}
	if _, ok := s.names[name]; !ok {
		s.names[name] = reason
	}
	return nil
}

func (s *fakeStore) Remove(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := cmpErr(s.err, s.writeErr); err != nil {
		return err
	}
	delete(s.names, name)
	return nil
}

func cmpErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

func (s *fakeStore) List(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	var out []string
	for n := range s.names {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, nil
}

func (s *fakeStore) has(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.names[name]
	return r, ok
}

// syncLog is a log buffer that is safe to read while the Monitor writes.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func storeMonitor(t *testing.T, store protection.Store, poll time.Duration, log *syncLog) *Monitor {
	t.Helper()
	cfg := Config{
		Tables:          []string{"accounts"},
		Sink:            SinkFunc(func(context.Context, *history.Txn) {}),
		ProtectionStore: store,
		ProtectionPoll:  poll,
	}
	if log != nil {
		cfg.Logger = slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	m, err := New(context.Background(), fakeServer{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

// eventually waits until cond is true, or fails after d.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > d {
			t.Fatalf("%s: not true after %v", what, d)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Since(start)
}

func TestStoreNamesLoadedAtStart(t *testing.T) {
	store := newFakeStore("deposit")
	m := storeMonitor(t, store, time.Hour, nil) // no poll during the test
	if !m.IsProtected("deposit") {
		t.Fatal("names in the store must be protected as soon as New returns")
	}
}

func TestStorePolledForNewAndRemovedNames(t *testing.T) {
	store := newFakeStore("deposit")
	m := storeMonitor(t, store, 20*time.Millisecond, nil)
	store.mu.Lock()
	store.names["withdraw"] = "from the collector"
	delete(store.names, "deposit")
	store.mu.Unlock()
	eventually(t, 2*time.Second, "poll picks up changes", func() bool {
		return m.IsProtected("withdraw") && !m.IsProtected("deposit")
	})
}

func TestProtectWritesToStoreInTheBackground(t *testing.T) {
	store := newFakeStore()
	m := storeMonitor(t, store, time.Hour, nil)
	block := make(chan struct{})
	store.mu.Lock()
	store.block = block
	store.mu.Unlock()

	done := make(chan []string)
	go func() { done <- m.Protect("deposit") }()
	select {
	case added := <-done:
		if !slices.Equal(added, []string{"deposit"}) || !m.IsProtected("deposit") {
			t.Fatalf("added = %v", added)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Protect waited for the store; it must never block the caller")
	}
	close(block)
	eventually(t, 2*time.Second, "store gets the name", func() bool {
		_, ok := store.has("deposit")
		return ok
	})

	a := checker.Anomaly{Kind: checker.LostUpdate, Summary: "Lost update on public.accounts(1)",
		Txns: []*history.Txn{{ID: "1", Name: "withdraw"}, {ID: "2", Name: "withdraw"}}}
	if names, complete := m.ProtectFrom(a); !slices.Equal(names, []string{"withdraw"}) || !complete {
		t.Fatalf("ProtectFrom = %v %v", names, complete)
	}
	eventually(t, 2*time.Second, "store gets the anomaly as the reason", func() bool {
		r, ok := store.has("withdraw")
		return ok && r == "lost-update: Lost update on public.accounts(1)"
	})

	m.Unprotect("deposit")
	if m.IsProtected("deposit") {
		t.Fatal("unprotect must act locally at once")
	}
	eventually(t, 2*time.Second, "store loses the name", func() bool {
		_, ok := store.has("deposit")
		return !ok
	})
}

func TestStoreFailureKeepsLastKnownNames(t *testing.T) {
	store := newFakeStore("deposit")
	log := &syncLog{}
	m := storeMonitor(t, store, 10*time.Millisecond, log)
	store.setErr(errors.New("connection refused"))

	// Protect still works locally while the store is down.
	if added := m.Protect("withdraw"); len(added) != 1 {
		t.Fatalf("added = %v", added)
	}
	eventually(t, 2*time.Second, "failure is logged", func() bool {
		return strings.Contains(log.String(), "connection refused")
	})
	time.Sleep(100 * time.Millisecond) // several failed polls
	if !m.IsProtected("deposit") || !m.IsProtected("withdraw") {
		t.Fatalf("a failing store must not drop names, protected = %v", m.Protected())
	}
	if n := strings.Count(log.String(), "level=WARN"); n != 1 {
		t.Fatalf("want one warning for the whole outage, got %d:\n%s", n, log.String())
	}

	// The store comes back: the pending write reaches it, and nothing is lost.
	store.setErr(nil)
	eventually(t, 2*time.Second, "pending write reaches the store", func() bool {
		_, ok := store.has("withdraw")
		return ok
	})
	eventually(t, 2*time.Second, "recovery is logged", func() bool {
		return strings.Contains(log.String(), "works again")
	})
	if got := m.Protected(); !slices.Equal(got, []string{"deposit", "withdraw"}) {
		t.Fatalf("protected = %v", got)
	}
}

func TestStoreDownAtStartDoesNotStopTheApp(t *testing.T) {
	store := newFakeStore("deposit")
	store.setErr(errors.New("no route to host"))
	log := &syncLog{}
	m := storeMonitor(t, store, 10*time.Millisecond, log)
	if m.IsProtected("deposit") {
		t.Fatal("nothing could be read yet")
	}
	store.setErr(nil)
	eventually(t, 2*time.Second, "names arrive once the store is back", func() bool {
		return m.IsProtected("deposit")
	})
	if !strings.Contains(log.String(), "no route to host") {
		t.Fatalf("the startup failure should be logged:\n%s", log.String())
	}
}

// A read that happens before a pending write reaches the store must not
// undo that write locally.
func TestPendingWritesWinOverStaleReads(t *testing.T) {
	store := newFakeStore("deposit")
	m := storeMonitor(t, store, time.Hour, nil)
	// Reads work but writes fail, so List keeps returning the old state.
	store.mu.Lock()
	store.writeErr = errors.New("read only")
	store.mu.Unlock()
	m.Protect("withdraw")
	m.Unprotect("deposit")
	m.syncProtection(context.Background())
	if got := m.Protected(); !slices.Equal(got, []string{"withdraw"}) {
		t.Fatalf("stale read changed pending names, protected = %v", got)
	}
}

func TestCloseWritesPendingNames(t *testing.T) {
	store := newFakeStore()
	m := storeMonitor(t, store, time.Hour, nil)
	m.Protect("a", "b", "c")
	m.Close()
	for _, n := range []string{"a", "b", "c"} {
		if _, ok := store.has(n); !ok {
			t.Fatalf("%q was not written before Close returned", n)
		}
	}
	m.Close() // twice is fine
}

// ---- Real PostgreSQL: several replicas sharing one store ----

// replica is one copy of the app: its own pool, its own in-app checker and
// its own Monitor, sharing only the database and the protection table.
type replica struct {
	pool *pgxpool.Pool
	mon  *Monitor
	col  *collected
}

func newReplica(t *testing.T, tables []string, protTable string, poll time.Duration) *replica {
	t.Helper()
	r := &replica{pool: testPool(t), col: &collected{}}
	store, err := protection.NewPostgres(r.pool, protTable)
	if err != nil {
		t.Fatal(err)
	}
	r.col.sink = NewCheckerSink(checker.Options{Window: 10 * time.Minute}, func(a checker.Anomaly) {
		r.col.mu.Lock()
		r.col.anomalies = append(r.col.anomalies, a)
		r.col.mu.Unlock()
		r.mon.ProtectFrom(a)
	})
	t.Cleanup(r.col.sink.Close)
	r.mon, err = New(context.Background(), r.pool, Config{
		Tables: tables, Sink: r.col.sink, MaxRetries: 200,
		ProtectionStore: store, ProtectionPoll: poll,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.mon.Close)
	return r
}

func protTable(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := fmt.Sprintf("iw_prot_%d", rand.Int63())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
	return name
}

func isolationIn(t *testing.T, r *replica, name string) string {
	t.Helper()
	ctx := context.Background()
	var iso string
	err := r.mon.Do(ctx, r.pool, pgx.TxOptions{}, func(tx *Tx) error {
		return tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&iso)
	}, WithName(name))
	if err != nil {
		t.Fatal(err)
	}
	return iso
}

func TestProtectionSharedAcrossReplicas(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ctr := table(t, pool, "id int PRIMARY KEY, n int")
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", ctr)); err != nil {
		t.Fatal(err)
	}
	prot := protTable(t, pool)
	const poll = 200 * time.Millisecond
	a := newReplica(t, []string{ctr}, prot, poll)
	b := newReplica(t, []string{ctr}, prot, poll)

	if got := isolationIn(t, b, "deposit"); got != "read committed" {
		t.Fatalf("before any anomaly, deposit runs at %q", got)
	}

	// Replica A sees a lost update between two of its own deposits.
	sel := fmt.Sprintf("SELECT n FROM %s WHERE id = 1", ctr)
	upd := fmt.Sprintf("UPDATE %s SET n = $1 WHERE id = 1", ctr)
	t1, _ := a.mon.Begin(ctx, a.pool, pgx.TxOptions{}, WithName("deposit"))
	t2, _ := a.mon.Begin(ctx, a.pool, pgx.TxOptions{}, WithName("deposit"))
	var n1, n2 int
	if err := t1.QueryRow(ctx, sel).Scan(&n1); err != nil {
		t.Fatal(err)
	}
	if err := t2.QueryRow(ctx, sel).Scan(&n2); err != nil {
		t.Fatal(err)
	}
	if _, err := t1.Exec(ctx, upd, n1+1); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := t2.Exec(ctx, upd, n2+1); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	protectedAt := time.Now()
	if got := a.col.all(); len(got) != 1 || got[0].Kind != checker.LostUpdate || !a.mon.IsProtected("deposit") {
		t.Fatalf("replica A should find the lost update and protect deposit, got %v protected=%v", kinds(got), a.mon.Protected())
	}

	// Replica B never saw it, but learns it from the store.
	took := eventually(t, 3*time.Second, "replica B protects deposit", func() bool { return b.mon.IsProtected("deposit") })
	t.Logf("replica B protected deposit %v after replica A (poll interval %v)", time.Since(protectedAt).Round(time.Millisecond), poll)
	if took > poll+time.Second {
		t.Fatalf("took %v, want about one poll interval (%v)", took, poll)
	}
	if got := b.col.all(); len(got) != 0 {
		t.Fatalf("replica B saw no anomaly itself, got %v", kinds(got))
	}
	if got := isolationIn(t, b, "deposit"); got != "serializable" {
		t.Fatalf("replica B runs deposit at %q, want serializable", got)
	}

	// The buggy read-then-write counter, run on both replicas at once. Each
	// replica only checks its own transactions, so neither could protect
	// the other: only the shared store makes both run at SERIALIZABLE.
	var before int64
	pool.QueryRow(ctx, sel).Scan(&before)
	incr := func(tx *Tx) error {
		var n int
		if err := tx.QueryRow(ctx, sel).Scan(&n); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, upd, n+1)
		return err
	}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		r := a
		if w%2 == 1 {
			r = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if err := r.mon.Do(ctx, r.pool, pgx.TxOptions{}, incr, WithName("deposit")); err != nil {
					t.Error(err)
					return
				}
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	var after int64
	pool.QueryRow(ctx, sel).Scan(&after)
	t.Logf("both replicas: committed=%d gained=%d lost=%d retries A=%d B=%d",
		ok.Load(), after-before, ok.Load()-(after-before), a.mon.Stats().Retried, b.mon.Stats().Retried)
	if after-before != ok.Load() {
		t.Fatalf("lost %d of %d increments after protection", ok.Load()-(after-before), ok.Load())
	}
	if na, nb := len(a.col.all()), len(b.col.all()); na != 1 || nb != 0 {
		t.Fatalf("no new anomalies expected, got A=%d B=%d", na, nb)
	}

	// Unprotect on one replica reaches the other.
	a.mon.Unprotect("deposit")
	eventually(t, 3*time.Second, "replica B unprotects deposit", func() bool { return !b.mon.IsProtected("deposit") })
	if got := isolationIn(t, b, "deposit"); got != "read committed" {
		t.Fatalf("after Unprotect, deposit runs at %q", got)
	}
}

// The collector sees every replica's traffic. When it finds an anomaly it
// writes the names to the same table, with the same statement, and every
// replica picks them up.
func TestNameWrittenByCollectorReachesEveryReplica(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	acct := table(t, pool, "id int PRIMARY KEY, n int")
	prot := protTable(t, pool)
	const poll = 200 * time.Millisecond
	a := newReplica(t, []string{acct}, prot, poll)
	b := newReplica(t, []string{acct}, prot, poll)

	if _, err := pool.Exec(ctx, protection.CreateTableSQL(prot)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+prot+" (name, reason) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
		"transfer", "write-skew seen by the collector"); err != nil {
		t.Fatal(err)
	}
	for _, r := range []*replica{a, b} {
		eventually(t, 3*time.Second, "replica protects transfer", func() bool { return r.mon.IsProtected("transfer") })
		if got := isolationIn(t, r, "transfer"); got != "serializable" {
			t.Fatalf("transfer runs at %q", got)
		}
	}
}

func TestProtectionSurvivesRestart(t *testing.T) {
	pool := testPool(t)
	acct := table(t, pool, "id int PRIMARY KEY, n int")
	prot := protTable(t, pool)

	first := newReplica(t, []string{acct}, prot, time.Hour)
	first.mon.Protect("deposit")
	first.mon.Close() // the app shuts down

	// A new process: no poll happens during the test, so only the load at
	// startup can bring the name back.
	second := newReplica(t, []string{acct}, prot, time.Hour)
	if !second.mon.IsProtected("deposit") {
		t.Fatalf("protection was lost in the restart, protected = %v", second.mon.Protected())
	}
	if got := isolationIn(t, second, "deposit"); got != "serializable" {
		t.Fatalf("after restart deposit runs at %q", got)
	}
}

func TestProtectionTableCannotBeWatched(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	prot := protTable(t, pool)
	store, _ := protection.NewPostgres(pool, prot)
	if err := store.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := New(ctx, pool, Config{Tables: []string{prot}, Sink: SinkFunc(func(context.Context, *history.Txn) {}), ProtectionStore: store})
	if err == nil || !strings.Contains(err.Error(), "must not be in Config.Tables") {
		t.Fatalf("watching the protection table must be refused, got %v", err)
	}
	_, err = New(ctx, pool, Config{Tables: []string{"public." + prot}, Sink: SinkFunc(func(context.Context, *history.Txn) {}),
		ProtectionStore: mustStore(t, pool, "public."+prot)})
	if err == nil {
		t.Fatal("watching the protection table must be refused when both names are qualified")
	}
}

func mustStore(t *testing.T, pool *pgxpool.Pool, table string) *protection.Postgres {
	t.Helper()
	s, err := protection.NewPostgres(pool, table)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
