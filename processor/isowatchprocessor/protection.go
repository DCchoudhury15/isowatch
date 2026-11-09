package isowatchprocessor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/protection"
)

const (
	protectionQueue   = 256
	protectionTimeout = 5 * time.Second
)

type protectItem struct {
	name   string
	reason string
}

// protector writes protected transaction names to the shared table, through
// the same protection.Postgres store the apps read, so both sides agree on
// the table. Each name is written at most once per process. A failed write
// is forgotten, so the next anomaly with that name tries again.
type protector struct {
	cfg    *pgxpool.Config
	table  string
	logger *zap.Logger
	tel    *telemetry
	w      *worker[protectItem]

	mu     sync.Mutex
	pool   *pgxpool.Pool
	store  *protection.Postgres
	names  map[string]bool // queued or written
	closed bool
}

func newProtector(cfg ProtectionConfig, logger *zap.Logger, tel *telemetry) (*protector, error) {
	pc, err := pgxpool.ParseConfig(string(cfg.DSN))
	if err != nil {
		return nil, fmt.Errorf("protection.dsn: %w", err)
	}
	pc.MaxConns = 2
	p := &protector{
		cfg:    pc,
		table:  cfg.Table,
		logger: logger,
		tel:    tel,
		names:  map[string]bool{},
	}
	p.w = newWorker(protectionQueue, p.write)
	return p, nil
}

// start creates the connection pool. It does not connect yet, so a database
// that is down does not stop the collector from starting.
func (p *protector) start() error {
	pool, err := pgxpool.NewWithConfig(context.Background(), p.cfg)
	if err != nil {
		return fmt.Errorf("protection: %w", err)
	}
	store, err := protection.NewPostgres(pool, p.table)
	if err != nil {
		pool.Close()
		return err
	}
	p.mu.Lock()
	p.pool, p.store = pool, store
	p.mu.Unlock()
	p.w.start()
	return nil
}

func (p *protector) shutdown(ctx context.Context) {
	p.w.shutdown(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.pool != nil {
		p.pool.Close()
	}
}

// protectable returns the names to protect for an anomaly, or nil when the
// anomaly cannot be helped by SERIALIZABLE or a transaction has no name.
// It follows the same rules as capture's Monitor.ProtectFrom.
func protectable(a checker.Anomaly) []string {
	if a.Unexpected || a.Kind == checker.G0 || a.Kind == checker.G1c || len(a.Txns) == 0 {
		return nil
	}
	for _, t := range a.Txns {
		if t.Name == "" {
			return nil
		}
	}
	return a.Names()
}

// protect queues the anomaly's names. It never blocks.
func (p *protector) protect(ctx context.Context, a checker.Anomaly, issueID string) {
	names := protectable(a)
	if len(names) == 0 {
		return
	}
	reason := fmt.Sprintf("isowatch: %s on %s (issue %s", a.Kind, strings.Join(anomalyTables(a), ", "), issueID)
	if ids := a.TraceIDs(); len(ids) > 0 {
		reason += ", trace " + ids[len(ids)-1]
	}
	reason += ")"
	for _, n := range names {
		p.mu.Lock()
		if p.names[n] || p.closed {
			p.mu.Unlock()
			continue
		}
		p.names[n] = true
		p.mu.Unlock()
		if !p.w.offer(protectItem{name: n, reason: reason}) {
			p.forget(n)
			p.tel.droppedNotification(ctx, "protection")
			p.logger.Warn("isowatch protection queue is full, name not written yet", zap.String("name", n))
		}
	}
}

func (p *protector) forget(name string) {
	p.mu.Lock()
	delete(p.names, name)
	p.mu.Unlock()
}

func (p *protector) write(ctx context.Context, it protectItem) {
	ctx, cancel := context.WithTimeout(ctx, protectionTimeout)
	defer cancel()
	p.mu.Lock()
	store := p.store
	p.mu.Unlock()
	// Add creates the table when it does not exist yet. If the apps created
	// it already, this user only needs INSERT on it.
	if err := store.Add(ctx, it.name, it.reason); err != nil {
		p.forget(it.name)
		p.logger.Warn("isowatch could not write a protected name", zap.String("name", it.name), zap.Error(err))
		return
	}
	p.logger.Info("isowatch protected a transaction name", zap.String("name", it.name), zap.String("reason", it.reason))
}
