// Package capture records what each database transaction read and wrote,
// so IsoWatch can check it for isolation anomalies.
//
// Use it in place of starting a pgx transaction yourself:
//
//	mon, err := capture.New(ctx, pool, capture.Config{
//		Tables: []string{"accounts"},
//		Sink:   capture.NewOTelSink(nil),
//	})
//	tx, err := mon.Begin(ctx, pool, pgx.TxOptions{})
//	... tx.QueryRow / tx.Exec / tx.Query as usual ...
//	err = tx.Commit(ctx)
//
// It needs PostgreSQL 18 or newer, because it uses RETURNING old.xmin to see
// exactly which row version each write replaced.
package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// KeyMode decides how primary key values are stored.
type KeyMode int

const (
	// HashKeys stores a short hash of the key. Equal keys still match, but
	// the real values (which may be emails or other personal data) never
	// leave your app. This is the default.
	HashKeys KeyMode = iota
	// RawKeys stores key values as text, joined with "|". Easier to read
	// in alerts. Use it only when keys are not sensitive.
	RawKeys
)

// Config configures a Monitor.
type Config struct {
	// Tables to watch, like "accounts" or "billing.ledger". Each must be a
	// normal table with a primary key.
	Tables []string
	// Sink receives each committed transaction. Required.
	Sink Sink
	// KeyMode decides how keys are stored. Default: HashKeys.
	KeyMode KeyMode
	// MaxRetries is how many times Do retries a protected transaction that
	// PostgreSQL rejected with a serialization failure. Default: 20.
	MaxRetries int
}

// Querier is the small part of pgx (Conn or Pool) that New needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TxStarter starts transactions. *pgx.Conn and *pgxpool.Pool both work.
type TxStarter interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Stats are running counters for a Monitor.
type Stats struct {
	Committed int64            // transactions sent to the sink
	Retried   int64            // retries of protected transactions
	Protected []string         // names currently protected
	Tracked   int64            // statements rewritten and recorded
	Untracked int64            // statements run unchanged
	Reasons   map[string]int64 // why statements were untracked
}

// Monitor holds the watched tables and settings. It is safe to share.
type Monitor struct {
	cfg        Config
	tables     map[string]*rewrite.Table // "schema.name" -> table
	defaultIso string

	planMu sync.RWMutex
	plans  map[string]*rewrite.Plan

	committed, tracked, untracked, retried atomic.Int64
	reasonMu                               sync.Mutex
	protected                              sync.Map // name -> struct{}
	reasons                                map[string]int64
}

// ErrUnsupportedServer is returned when PostgreSQL is older than 18.
var ErrUnsupportedServer = errors.New("isowatch: PostgreSQL 18 or newer is required (it needs RETURNING old.xmin)")

const tableInfoSQL = `
SELECT n.nspname, c.relname, c.relkind::text,
       coalesce((SELECT array_agg(a.attname::text ORDER BY k.ord)
                 FROM pg_index i
                 CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
                 WHERE i.indrelid = c.oid AND i.indisprimary), '{}'::text[]),
       coalesce(to_regclass(quote_ident(c.relname)) = c.oid, false)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = to_regclass($1)`

// New checks the database and the watched tables and returns a Monitor.
func New(ctx context.Context, db Querier, cfg Config) (*Monitor, error) {
	if cfg.Sink == nil {
		return nil, errors.New("isowatch: Config.Sink is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("isowatch: Config.Tables is empty")
	}
	var version int
	var iso string
	err := db.QueryRow(ctx, `SELECT current_setting('server_version_num')::int, current_setting('default_transaction_isolation')`).Scan(&version, &iso)
	if err != nil {
		return nil, fmt.Errorf("isowatch: reading server settings: %w", err)
	}
	if version < 180000 {
		return nil, ErrUnsupportedServer
	}
	m := &Monitor{
		cfg:        cfg,
		tables:     map[string]*rewrite.Table{},
		defaultIso: iso,
		plans:      map[string]*rewrite.Plan{},
		reasons:    map[string]int64{},
	}
	for _, name := range cfg.Tables {
		var schema, rel, kind string
		var pk []string
		var unqualified bool
		err := db.QueryRow(ctx, tableInfoSQL, name).Scan(&schema, &rel, &kind, &pk, &unqualified)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("isowatch: table %q not found", name)
		}
		if err != nil {
			return nil, fmt.Errorf("isowatch: reading table %q: %w", name, err)
		}
		if kind != "r" {
			return nil, fmt.Errorf("isowatch: %q is not a plain table (views and partitioned tables are not supported yet)", name)
		}
		if len(pk) == 0 {
			return nil, fmt.Errorf("isowatch: table %q has no primary key", name)
		}
		t := &rewrite.Table{Schema: schema, Name: rel, PK: pk, Unqualified: unqualified}
		m.tables[t.QualifiedName()] = t
	}
	return m, nil
}

// Lookup implements rewrite.Catalog.
func (m *Monitor) Lookup(schema, name string) (*rewrite.Table, bool) {
	if schema != "" {
		t, ok := m.tables[schema+"."+name]
		return t, ok
	}
	for _, t := range m.tables {
		if t.Unqualified && t.Name == name {
			return t, true
		}
	}
	return nil, false
}

const planCacheSize = 2048

func (m *Monitor) plan(sql string) *rewrite.Plan {
	m.planMu.RLock()
	p, ok := m.plans[sql]
	m.planMu.RUnlock()
	if ok {
		return p
	}
	p = rewrite.Make(sql, m)
	m.planMu.Lock()
	if len(m.plans) >= planCacheSize {
		m.plans = map[string]*rewrite.Plan{} // simple reset keeps memory bounded
	}
	m.plans[sql] = p
	m.planMu.Unlock()
	return p
}

func (m *Monitor) countUntracked(reason string) {
	m.untracked.Add(1)
	m.reasonMu.Lock()
	m.reasons[reason]++
	m.reasonMu.Unlock()
}

// Stats returns a snapshot of the counters.
func (m *Monitor) Stats() Stats {
	m.reasonMu.Lock()
	r := make(map[string]int64, len(m.reasons))
	for k, v := range m.reasons {
		r[k] = v
	}
	m.reasonMu.Unlock()
	return Stats{
		Committed: m.committed.Load(),
		Retried:   m.retried.Load(),
		Protected: m.Protected(),
		Tracked:   m.tracked.Load(),
		Untracked: m.untracked.Load(),
		Reasons:   r,
	}
}

// key turns primary key values into the stored key.
func (m *Monitor) key(vals []string) string {
	if m.cfg.KeyMode == RawKeys {
		return strings.Join(vals, "|")
	}
	h := sha256.New()
	for _, v := range vals {
		fmt.Fprintf(h, "%d:%s;", len(v), v) // length prefix avoids ambiguity
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func isoName(l pgx.TxIsoLevel) string {
	switch l {
	case pgx.Serializable:
		return "serializable"
	case pgx.RepeatableRead:
		return "repeatable read"
	case pgx.ReadCommitted, pgx.ReadUncommitted:
		// PostgreSQL runs read uncommitted as read committed.
		return "read committed"
	}
	return ""
}
