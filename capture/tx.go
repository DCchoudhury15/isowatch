package capture

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
	"github.com/DCchoudhury15/isowatch/history"
)

// ErrTxControl is returned when BEGIN, COMMIT or ROLLBACK is sent as SQL.
// Use Tx.Commit and Tx.Rollback instead, so IsoWatch knows the outcome.
var ErrTxControl = errors.New("isowatch: use Tx.Commit or Tx.Rollback instead of sending transaction control SQL")

type savepoint struct {
	name   string
	writes int
}

// Tx is a pgx transaction that records what it reads and writes.
// Like pgx.Tx, it must not be used from several goroutines at once.
type Tx struct {
	m        *Monitor
	tx       pgx.Tx
	beginCtx context.Context
	txn      history.Txn
	stmt     int
	queries  map[string]int
	sps      []savepoint
	done     bool
}

// Option changes how one transaction is started.
type Option func(*options)

type options struct {
	name string
}

// WithName labels the transaction, for example "deposit". The name shows up
// in alerts, can be ignored with checker.Options.IgnoreNames, and is what
// Protect works on. Use one name per kind of transaction, not per request.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

func buildOptions(o []Option) options {
	var out options
	for _, f := range o {
		f(&out)
	}
	return out
}

// Begin starts a transaction. The span in ctx (if any) becomes the parent
// of the IsoWatch span, which links anomalies to your request traces.
//
// If the transaction's name is protected (see Protect), it runs at
// SERIALIZABLE. Begin does not retry; use Do for automatic retries.
func (m *Monitor) Begin(ctx context.Context, db TxStarter, opts pgx.TxOptions, o ...Option) (*Tx, error) {
	bo := buildOptions(o)
	if bo.name != "" && m.IsProtected(bo.name) {
		opts.IsoLevel = pgx.Serializable
	}
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	iso := isoName(opts.IsoLevel)
	if iso == "" {
		iso = m.defaultIso
	}
	t := &Tx{
		m:        m,
		tx:       tx,
		beginCtx: ctx,
		queries:  map[string]int{},
		txn: history.Txn{
			ID:        newID(),
			Name:      bo.name,
			Isolation: iso,
			Start:     time.Now(),
		},
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		t.txn.TraceID = sc.TraceID().String()
		t.txn.SpanID = sc.SpanID().String()
	}
	return t, nil
}

// Do runs fn in a transaction and commits it, or rolls back if fn fails.
//
// If the transaction's name is protected, it runs at SERIALIZABLE and Do
// retries it when PostgreSQL rejects it with a serialization failure
// (40001) or a deadlock (40P01). fn may then run more than once, so it must
// not do things outside the database that cannot be repeated (like sending
// an email) before the transaction commits.
func (m *Monitor) Do(ctx context.Context, db TxStarter, opts pgx.TxOptions, fn func(*Tx) error, o ...Option) error {
	bo := buildOptions(o)
	retries := 0
	if bo.name != "" && m.IsProtected(bo.name) {
		retries = m.maxRetries()
	}
	for attempt := 0; ; attempt++ {
		err := m.doOnce(ctx, db, opts, fn, o)
		if err == nil || attempt >= retries || !retryable(err) {
			return err
		}
		m.retried.Add(1)
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
	}
}

func (m *Monitor) doOnce(ctx context.Context, db TxStarter, opts pgx.TxOptions, fn func(*Tx) error, o []Option) error {
	tx, err := m.Begin(ctx, db, opts, o...)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Raw returns the underlying pgx.Tx. Statements sent through it are not
// tracked.
func (t *Tx) Raw() pgx.Tx { return t.tx }

// Conn returns the underlying connection.
func (t *Tx) Conn() *pgx.Conn { return t.tx.Conn() }

// prepare expands pgx query options and decides how to run a statement.
func (t *Tx) prepare(ctx context.Context, sql string, args []any) (string, []any, *rewrite.Plan, error) {
	var lead []any
	var rewriter pgx.QueryRewriter
	for len(args) > 0 {
		switch a := args[0].(type) {
		case pgx.QueryExecMode:
			lead = append(lead, a)
		case pgx.QueryRewriter:
			rewriter = a
		case pgx.QueryResultFormats, pgx.QueryResultFormatsByOID:
			// Custom result formats would not cover our hidden columns, so
			// run this statement as is.
			return sql, append(append(lead, a), args[1:]...), &rewrite.Plan{Kind: rewrite.Untracked, SQL: sql, Reason: "custom result formats"}, nil
		default:
			goto done
		}
		args = args[1:]
	}
done:
	if rewriter != nil {
		var err error
		sql, args, err = rewriter.RewriteQuery(ctx, t.tx.Conn(), sql, args)
		if err != nil {
			return "", nil, nil, fmt.Errorf("rewrite query failed: %w", err)
		}
	}
	p := t.m.plan(sql)
	return p.SQL, append(lead, args...), p, nil
}

// after updates savepoint and isolation bookkeeping once a statement that
// was run unchanged has succeeded.
func (t *Tx) after(p *rewrite.Plan) {
	switch p.Kind {
	case rewrite.Savepoint:
		t.sps = append(t.sps, savepoint{name: p.Name, writes: len(t.txn.Writes)})
	case rewrite.Release:
		for i := len(t.sps) - 1; i >= 0; i-- {
			if t.sps[i].name == p.Name {
				t.sps = t.sps[:i]
				break
			}
		}
	case rewrite.RollbackTo:
		// Writes made after the savepoint were undone, so forget them.
		// Reads stay: the application may still have used what it read.
		for i := len(t.sps) - 1; i >= 0; i-- {
			if t.sps[i].name == p.Name {
				t.txn.Writes = t.txn.Writes[:t.sps[i].writes]
				t.sps = t.sps[:i+1]
				break
			}
		}
	}
	if p.SetsIsolation {
		t.txn.Isolation = "" // no longer sure, so never call anything "unexpected"
	}
}

// queryIndex stores normalized SQL once per transaction.
func (t *Tx) queryIndex(original string) int {
	if q, ok := t.queries[original]; ok {
		return q
	}
	q := len(t.txn.Statements)
	t.txn.Statements = append(t.txn.Statements, rewrite.Normalize(original))
	t.queries[original] = q
	return q
}

// Exec runs a statement and returns its command tag.
func (t *Tx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	run, runArgs, p, err := t.prepare(ctx, sql, args)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	switch p.Kind {
	case rewrite.Forbidden:
		return pgconn.CommandTag{}, ErrTxControl
	case rewrite.Read, rewrite.Write:
		rows, err := t.query(ctx, sql, run, runArgs, p)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		for rows.Next() {
		}
		rows.Close()
		return rows.CommandTag(), rows.Err()
	}
	if p.Kind == rewrite.Untracked {
		t.m.countUntracked(p.Reason)
	}
	tag, err := t.tx.Exec(ctx, run, runArgs...)
	if err == nil {
		t.after(p)
	}
	return tag, err
}

// Query runs a statement and returns its rows. Hidden columns are removed.
func (t *Tx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	run, runArgs, p, err := t.prepare(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	switch p.Kind {
	case rewrite.Forbidden:
		return nil, ErrTxControl
	case rewrite.Read, rewrite.Write:
		return t.query(ctx, sql, run, runArgs, p)
	}
	if p.Kind == rewrite.Untracked {
		t.m.countUntracked(p.Reason)
	}
	rows, err := t.tx.Query(ctx, run, runArgs...)
	if err != nil {
		return nil, err
	}
	if p.Kind == rewrite.Untracked && !p.SetsIsolation {
		return rows, nil
	}
	// Savepoint and SET commands return no rows. Read them to completion so
	// we only update our bookkeeping if they succeeded.
	for rows.Next() {
	}
	rows.Close()
	if rows.Err() == nil {
		t.after(p)
	}
	return rows, rows.Err()
}

func (t *Tx) query(ctx context.Context, original, run string, args []any, p *rewrite.Plan) (pgx.Rows, error) {
	inner, err := t.tx.Query(ctx, run, args...)
	if err != nil {
		return nil, err
	}
	t.m.tracked.Add(1)
	r := &rows{inner: inner, t: t, plan: p, stmt: t.stmt, q: t.queryIndex(original)}
	t.stmt++
	return r, nil
}

// QueryRow runs a statement and returns its first row.
func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rs, err := t.Query(ctx, sql, args...)
	return &row{rows: rs, err: err}
}

// Commit commits the transaction. Only a successful commit is reported.
func (t *Tx) Commit(ctx context.Context) error {
	if t.done {
		return pgx.ErrTxClosed
	}
	err := t.tx.Commit(ctx)
	t.done = true
	if err != nil {
		return err
	}
	t.txn.Commit = time.Now()
	t.m.committed.Add(1)
	if len(t.txn.Reads) > 0 || len(t.txn.Writes) > 0 {
		txn := t.txn
		t.m.cfg.Sink.Emit(t.beginCtx, &txn)
	}
	return nil
}

// Rollback rolls back the transaction. Nothing is reported.
func (t *Tx) Rollback(ctx context.Context) error {
	t.done = true
	return t.tx.Rollback(ctx)
}

// record turns the hidden column values of one row into reads or writes.
func (t *Tx) record(p *rewrite.Plan, stmt, q int, hidden [][]byte) {
	pos := 0
	for _, g := range p.Groups {
		n := 1 + len(g.Table.PK)
		switch p.Kind {
		case rewrite.Read:
			x, key, ok := t.decode(hidden[pos : pos+n])
			pos += n
			if !ok || t.txn.Owns(x) {
				continue // NULL from an outer join, or our own write
			}
			t.txn.Reads = append(t.txn.Reads, history.Read{
				Table: g.Table.QualifiedName(), Key: key, Xmin: x, Stmt: stmt, Q: q,
			})
		case rewrite.Write:
			oldX, oldKey, hasOld := t.decode(hidden[pos : pos+n])
			newX, newKey, hasNew := t.decode(hidden[pos+n : pos+2*n])
			pos += 2 * n
			if !hasOld && !hasNew {
				continue
			}
			w := history.Write{Table: g.Table.QualifiedName(), Stmt: stmt, Q: q}
			if hasOld {
				w.OldXmin, w.OldKey = oldX, oldKey
			}
			if hasNew {
				w.NewXmin, w.NewKey = newX, newKey
				if !t.txn.Owns(newX) {
					// The xmin of a version we just wrote is our own xid
					// (or a savepoint's sub-xid).
					t.txn.Xids = append(t.txn.Xids, newX)
				}
			}
			t.txn.Writes = append(t.txn.Writes, w)
		}
	}
}

// decode reads "xmin, pk1, pk2..." text values.
func (t *Tx) decode(vals [][]byte) (uint32, string, bool) {
	if len(vals) == 0 || vals[0] == nil {
		return 0, "", false
	}
	x, err := strconv.ParseUint(string(vals[0]), 10, 32)
	if err != nil {
		return 0, "", false
	}
	keys := make([]string, len(vals)-1)
	for i, v := range vals[1:] {
		keys[i] = string(v)
	}
	return uint32(x), t.m.key(keys), true
}
