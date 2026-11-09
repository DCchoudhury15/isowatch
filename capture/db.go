package capture

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
	"github.com/DCchoudhury15/isowatch/history"
)

// DB is a pgxpool.Pool that IsoWatch watches. It has the same Exec, Query,
// QueryRow, Begin and BeginTx methods, so adopting IsoWatch can be one line:
//
//	db := mon.Wrap(pool)
//
// Each statement run directly on DB (outside a transaction) is its own
// transaction in PostgreSQL ("autocommit"), and IsoWatch records it as a
// one-statement transaction, with the trace and span from ctx. A request
// that reads a row with one statement and writes it with another is a
// lost update waiting to happen. Each statement on its own is fine, so
// check with checker.ByTrace (or group_by: trace in the collector) to see
// the whole request.
//
// Begin, BeginTx and Do return a tracked *Tx, the same as Monitor.Begin
// and Monitor.Do. DB is safe for concurrent use.
type DB struct {
	m    *Monitor
	pool *pgxpool.Pool
}

// Wrap returns pool with IsoWatch tracking. It does not take over the pool:
// closing either one closes the same pool.
func (m *Monitor) Wrap(pool *pgxpool.Pool) *DB {
	return &DB{m: m, pool: pool}
}

// Pool returns the underlying pool. Statements sent through it are not
// tracked.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Monitor returns the Monitor that tracks this DB.
func (db *DB) Monitor() *Monitor { return db.m }

type nameKey struct{}

// WithNameContext returns a ctx that names the statements and transactions
// run with it, for example "deposit", the same way WithName does for one
// transaction. Use it in a request handler, so every statement the request
// runs on a DB carries the name. An explicit WithName option wins.
func WithNameContext(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, nameKey{}, name)
}

// NameFromContext returns the name set by WithNameContext, or "".
func NameFromContext(ctx context.Context) string {
	s, _ := ctx.Value(nameKey{}).(string)
	return s
}

// Begin starts a tracked transaction, like Monitor.Begin.
func (db *DB) Begin(ctx context.Context, o ...Option) (*Tx, error) {
	return db.m.Begin(ctx, db.pool, pgx.TxOptions{}, o...)
}

// BeginTx starts a tracked transaction with options, like Monitor.Begin.
func (db *DB) BeginTx(ctx context.Context, opts pgx.TxOptions, o ...Option) (*Tx, error) {
	return db.m.Begin(ctx, db.pool, opts, o...)
}

// Do runs fn in a tracked transaction and commits it, like Monitor.Do
// (including the retries of protected transactions).
func (db *DB) Do(ctx context.Context, opts pgx.TxOptions, fn func(*Tx) error, o ...Option) error {
	return db.m.Do(ctx, db.pool, opts, fn, o...)
}

// Exec runs a statement and returns its command tag.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c, err := db.pool.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	run, runArgs, p, err := db.m.prepare(ctx, c.Conn(), sql, args)
	if err != nil {
		c.Release()
		return pgconn.CommandTag{}, err
	}
	if !tracks(p) {
		defer c.Release()
		if p.Kind == rewrite.Untracked {
			db.m.countUntracked(p)
		}
		return c.Exec(ctx, run, runArgs...)
	}
	rs, err := db.track(ctx, c, sql, run, runArgs, p)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	for rs.Next() {
	}
	rs.Close()
	return rs.CommandTag(), rs.Err()
}

// Query runs a statement and returns its rows. Hidden columns are removed.
// Close the rows (or read them to the end) to give the connection back,
// the same as with pgxpool.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c, err := db.pool.Acquire(ctx)
	if err != nil {
		return errRows{err}, err
	}
	run, runArgs, p, err := db.m.prepare(ctx, c.Conn(), sql, args)
	if err != nil {
		c.Release()
		return errRows{err}, err
	}
	if !tracks(p) {
		if p.Kind == rewrite.Untracked {
			db.m.countUntracked(p)
		}
		inner, err := c.Query(ctx, run, runArgs...)
		if err != nil {
			c.Release()
			return errRows{err}, err
		}
		return &stmtRows{Rows: inner, c: c}, nil
	}
	rs, err := db.track(ctx, c, sql, run, runArgs, p)
	if err != nil {
		return errRows{err}, err
	}
	return rs, nil
}

// QueryRow runs a statement and returns its first row.
func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rs, err := db.Query(ctx, sql, args...)
	return &row{rows: rs, err: err}
}

// SendBatch sends a batch through the pool. Batches are not tracked yet;
// each queued statement is counted as untracked.
func (db *DB) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	for _, q := range b.QueuedQueries {
		db.m.countUntracked(db.m.unchangedPlan(q.SQL, "batch (SendBatch)"))
	}
	return db.pool.SendBatch(ctx, b)
}

// CopyFrom copies rows into a table through the pool. COPY is not tracked;
// it is counted as untracked.
func (db *DB) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	db.m.countUntracked(db.m.unchangedPlan("COPY "+tableName.Sanitize()+" FROM STDIN", "COPY (CopyFrom)"))
	return db.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Ping checks that the database can be reached.
func (db *DB) Ping(ctx context.Context) error { return db.pool.Ping(ctx) }

// Close closes the underlying pool.
func (db *DB) Close() { db.pool.Close() }

// track runs a rewritten statement on c as a one-statement transaction.
// c is released when the returned rows are closed.
func (db *DB) track(ctx context.Context, c *pgxpool.Conn, original, run string, args []any, p *rewrite.Plan) (*stmtRows, error) {
	norm := p.Normalized
	if norm == "" {
		norm = rewrite.Normalize(original)
	}
	st := &Tx{m: db.m, txn: history.Txn{
		ID:   newID(),
		Name: NameFromContext(ctx),
		// A statement outside a transaction runs at the session default.
		Isolation:  db.m.defaultIso,
		Start:      time.Now(),
		Statements: []string{norm},
	}}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		st.txn.TraceID = sc.TraceID().String()
		st.txn.SpanID = sc.SpanID().String()
	}
	inner, err := c.Query(ctx, run, args...)
	if err != nil {
		c.Release()
		return nil, err
	}
	db.m.tracked.Add(1)
	return &stmtRows{
		Rows: &rows{inner: inner, t: st, plan: p},
		c:    c,
		done: func(err error) { db.m.finishStatement(ctx, st, err) },
	}, nil
}

// finishStatement reports a one-statement transaction once its rows are
// closed. In autocommit mode PostgreSQL commits the statement before it
// says the statement is complete, so no error means it committed. On an
// error nothing is reported: a missing transaction can hide an anomaly,
// but never invent one.
func (m *Monitor) finishStatement(ctx context.Context, st *Tx, err error) {
	if err != nil {
		return
	}
	st.txn.Commit = time.Now()
	m.committed.Add(1)
	if len(st.txn.Reads) > 0 || len(st.txn.Writes) > 0 {
		txn := st.txn
		m.cfg.Sink.Emit(ctx, &txn)
	}
}

// stmtRows are the rows of one statement run on the pool. Like pgxpool's
// rows, they give the connection back when they are closed, which also
// happens when Next returns false or Scan fails.
type stmtRows struct {
	pgx.Rows
	c      *pgxpool.Conn
	done   func(error) // nil for untracked statements
	closed bool
}

func (r *stmtRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.Close()
	return false
}

func (r *stmtRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	if err != nil {
		r.Close()
	}
	return err
}

func (r *stmtRows) Values() ([]any, error) {
	v, err := r.Rows.Values()
	if err != nil {
		r.Close()
	}
	return v, err
}

func (r *stmtRows) Close() {
	if r.closed {
		return
	}
	r.closed = true
	r.Rows.Close()
	err := r.Rows.Err()
	r.c.Release()
	if r.done != nil {
		r.done(err)
	}
}

// errRows is what Query returns with an error, like pgxpool does, so code
// that defers rows.Close() before checking the error keeps working.
type errRows struct{ err error }

func (errRows) Close()                                       {}
func (e errRows) Err() error                                 { return e.err }
func (errRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (errRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (errRows) Next() bool                                   { return false }
func (e errRows) Scan(...any) error                          { return e.err }
func (e errRows) Values() ([]any, error)                     { return nil, e.err }
func (errRows) RawValues() [][]byte                          { return nil }
func (errRows) Conn() *pgx.Conn                              { return nil }
func (errRows) TypeMap() *pgtype.Map                         { return nil }
