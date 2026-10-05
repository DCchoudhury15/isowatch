package capture

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
)

// rows wraps pgx.Rows and hides the columns IsoWatch added. Your code sees
// exactly the columns it asked for.
type rows struct {
	inner pgx.Rows
	t     *Tx
	plan  *rewrite.Plan
	stmt  int
	q     int
}

var _ pgx.Rows = (*rows)(nil)

func (r *rows) visible(n int) int {
	if n -= r.plan.Hidden; n < 0 {
		return 0
	}
	return n
}

func (r *rows) Close()                        { r.inner.Close() }
func (r *rows) Err() error                    { return r.inner.Err() }
func (r *rows) CommandTag() pgconn.CommandTag { return r.inner.CommandTag() }
func (r *rows) Conn() *pgx.Conn               { return r.inner.Conn() }
func (r *rows) TypeMap() *pgtype.Map          { return r.inner.TypeMap() }

func (r *rows) FieldDescriptions() []pgconn.FieldDescription {
	fd := r.inner.FieldDescriptions()
	return fd[:r.visible(len(fd))]
}

// Next moves to the next row and records its hidden columns.
func (r *rows) Next() bool {
	if !r.inner.Next() {
		return false
	}
	raw := r.inner.RawValues()
	if len(raw) >= r.plan.Hidden {
		// record copies what it needs; raw is only valid until the next call.
		r.t.record(r.plan, r.stmt, r.q, raw[len(raw)-r.plan.Hidden:])
	}
	return true
}

// Scan fills dest with the visible columns. nil destinations make pgx skip
// the hidden columns.
func (r *rows) Scan(dest ...any) error {
	all := make([]any, len(dest)+r.plan.Hidden)
	copy(all, dest)
	return r.inner.Scan(all...)
}

func (r *rows) Values() ([]any, error) {
	v, err := r.inner.Values()
	if err != nil {
		return nil, err
	}
	return v[:r.visible(len(v))], nil
}

func (r *rows) RawValues() [][]byte {
	raw := r.inner.RawValues()
	return raw[:r.visible(len(raw))]
}

// row is what QueryRow returns.
type row struct {
	rows pgx.Rows
	err  error
}

func (r *row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}
