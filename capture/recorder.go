package capture

import (
	"context"
	"time"

	"github.com/DCchoudhury15/isowatch/capture/rewrite"
	"github.com/DCchoudhury15/isowatch/history"
)

// Recorder records one transaction from statements that some other program
// sends to PostgreSQL, like the IsoWatch proxy. Apps use Begin, Do or Wrap
// instead.
//
// The caller plans each statement with Monitor.Plan, sends the plan's SQL,
// and gives the Recorder the hidden values of each result row. It must
// remove the hidden columns before the app sees the rows. A Recorder is not
// safe for concurrent use.
type Recorder struct {
	t Tx
}

// Stmt identifies one run of a tracked statement within a Recorder.
type Stmt struct{ n, q int }

// NewRecorder starts recording a transaction. Its isolation level is the
// server default until the caller changes Txn().Isolation.
func (m *Monitor) NewRecorder() *Recorder {
	return &Recorder{t: Tx{
		m:       m,
		queries: map[string]int{},
		txn: history.Txn{
			ID:        newID(),
			Isolation: m.defaultIso,
			Start:     time.Now(),
		},
	}}
}

// Plan returns how to run one SQL statement. Plans are cached.
func (m *Monitor) Plan(sql string) *rewrite.Plan { return m.plan(sql) }

// CountUntracked counts a statement that was run unchanged, for Stats and
// the coverage report.
func (m *Monitor) CountUntracked(p *rewrite.Plan) { m.countUntracked(p) }

// DefaultIsolation returns default_transaction_isolation as the server
// reported it when the Monitor was created, like "read committed".
func (m *Monitor) DefaultIsolation() string { return m.defaultIso }

// Txn returns the transaction being recorded. The caller may set its Name,
// Isolation, TraceID and SpanID. Set Isolation to "" when it is not known
// for sure: IsoWatch then never calls an anomaly unexpected.
func (r *Recorder) Txn() *history.Txn { return &r.t.txn }

// Statement starts one run of a tracked statement (a Read, AggRead or Write
// plan). original is the SQL the app sent, before rewriting.
func (r *Recorder) Statement(original string, p *rewrite.Plan) Stmt {
	r.t.m.tracked.Add(1)
	s := Stmt{n: r.t.stmt, q: r.t.queryIndex(original, p.Normalized)}
	r.t.stmt++
	return s
}

// Row records the hidden values of one result row of s: the last p.Hidden
// values of the row, in text format. Row copies what it keeps.
func (r *Recorder) Row(p *rewrite.Plan, s Stmt, hidden [][]byte) {
	if len(hidden) != p.Hidden {
		return
	}
	r.t.record(p, s.n, s.q, hidden)
}

// Done updates the savepoint bookkeeping after a Savepoint, Release or
// RollbackTo statement succeeded: a rollback to a savepoint forgets the
// writes made after it.
func (r *Recorder) Done(p *rewrite.Plan) {
	switch p.Kind {
	case rewrite.Savepoint, rewrite.Release, rewrite.RollbackTo:
		r.t.after(p)
	}
}

// Empty reports whether nothing was recorded yet.
func (r *Recorder) Empty() bool {
	return len(r.t.txn.Reads) == 0 && len(r.t.txn.Writes) == 0
}

// Commit sends the transaction to the Monitor's sink, if it read or wrote
// a watched table. Call it only once PostgreSQL has confirmed the commit.
// ctx is passed to the sink: its span becomes the parent of the IsoWatch
// span.
func (r *Recorder) Commit(ctx context.Context) {
	r.t.txn.Commit = time.Now()
	r.t.m.committed.Add(1)
	if !r.Empty() {
		txn := r.t.txn
		r.t.m.cfg.Sink.Emit(ctx, &txn)
	}
}
