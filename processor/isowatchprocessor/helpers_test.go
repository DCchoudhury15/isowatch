package isowatchprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/DCchoudhury15/isowatch/history"
)

// testClock is a clock the tests move by hand.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// startProcessor creates and starts a processor with the given settings.
func startProcessor(t *testing.T, set processor.Settings, cfg *Config) (*isowatch, *consumertest.TracesSink, *testClock) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	tp, err := NewFactory().CreateTraces(context.Background(), set, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	p := tp.(*isowatch)
	clock := newTestClock()
	p.now = clock.Now
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, sink, clock
}

func nopSettings() processor.Settings { return processortest.NewNopSettings(typeStr) }

// ids for occurrence n, side 1 or 2 of the race.
func traceID(n, side int) pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint32(id[0:], uint32(n))
	id[4] = byte(side)
	id[15] = 0xAA
	return id
}

func txnSpanID(n, side int) pcommon.SpanID {
	var id pcommon.SpanID
	binary.BigEndian.PutUint32(id[0:], uint32(n))
	id[4] = byte(side)
	id[7] = 1
	return id
}

// requestSpanID is the span of the request that ran the transaction: the
// parent of the isowatch.txn span.
func requestSpanID(n, side int) pcommon.SpanID {
	id := txnSpanID(n, side)
	id[7] = 2
	return id
}

// race describes one occurrence of a lost update: two transactions read
// the same row version and both write it.
type race struct {
	table        string
	nameA, nameB string
	sel, upd     string // SQL of the read and of the write
}

var depositRace = race{
	table: "public.acct", nameA: "deposit", nameB: "deposit",
	sel: "SELECT balance FROM acct WHERE id = $1",
	upd: "UPDATE acct SET balance = $1 WHERE id = $2",
}

// addRequest adds the request span and the isowatch.txn span for one side.
func addRequest(td ptrace.Traces, n, side int, txn *history.Txn) {
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	req := spans.AppendEmpty()
	req.SetName("POST /deposit")
	req.SetTraceID(traceID(n, side))
	req.SetSpanID(requestSpanID(n, side))

	span := spans.AppendEmpty()
	span.SetName(history.SpanName)
	span.SetTraceID(traceID(n, side))
	span.SetSpanID(txnSpanID(n, side))
	span.SetParentSpanID(requestSpanID(n, side))
	now := time.Now()
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Millisecond)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	a := span.Attributes()
	a.PutInt(history.AttrSchema, history.SchemaVersion)
	a.PutStr(history.AttrTxnID, txn.ID)
	a.PutStr(history.AttrTxnName, txn.Name)
	a.PutStr(history.AttrIsolation, "read committed")
	xs := a.PutEmptySlice(history.AttrXids)
	for _, x := range txn.Xids {
		xs.AppendEmpty().SetInt(int64(x))
	}
	data, _ := history.EncodeHistory(txn)
	a.PutStr(history.AttrHistory, data)
}

// raceTraces builds occurrence n of a race on row n: A writes version b+1
// on top of b, then B, which also read b, replaces b+1. B lost A's write.
func raceTraces(n int, r race) ptrace.Traces {
	b := uint32(1000 + n*10)
	key := fmt.Sprint(n)
	a := &history.Txn{ID: fmt.Sprintf("T%d-a", n), Name: r.nameA, Xids: []uint32{b + 1},
		Statements: []string{r.sel, r.upd},
		Reads:      []history.Read{{Table: r.table, Key: key, Xmin: b, Stmt: 0, Q: 0}},
		Writes:     []history.Write{{Table: r.table, OldKey: key, OldXmin: b, NewKey: key, NewXmin: b + 1, Stmt: 1, Q: 1}}}
	bt := &history.Txn{ID: fmt.Sprintf("T%d-b", n), Name: r.nameB, Xids: []uint32{b + 2},
		Statements: []string{r.sel, r.upd},
		Reads:      []history.Read{{Table: r.table, Key: key, Xmin: b, Stmt: 0, Q: 0}},
		Writes:     []history.Write{{Table: r.table, OldKey: key, OldXmin: b + 1, NewKey: key, NewXmin: b + 2, Stmt: 1, Q: 1}}}
	td := ptrace.NewTraces()
	addRequest(td, n, 1, a)
	addRequest(td, n, 2, bt)
	return td
}

func sendRace(t *testing.T, p *isowatch, n int, r race) {
	t.Helper()
	if err := p.ConsumeTraces(context.Background(), raceTraces(n, r)); err != nil {
		t.Fatal(err)
	}
}

func strAttr(t *testing.T, s ptrace.Span, key string) string {
	t.Helper()
	v, ok := s.Attributes().Get(key)
	if !ok {
		t.Fatalf("span has no %s", key)
	}
	return v.Str()
}

func intAttr(t *testing.T, s ptrace.Span, key string) int64 {
	t.Helper()
	v, ok := s.Attributes().Get(key)
	if !ok {
		t.Fatalf("span has no %s", key)
	}
	return v.Int()
}

func spansNamed(sink *consumertest.TracesSink, name string) []ptrace.Span {
	var out []ptrace.Span
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					if spans.At(k).Name() == name {
						out = append(out, spans.At(k))
					}
				}
			}
		}
	}
	return out
}
