package isowatchprocessor

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/DCchoudhury15/isowatch/history"
)

// addTxnSpan adds an isowatch.txn span like the capture library sends.
func addTxnSpan(td ptrace.Traces, traceByte, spanByte byte, txn *history.Txn) {
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName(history.SpanName)
	span.SetTraceID(pcommon.TraceID{traceByte, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	span.SetSpanID(pcommon.SpanID{spanByte, 1, 2, 3, 4, 5, 6, 7})
	now := time.Now()
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
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

func newTestProcessor(t *testing.T, cfg *Config) (*isowatch, *consumertest.TracesSink) {
	t.Helper()
	sink := new(consumertest.TracesSink)
	p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p.(*isowatch), sink
}

func anomalySpans(sink *consumertest.TracesSink) []ptrace.Span {
	var out []ptrace.Span
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					if spans.At(k).Name() == history.AnomalySpan {
						out = append(out, spans.At(k))
					}
				}
			}
		}
	}
	return out
}

func TestLostUpdateBecomesAnomalySpan(t *testing.T) {
	p, sink := newTestProcessor(t, createDefaultConfig())
	ctx := context.Background()

	// Same history PostgreSQL produced: both read xmin 760, T1 wrote 761,
	// T2 replaced 761 with 762. Sent in two separate batches.
	td1 := ptrace.NewTraces()
	addTxnSpan(td1, 0xA, 0x1, &history.Txn{ID: "T1", Name: "deposit", Xids: []uint32{761},
		Statements: []string{"SELECT balance FROM acct WHERE id = $1", "UPDATE acct SET balance = $1 WHERE id = $2"},
		Reads:      []history.Read{{Table: "public.acct", Key: "1", Xmin: 760, Stmt: 0, Q: 0}},
		Writes:     []history.Write{{Table: "public.acct", OldKey: "1", OldXmin: 760, NewKey: "1", NewXmin: 761, Stmt: 1, Q: 1}}})
	if err := p.ConsumeTraces(ctx, td1); err != nil {
		t.Fatal(err)
	}
	if n := len(anomalySpans(sink)); n != 0 {
		t.Fatalf("no anomaly yet, got %d", n)
	}

	td2 := ptrace.NewTraces()
	addTxnSpan(td2, 0xB, 0x2, &history.Txn{ID: "T2", Xids: []uint32{762},
		Statements: []string{"SELECT balance FROM acct WHERE id = $1", "UPDATE acct SET balance = $1 WHERE id = $2"},
		Reads:      []history.Read{{Table: "public.acct", Key: "1", Xmin: 760, Stmt: 0, Q: 0}},
		Writes:     []history.Write{{Table: "public.acct", OldKey: "1", OldXmin: 761, NewKey: "1", NewXmin: 762, Stmt: 1, Q: 1}}})
	if err := p.ConsumeTraces(ctx, td2); err != nil {
		t.Fatal(err)
	}

	got := anomalySpans(sink)
	if len(got) != 1 {
		t.Fatalf("want 1 anomaly span, got %d", len(got))
	}
	a := got[0]
	if k, _ := a.Attributes().Get("isowatch.anomaly.kind"); k.Str() != "lost-update" {
		t.Fatalf("kind = %q", k.Str())
	}
	if a.TraceID()[0] != 0xB || a.ParentSpanID()[0] != 0x2 {
		t.Fatal("anomaly must live in the trace of the transaction that completed the cycle")
	}
	if a.Links().Len() != 2 {
		t.Fatalf("want links to both transactions, got %d", a.Links().Len())
	}
	if a.Status().Code() != ptrace.StatusCodeError || !strings.Contains(a.Status().Message(), "Lost update") {
		t.Fatalf("status = %v %q", a.Status().Code(), a.Status().Message())
	}
	if fix, _ := a.Attributes().Get("isowatch.anomaly.fix"); !strings.Contains(fix.Str(), "FOR UPDATE") {
		t.Fatalf("fix = %q", fix.Str())
	}
	if names, ok := a.Attributes().Get("isowatch.anomaly.names"); !ok || names.Slice().At(0).Str() != "deposit" {
		t.Fatal("names should include deposit")
	}
	if steps, ok := a.Attributes().Get("isowatch.anomaly.steps"); !ok || !strings.Contains(steps.Str(), "SELECT balance") {
		t.Fatal("steps should carry the SQL")
	}
	tr, _ := a.Attributes().Get("isowatch.anomaly.traces")
	if tr.Slice().Len() != 2 {
		t.Fatalf("want 2 traces listed, got %d", tr.Slice().Len())
	}
	// The original spans are passed through untouched.
	if sink.SpanCount() != 3 {
		t.Fatalf("want 2 txn spans + 1 anomaly span, got %d", sink.SpanCount())
	}
}

func TestDropTxnSpans(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.DropTxnSpans = true
	p, sink := newTestProcessor(t, cfg)
	td := ptrace.NewTraces()
	addTxnSpan(td, 0xA, 0x1, &history.Txn{ID: "T1", Reads: []history.Read{{Table: "t", Key: "1", Xmin: 5}}})
	other := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().AppendEmpty()
	other.SetName("GET /orders")
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 1 {
		t.Fatalf("only the app span should remain, got %d spans", sink.SpanCount())
	}
}

func TestBadSpanIsSkipped(t *testing.T) {
	p, sink := newTestProcessor(t, createDefaultConfig())
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName(history.SpanName)
	span.Attributes().PutStr(history.AttrHistory, "{not json")
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 1 {
		t.Fatal("bad span must pass through without breaking the pipeline")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := createDefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := createDefaultConfig()
	bad.GroupBy = "request"
	if bad.Validate() == nil {
		t.Fatal("group_by must be checked")
	}
	bad = createDefaultConfig()
	bad.Window = 0
	if bad.Validate() == nil {
		t.Fatal("window must be checked")
	}
}

func TestIgnoreNamesFromConfig(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.IgnoreNames = []string{"deposit"}
	p, sink := newTestProcessor(t, cfg)
	td := ptrace.NewTraces()
	addTxnSpan(td, 0xA, 0x1, &history.Txn{ID: "T1", Name: "deposit", Xids: []uint32{761},
		Reads:  []history.Read{{Table: "public.acct", Key: "1", Xmin: 760}},
		Writes: []history.Write{{Table: "public.acct", OldKey: "1", OldXmin: 760, NewKey: "1", NewXmin: 761, Stmt: 1}}})
	addTxnSpan(td, 0xB, 0x2, &history.Txn{ID: "T2", Xids: []uint32{762},
		Reads:  []history.Read{{Table: "public.acct", Key: "1", Xmin: 760}},
		Writes: []history.Write{{Table: "public.acct", OldKey: "1", OldXmin: 761, NewKey: "1", NewXmin: 762, Stmt: 1}}})
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if n := len(anomalySpans(sink)); n != 0 {
		t.Fatalf("anomaly should be ignored, got %d", n)
	}
	if p.checker.Stats().Suppressed != 1 {
		t.Fatal("should count one suppressed anomaly")
	}
}
