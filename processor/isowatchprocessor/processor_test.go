package isowatchprocessor

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
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
	cfg := createDefaultConfig()
	cfg.DropTxnSpans = false // this test checks that the txn spans pass through
	p, sink := newTestProcessor(t, cfg)
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
	if id, ok := a.Attributes().Get("isowatch.issue.id"); !ok || len(id.Str()) != 16 {
		t.Fatal("every anomaly span needs an isowatch.issue.id")
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
	cfg := createDefaultConfig()
	cfg.DropTxnSpans = false // with dropping on, the bad span would be removed too
	p, sink := newTestProcessor(t, cfg)
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

func TestNewOptionsAreChecked(t *testing.T) {
	bad := map[string]func(c *Config){
		"report":           func(c *Config) { c.Report = "sometimes" },
		"issue_interval":   func(c *Config) { c.IssueInterval = 0 },
		"max_issues":       func(c *Config) { c.MaxIssues = 0 },
		"webhook.url":      func(c *Config) { c.Webhook.URL = "hooks.example.com/x" },
		"webhook.timeout":  func(c *Config) { c.Webhook.URL = "https://example.com/hook"; c.Webhook.Timeout = 0 },
		"protection.dsn":   func(c *Config) { c.Protection.DSN = "postgres://host:notaport/db" },
		"protection.table": func(c *Config) { c.Protection.DSN = "postgres://localhost/db"; c.Protection.Table = "" },
	}
	for name, mutate := range bad {
		c := createDefaultConfig()
		mutate(c)
		if c.Validate() == nil {
			t.Errorf("%s must be checked", name)
		}
	}
}

// The options load from collector YAML under the documented names.
func TestConfigFromYAML(t *testing.T) {
	if err := componenttest.CheckConfigStruct(createDefaultConfig()); err != nil {
		t.Fatal(err)
	}
	conf := confmap.NewFromStringMap(map[string]any{
		"report":         "every",
		"issue_interval": "30s",
		"max_issues":     50,
		"drop_txn_spans": false,
		"webhook":        map[string]any{"url": "https://hooks.example.com/abc", "timeout": "2s", "repeat": true},
		"protection":     map[string]any{"dsn": "postgres://isowatch@db:5432/app", "table": "app.protected"},
	})
	cfg := createDefaultConfig()
	if err := conf.Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Report != "every" || cfg.IssueInterval != 30*time.Second || cfg.MaxIssues != 50 || cfg.DropTxnSpans ||
		cfg.Webhook.URL != "https://hooks.example.com/abc" || cfg.Webhook.Timeout != 2*time.Second || !cfg.Webhook.Repeat ||
		cfg.Protection.DSN != "postgres://isowatch@db:5432/app" || cfg.Protection.Table != "app.protected" {
		t.Fatalf("config = %+v", cfg)
	}
	// Defaults for what is not set.
	def := createDefaultConfig()
	if err := confmap.NewFromStringMap(map[string]any{"webhook": map[string]any{"url": "https://x.example/h"}}).Unmarshal(def); err != nil {
		t.Fatal(err)
	}
	if def.Report != "per_issue" || def.IssueInterval != time.Minute || !def.DropTxnSpans ||
		def.Webhook.Timeout != 5*time.Second || def.Protection.Table != "isowatch_protected" {
		t.Fatalf("defaults = %+v", def)
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

// By default the isowatch.txn spans are removed after checking. The
// anomaly span then hangs from the request span that ran the transaction,
// and its links point at the request spans, so nothing points at a span the
// backend never receives.
func TestDropTxnSpansIsTheDefault(t *testing.T) {
	p, sink, _ := startProcessor(t, nopSettings(), createDefaultConfig())
	sendRace(t, p, 0, depositRace)

	if n := len(spansNamed(sink, history.SpanName)); n != 0 {
		t.Fatalf("isowatch.txn spans should be dropped by default, %d left", n)
	}
	if n := len(spansNamed(sink, "POST /deposit")); n != 2 {
		t.Fatalf("app spans must stay, got %d", n)
	}
	got := spansNamed(sink, history.AnomalySpan)
	if len(got) != 1 {
		t.Fatalf("want 1 anomaly span, got %d", len(got))
	}
	a := got[0]
	if a.TraceID() != traceID(0, 2) || a.ParentSpanID() != requestSpanID(0, 2) {
		t.Fatalf("anomaly parent = %s, want the request span %s", a.ParentSpanID(), requestSpanID(0, 2))
	}
	links := map[string]bool{}
	for i := 0; i < a.Links().Len(); i++ {
		links[a.Links().At(i).SpanID().String()] = true
	}
	if len(links) != 2 || !links[requestSpanID(0, 1).String()] || !links[requestSpanID(0, 2).String()] {
		t.Fatalf("links = %v, want both request spans", links)
	}
	// No empty resources or scopes are sent on.
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			if sss.Len() == 0 {
				t.Fatal("empty resource sent on")
			}
			for j := 0; j < sss.Len(); j++ {
				if sss.At(j).Spans().Len() == 0 {
					t.Fatal("empty scope sent on")
				}
			}
		}
	}
}

// A collector that only checks (the central checker) sends nothing on for
// batches that held only isowatch.txn spans and no anomaly.
func TestNothingSentWhenOnlyTxnSpansArrive(t *testing.T) {
	p, sink, _ := startProcessor(t, nopSettings(), createDefaultConfig())
	td := ptrace.NewTraces()
	addTxnSpan(td, 0xA, 0x1, &history.Txn{ID: "T1", Reads: []history.Read{{Table: "t", Key: "1", Xmin: 5}}})
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if n := len(sink.AllTraces()); n != 0 {
		t.Fatalf("want nothing sent on, got %d batches", n)
	}
}
