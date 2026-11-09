package capture

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/DCchoudhury15/isowatch/history"
)

// sinkTxn is a small transaction with a read and a write on one row.
func sinkTxn(id string) *history.Txn {
	now := time.Now()
	return &history.Txn{
		ID:         id,
		Name:       "deposit",
		Xids:       []uint32{700},
		Isolation:  "read committed",
		Start:      now.Add(-time.Millisecond),
		Commit:     now,
		Reads:      []history.Read{{Table: "accounts", Key: "1", Xmin: 600, Stmt: 0, Q: 0}},
		Writes:     []history.Write{{Table: "accounts", OldKey: "1", OldXmin: 600, NewKey: "1", NewXmin: 700, Stmt: 1, Q: 1}},
		Statements: []string{"SELECT balance FROM accounts WHERE id = $1", "UPDATE accounts SET balance = $1 WHERE id = $2"},
	}
}

// unsampledRequest returns a context holding a request span that the app's
// sampler dropped, like most requests under head sampling.
func unsampledRequest(t *testing.T) (context.Context, trace.SpanContext) {
	t.Helper()
	app := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	ctx, span := app.Tracer("app").Start(context.Background(), "POST /deposit")
	t.Cleanup(func() { span.End() })
	sc := span.SpanContext()
	if !sc.IsValid() || sc.IsSampled() {
		t.Fatalf("test setup: want a valid, unsampled request span, got %+v", sc)
	}
	return ctx, sc
}

func historyAttr(t *testing.T, s tracetest.SpanStub) string {
	t.Helper()
	for _, kv := range s.Attributes {
		if string(kv.Key) == history.AttrHistory {
			return kv.Value.AsString()
		}
	}
	t.Fatalf("span %q has no %s attribute", s.Name, history.AttrHistory)
	return ""
}

// OTelSink goes through the app's sampler, so an unsampled request loses its
// isowatch.txn span. ExporterSink must keep it, in the request's trace.
func TestExporterSinkKeepsUnsampledRequests(t *testing.T) {
	ctx, req := unsampledRequest(t)

	appExp := tracetest.NewInMemoryExporter()
	app := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())), sdktrace.WithSyncer(appExp))
	NewOTelSink(app).Emit(ctx, sinkTxn("t1"))
	if n := len(appExp.GetSpans()); n != 0 {
		t.Fatalf("test setup: OTelSink should follow the app's sampler and drop the span, but %d were kept", n)
	}

	exp := tracetest.NewInMemoryExporter()
	sink := NewExporterSink(exp, ExporterOptions{})
	sink.Emit(ctx, sinkTxn("t1"))
	if err := sink.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("ExporterSink sent %d spans for an unsampled request, want 1", len(spans))
	}
	s := spans[0]
	if s.Name != history.SpanName {
		t.Errorf("span name %q, want %q", s.Name, history.SpanName)
	}
	if s.SpanContext.TraceID() != req.TraceID() {
		t.Errorf("span is in trace %s, want the request's trace %s", s.SpanContext.TraceID(), req.TraceID())
	}
	if s.Parent.SpanID() != req.SpanID() {
		t.Errorf("span parent %s, want the request span %s", s.Parent.SpanID(), req.SpanID())
	}
	if !s.SpanContext.IsSampled() {
		t.Error("the isowatch.txn span should be marked sampled, so the collector keeps it")
	}
}

// keepExporter records spans and keeps them after Shutdown
// (tracetest.InMemoryExporter clears them on Shutdown).
type keepExporter struct {
	mu    sync.Mutex
	names []string
}

func (k *keepExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, s := range spans {
		k.names = append(k.names, s.Name())
	}
	return nil
}

func (k *keepExporter) Shutdown(context.Context) error { return nil }

// Shutdown must send what is still queued, so a stopping app does not lose
// its last transactions.
func TestExporterSinkShutdownSendsPending(t *testing.T) {
	exp := &keepExporter{}
	sink := NewExporterSink(exp, ExporterOptions{BatchTimeout: time.Hour})
	for i := range 3 {
		sink.Emit(context.Background(), sinkTxn(fmt.Sprint("t", i)))
	}
	if err := sink.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(exp.names); n != 3 {
		t.Fatalf("after Shutdown %d spans were sent, want 3", n)
	}
}

// The OpenTelemetry environment variables can cap attribute length. A cut
// history cannot be decoded by the collector, so ExporterSink must ignore
// them and send the whole history.
func TestExporterSinkNeverCutsHistory(t *testing.T) {
	t.Setenv("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT", "64")
	t.Setenv("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", "64")

	txn := sinkTxn("big")
	for i := range 500 {
		txn.Reads = append(txn.Reads, history.Read{Table: "accounts", Key: fmt.Sprint(i + 2), Xmin: 600, Stmt: 2, Q: 0})
	}

	exp := tracetest.NewInMemoryExporter()
	sink := NewExporterSink(exp, ExporterOptions{})
	sink.Emit(context.Background(), txn)
	if err := sink.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	raw := historyAttr(t, spans[0])
	var got history.Txn
	if err := history.DecodeHistory(raw, &got); err != nil {
		t.Fatalf("history attribute (%d bytes) cannot be decoded, it was probably cut: %v", len(raw), err)
	}
	if len(got.Reads) != len(txn.Reads) {
		t.Fatalf("decoded %d reads, want %d", len(got.Reads), len(txn.Reads))
	}
}

// blockingExporter never finishes an export until released, like a
// collector that is down and a network that hangs.
type blockingExporter struct {
	release chan struct{}
	once    sync.Once
}

func (b *blockingExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil
}

func (b *blockingExporter) Shutdown(context.Context) error {
	b.once.Do(func() { close(b.release) })
	return nil
}

// When the collector is slow or down, Emit must drop spans, not wait. It
// runs on the app's request path after every commit.
func TestExporterSinkDropsInsteadOfBlocking(t *testing.T) {
	exp := &blockingExporter{release: make(chan struct{})}
	sink := NewExporterSink(exp, ExporterOptions{MaxQueueSize: 4, BatchTimeout: time.Millisecond})
	t.Cleanup(func() {
		_ = exp.Shutdown(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sink.Shutdown(ctx)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 2000 {
			sink.Emit(context.Background(), sinkTxn(fmt.Sprint("t", i)))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Emit blocked while the exporter was stuck: a down collector would stall the app")
	}
}

// otlpRequest is what the fake collector received.
type otlpRequest struct {
	path, encoding, tenant, contentType string
	body                                []byte
}

// NewOTLPSink must send OTLP/HTTP protobuf to the configured endpoint and
// path, with the headers and gzip it was given.
func TestOTLPSinkSendsToCollector(t *testing.T) {
	got := make(chan otlpRequest, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- otlpRequest{
			path:        r.URL.Path,
			encoding:    r.Header.Get("Content-Encoding"),
			tenant:      r.Header.Get("X-Tenant"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	ctx, req := unsampledRequest(t)
	sink, err := NewOTLPSink(context.Background(), OTLPOptions{
		Endpoint: strings.TrimPrefix(srv.URL, "http://"),
		URLPath:  "/custom/v1/traces",
		Insecure: true,
		Headers:  map[string]string{"X-Tenant": "bank"},
		Gzip:     true,
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Shutdown(context.Background()) })

	sink.Emit(ctx, sinkTxn("t1"))
	if err := sink.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	var r otlpRequest
	select {
	case r = <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the collector received nothing")
	}
	if r.path != "/custom/v1/traces" {
		t.Errorf("path %q, want /custom/v1/traces", r.path)
	}
	if r.tenant != "bank" {
		t.Errorf("X-Tenant header %q, want %q", r.tenant, "bank")
	}
	if r.contentType != "application/x-protobuf" {
		t.Errorf("Content-Type %q, want application/x-protobuf", r.contentType)
	}
	if r.encoding != "gzip" {
		t.Fatalf("Content-Encoding %q, want gzip", r.encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}

	var msg coltrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(plain, &msg); err != nil {
		t.Fatal(err)
	}
	var names []string
	var hist, id, name string
	var traceID []byte
	for _, rs := range msg.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				names = append(names, s.Name)
				traceID = s.TraceId
				for _, kv := range s.Attributes {
					switch kv.Key {
					case history.AttrHistory:
						hist = kv.Value.GetStringValue()
					case history.AttrTxnID:
						id = kv.Value.GetStringValue()
					case history.AttrTxnName:
						name = kv.Value.GetStringValue()
					}
				}
			}
		}
	}
	if len(names) != 1 || names[0] != history.SpanName {
		t.Fatalf("collector got spans %q, want one %q", names, history.SpanName)
	}
	if want := req.TraceID(); !bytes.Equal(traceID, want[:]) {
		t.Errorf("span trace id %x, want the request's %s", traceID, want)
	}
	if id != "t1" || name != "deposit" {
		t.Errorf("span says txn %q named %q, want t1 named deposit", id, name)
	}
	var txn history.Txn
	if err := history.DecodeHistory(hist, &txn); err != nil {
		t.Fatal(err)
	}
	if len(txn.Reads) != 1 || len(txn.Writes) != 1 || len(txn.Statements) != 2 {
		t.Errorf("decoded %d reads, %d writes, %d statements, want 1, 1, 2", len(txn.Reads), len(txn.Writes), len(txn.Statements))
	}
}
