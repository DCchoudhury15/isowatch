package capture

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

// Sink receives every committed transaction that read or wrote a watched
// table. Emit is called on the goroutine that committed, so keep it fast.
type Sink interface {
	Emit(ctx context.Context, txn *history.Txn)
}

// SinkFunc turns a function into a Sink.
type SinkFunc func(ctx context.Context, txn *history.Txn)

// Emit calls f.
func (f SinkFunc) Emit(ctx context.Context, txn *history.Txn) { f(ctx, txn) }

// OTelSink sends each transaction as an OpenTelemetry span named
// "isowatch.txn". The span is a child of the span that was active when the
// transaction began, so it lands in the same trace as the request.
// The IsoWatch collector processor reads these spans.
//
// The span goes through your app's tracer provider, so your sampler decides
// whether it is kept. With head sampling at rate p, an anomaly between two
// transactions is only seen when both are kept, about p*p of the time. If
// you sample, use NewOTLPSink or NewExporterSink instead.
type OTelSink struct {
	tracer trace.Tracer
}

// NewOTelSink uses the given tracer provider, or the global one if nil.
func NewOTelSink(tp trace.TracerProvider) *OTelSink {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return &OTelSink{tracer: tp.Tracer(scopeName)}
}

// Emit records the transaction as a span.
func (s *OTelSink) Emit(ctx context.Context, txn *history.Txn) { emitSpan(ctx, s.tracer, txn) }

// scopeName is the instrumentation scope of the isowatch.txn spans.
const scopeName = "github.com/DCchoudhury15/isowatch/capture"

// emitSpan records one transaction as an isowatch.txn span, as a child of
// the span in ctx.
func emitSpan(ctx context.Context, tracer trace.Tracer, txn *history.Txn) {
	data, err := history.EncodeHistory(txn)
	if err != nil {
		return
	}
	xids := make([]int64, len(txn.Xids))
	for i, x := range txn.Xids {
		xids[i] = int64(x)
	}
	_, span := tracer.Start(ctx, history.SpanName,
		trace.WithTimestamp(txn.Start),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int(history.AttrSchema, history.SchemaVersion),
			attribute.String(history.AttrTxnID, txn.ID),
			attribute.String(history.AttrTxnName, txn.Name),
			attribute.String(history.AttrIsolation, txn.Isolation),
			attribute.Int64Slice(history.AttrXids, xids),
			attribute.String(history.AttrHistory, data),
		))
	span.End(trace.WithTimestamp(txn.Commit))
}

// ExporterSink sends each transaction as an "isowatch.txn" span, like
// OTelSink, but through a tracer provider of its own that keeps every span.
// Your app's sampler does not apply, so every transaction reaches the
// collector, and an anomaly between two requests is not missed because one
// of them was not sampled.
//
// The span still has the trace id of your request and the request's span as
// its parent, so anomalies link to your request traces. When the request
// itself was not sampled, its trace in your backend holds only the IsoWatch
// spans.
//
// Spans are sent in batches in the background. If the queue is full (the
// collector is slow or down), new spans are dropped instead of slowing your
// app. That can only hide an anomaly, never invent one. Call Shutdown when
// your app stops, to send what is left.
type ExporterSink struct {
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
}

// ExporterOptions configures an ExporterSink.
type ExporterOptions struct {
	// Resource describes your service on each span. Default:
	// resource.Default(), which reads OTEL_SERVICE_NAME and
	// OTEL_RESOURCE_ATTRIBUTES like the rest of OpenTelemetry.
	Resource *resource.Resource
	// MaxQueueSize is how many spans may wait to be sent. Default: 8192.
	MaxQueueSize int
	// BatchTimeout is the longest a span waits before it is sent.
	// Default: 1s.
	BatchTimeout time.Duration
}

// NewExporterSink sends isowatch.txn spans to exp (for example an OTLP gRPC
// exporter from otlptracegrpc.New) through a tracer provider that samples
// every span. It ignores the OTEL_TRACES_SAMPLER and span limit environment
// variables, because a dropped span hides anomalies and a cut history
// attribute cannot be read by the collector.
func NewExporterSink(exp sdktrace.SpanExporter, o ExporterOptions) *ExporterSink {
	res := o.Resource
	if res == nil {
		res = resource.Default()
	}
	queue := o.MaxQueueSize
	if queue <= 0 {
		queue = 8192
	}
	timeout := o.BatchTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{
			AttributeValueLengthLimit:   -1, // never cut the history
			AttributeCountLimit:         sdktrace.DefaultAttributeCountLimit,
			EventCountLimit:             sdktrace.DefaultEventCountLimit,
			LinkCountLimit:              sdktrace.DefaultLinkCountLimit,
			AttributePerEventCountLimit: sdktrace.DefaultAttributePerEventCountLimit,
			AttributePerLinkCountLimit:  sdktrace.DefaultAttributePerLinkCountLimit,
		}),
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxQueueSize(queue),
			sdktrace.WithBatchTimeout(timeout)),
	)
	return &ExporterSink{tp: tp, tracer: tp.Tracer(scopeName)}
}

// OTLPOptions configures NewOTLPSink.
type OTLPOptions struct {
	// Endpoint is the host:port of the OTLP/HTTP receiver of the collector
	// that runs the isowatch processor, like "otel-collector:4318". Default:
	// the OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT
	// environment variable, or else "localhost:4318".
	Endpoint string
	// URLPath is the path on the receiver. Default: "/v1/traces".
	URLPath string
	// Insecure uses plain HTTP instead of HTTPS.
	Insecure bool
	// Headers are sent with every request, for example for authentication.
	Headers map[string]string
	// Gzip compresses each batch. The history attribute is repetitive JSON,
	// so this makes requests several times smaller.
	Gzip bool
	// Timeout bounds each export request. Default: 10s.
	Timeout time.Duration

	ExporterOptions
}

// NewOTLPSink is NewExporterSink with an OTLP/HTTP exporter: every
// transaction is sent to the collector, whatever your app's sampler
// decides.
//
//	sink, err := capture.NewOTLPSink(ctx, capture.OTLPOptions{
//		Endpoint: "otel-collector:4318",
//		Insecure: true,
//	})
//	defer sink.Shutdown(context.Background())
func NewOTLPSink(ctx context.Context, o OTLPOptions) (*ExporterSink, error) {
	var opts []otlptracehttp.Option
	if o.Endpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpoint(o.Endpoint))
	}
	if o.URLPath != "" {
		opts = append(opts, otlptracehttp.WithURLPath(o.URLPath))
	}
	if o.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if len(o.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(o.Headers))
	}
	if o.Gzip {
		opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
	}
	if o.Timeout > 0 {
		opts = append(opts, otlptracehttp.WithTimeout(o.Timeout))
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return NewExporterSink(exp, o.ExporterOptions), nil
}

// Emit records the transaction as a span.
func (s *ExporterSink) Emit(ctx context.Context, txn *history.Txn) { emitSpan(ctx, s.tracer, txn) }

// ForceFlush sends every span that is waiting.
func (s *ExporterSink) ForceFlush(ctx context.Context) error { return s.tp.ForceFlush(ctx) }

// Shutdown sends every span that is waiting and stops the sink.
func (s *ExporterSink) Shutdown(ctx context.Context) error { return s.tp.Shutdown(ctx) }

// CheckerSink checks transactions inside your own process, with no
// collector needed. Good for tests, staging and small apps.
type CheckerSink struct {
	OnAnomaly func(checker.Anomaly)

	opts    checker.Options
	checker atomic.Pointer[checker.Checker]
	stop    chan struct{}
	once    sync.Once
}

// NewCheckerSink starts a CheckerSink. It forgets old transactions every
// few seconds. Call Close when done.
func NewCheckerSink(opts checker.Options, onAnomaly func(checker.Anomaly)) *CheckerSink {
	s := &CheckerSink{OnAnomaly: onAnomaly, opts: opts, stop: make(chan struct{})}
	s.checker.Store(checker.New(opts))
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-s.stop:
				return
			case now := <-tick.C:
				s.Checker().Evict(now)
			}
		}
	}()
	return s
}

// Emit adds the transaction to the checker and reports anomalies.
func (s *CheckerSink) Emit(_ context.Context, txn *history.Txn) {
	for _, a := range s.Checker().Add(txn) {
		if s.OnAnomaly != nil {
			s.OnAnomaly(a)
		}
	}
}

// Checker returns the current checker.
func (s *CheckerSink) Checker() *checker.Checker { return s.checker.Load() }

// Reset starts over with an empty checker.
func (s *CheckerSink) Reset() { s.checker.Store(checker.New(s.opts)) }

// Close stops the background eviction.
func (s *CheckerSink) Close() { s.once.Do(func() { close(s.stop) }) }

// MultiSink sends each transaction to several sinks.
type MultiSink []Sink

// Emit calls every sink.
func (m MultiSink) Emit(ctx context.Context, txn *history.Txn) {
	for _, s := range m {
		s.Emit(ctx, txn)
	}
}
