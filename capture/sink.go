package capture

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
type OTelSink struct {
	tracer trace.Tracer
}

// NewOTelSink uses the given tracer provider, or the global one if nil.
func NewOTelSink(tp trace.TracerProvider) *OTelSink {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return &OTelSink{tracer: tp.Tracer("github.com/DCchoudhury15/isowatch/capture")}
}

// Emit records the transaction as a span.
func (s *OTelSink) Emit(ctx context.Context, txn *history.Txn) {
	data, err := history.EncodeHistory(txn)
	if err != nil {
		return
	}
	xids := make([]int64, len(txn.Xids))
	for i, x := range txn.Xids {
		xids[i] = int64(x)
	}
	_, span := s.tracer.Start(ctx, history.SpanName,
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
