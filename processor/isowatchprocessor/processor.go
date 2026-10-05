package isowatchprocessor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

type isowatch struct {
	logger  *zap.Logger
	cfg     *Config
	next    consumer.Traces
	checker *checker.Checker

	stop chan struct{}
	wg   sync.WaitGroup
}

func newProcessor(logger *zap.Logger, cfg *Config, next consumer.Traces) *isowatch {
	group := checker.ByTransaction
	if cfg.GroupBy == "trace" {
		group = checker.ByTrace
	}
	return &isowatch{
		logger: logger,
		cfg:    cfg,
		next:   next,
		checker: checker.New(checker.Options{
			Window:       cfg.Window,
			GroupBy:      group,
			MaxDepth:     cfg.MaxDepth,
			IgnoreTables: cfg.IgnoreTables,
			IgnoreNames:  cfg.IgnoreNames,
		}),
		stop: make(chan struct{}),
	}
}

func (p *isowatch) Start(context.Context, component.Host) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		tick := time.NewTicker(p.cfg.EvictInterval)
		defer tick.Stop()
		for {
			select {
			case <-p.stop:
				return
			case now := <-tick.C:
				p.checker.Evict(now)
			}
		}
	}()
	return nil
}

func (p *isowatch) Shutdown(context.Context) error {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.wg.Wait()
	return nil
}

func (p *isowatch) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// found pairs an anomaly with the span that completed it.
type found struct {
	anomaly checker.Anomaly
	trace   pcommon.TraceID
	parent  pcommon.SpanID
}

func (p *isowatch) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var all []found
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Name() != history.SpanName {
					continue
				}
				txn, err := decode(span)
				if err != nil {
					p.logger.Debug("skipping bad isowatch span", zap.Error(err))
					continue
				}
				for _, a := range p.checker.Add(txn) {
					all = append(all, found{anomaly: a, trace: span.TraceID(), parent: span.SpanID()})
				}
			}
			if p.cfg.DropTxnSpans {
				spans.RemoveIf(func(s ptrace.Span) bool { return s.Name() == history.SpanName })
			}
		}
	}
	if len(all) > 0 {
		p.appendAnomalies(td, all)
	}
	return p.next.ConsumeTraces(ctx, td)
}

// decode turns an isowatch.txn span back into a transaction.
func decode(span ptrace.Span) (*history.Txn, error) {
	attrs := span.Attributes()
	t := &history.Txn{
		Start:   span.StartTimestamp().AsTime(),
		Commit:  span.EndTimestamp().AsTime(),
		TraceID: span.TraceID().String(),
		SpanID:  span.SpanID().String(),
	}
	if v, ok := attrs.Get(history.AttrTxnID); ok {
		t.ID = v.Str()
	}
	if t.ID == "" {
		t.ID = span.SpanID().String()
	}
	if v, ok := attrs.Get(history.AttrTxnName); ok {
		t.Name = v.Str()
	}
	if v, ok := attrs.Get(history.AttrIsolation); ok {
		t.Isolation = v.Str()
	}
	if v, ok := attrs.Get(history.AttrXids); ok && v.Type() == pcommon.ValueTypeSlice {
		s := v.Slice()
		for i := 0; i < s.Len(); i++ {
			t.Xids = append(t.Xids, uint32(s.At(i).Int()))
		}
	}
	v, ok := attrs.Get(history.AttrHistory)
	if !ok {
		return nil, errMissingHistory
	}
	if err := history.DecodeHistory(v.Str(), t); err != nil {
		return nil, err
	}
	return t, nil
}

type decodeError string

func (e decodeError) Error() string { return string(e) }

const errMissingHistory = decodeError("isowatch: span has no " + history.AttrHistory + " attribute")

type edgeJSON struct {
	From  string `json:"from_trace"`
	To    string `json:"to_trace"`
	Kind  string `json:"kind"`
	Table string `json:"table"`
	Key   string `json:"key"`
	Xmin  uint32 `json:"xmin"`
}

// appendAnomalies adds one isowatch.anomaly span per anomaly, in the trace of
// the transaction that completed the cycle, with links to the others.
func (p *isowatch) appendAnomalies(td ptrace.Traces, all []found) {
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "isowatch")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("github.com/DCchoudhury15/isowatch/processor/isowatchprocessor")
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, f := range all {
		a := f.anomaly
		span := ss.Spans().AppendEmpty()
		span.SetName(history.AnomalySpan)
		span.SetTraceID(f.trace)
		span.SetParentSpanID(f.parent)
		span.SetSpanID(newSpanID())
		span.SetStartTimestamp(now)
		span.SetEndTimestamp(now)
		span.SetKind(ptrace.SpanKindInternal)
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage(a.Summary)

		attrs := span.Attributes()
		attrs.PutStr("isowatch.anomaly.kind", string(a.Kind))
		attrs.PutBool("isowatch.anomaly.unexpected", a.Unexpected)
		attrs.PutStr("isowatch.anomaly.summary", a.Summary)
		attrs.PutStr("isowatch.anomaly.fix", a.Fix)
		if names := a.Names(); len(names) > 0 {
			ns := attrs.PutEmptySlice("isowatch.anomaly.names")
			for _, n := range names {
				ns.AppendEmpty().SetStr(n)
			}
		}
		if b, err := json.Marshal(a.Steps); err == nil {
			attrs.PutStr("isowatch.anomaly.steps", string(b))
		}
		if a.SkippedVersions > 0 {
			attrs.PutInt("isowatch.anomaly.skipped_versions", int64(a.SkippedVersions))
		}
		traces := attrs.PutEmptySlice("isowatch.anomaly.traces")
		for _, id := range a.TraceIDs() {
			traces.AppendEmpty().SetStr(id)
		}
		var edges []edgeJSON
		for _, e := range a.Edges {
			edges = append(edges, edgeJSON{
				From: traceOf(a, e.From), To: traceOf(a, e.To), Kind: string(e.Kind),
				Table: e.Version.Table, Key: e.Version.Key, Xmin: e.Version.Xmin,
			})
		}
		if b, err := json.Marshal(edges); err == nil {
			attrs.PutStr("isowatch.anomaly.edges", string(b))
		}

		// Link to every transaction span in the cycle, so you can jump from
		// the alert to each request involved.
		for _, t := range a.Txns {
			tid, err1 := parseTraceID(t.TraceID)
			sid, err2 := parseSpanID(t.SpanID)
			if err1 != nil || err2 != nil {
				continue
			}
			l := span.Links().AppendEmpty()
			l.SetTraceID(tid)
			l.SetSpanID(sid)
			l.Attributes().PutStr("isowatch.txn.id", t.ID)
		}

		p.logger.Warn("isolation anomaly",
			zap.String("kind", string(a.Kind)),
			zap.Bool("unexpected", a.Unexpected),
			zap.Strings("traces", a.TraceIDs()),
			zap.Strings("names", a.Names()),
			zap.String("summary", a.Summary),
			zap.String("fix", a.Fix))
	}
}

func traceOf(a checker.Anomaly, node string) string {
	for i, e := range a.Edges {
		if e.From == node && i < len(a.Txns) {
			return a.Txns[i].TraceID
		}
	}
	return ""
}

func newSpanID() pcommon.SpanID {
	var id pcommon.SpanID
	_, _ = rand.Read(id[:])
	return id
}
