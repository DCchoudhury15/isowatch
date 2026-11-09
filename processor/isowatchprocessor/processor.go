package isowatchprocessor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/DCchoudhury15/isowatch/checker"
	"github.com/DCchoudhury15/isowatch/history"
)

type isowatch struct {
	logger  *zap.Logger
	cfg     *Config
	next    consumer.Traces
	checker *checker.Checker
	issues  *issues
	tel     *telemetry
	hook    *webhook   // nil when no webhook is configured
	prot    *protector // nil when protection is off
	now     func() time.Time

	// Lost writes are measured on the evict timer. See measureLostWrites.
	lostDirty atomic.Bool
	lostBase  int
	lastSup   int64

	stop chan struct{}
	wg   sync.WaitGroup
}

func newProcessor(set processor.Settings, cfg *Config, next consumer.Traces) (*isowatch, error) {
	group := checker.ByTransaction
	if cfg.GroupBy == "trace" {
		group = checker.ByTrace
	}
	p := &isowatch{
		logger: set.Logger,
		cfg:    cfg,
		next:   next,
		checker: checker.New(checker.Options{
			Window:       cfg.Window,
			GroupBy:      group,
			MaxDepth:     cfg.MaxDepth,
			IgnoreTables: cfg.IgnoreTables,
			IgnoreNames:  cfg.IgnoreNames,
		}),
		issues: newIssues(cfg),
		now:    time.Now,
		stop:   make(chan struct{}),
	}
	tel, err := newTelemetry(set.MeterProvider, p)
	if err != nil {
		return nil, err
	}
	p.tel = tel
	if cfg.Webhook.URL != "" {
		p.hook = newWebhook(cfg.Webhook, set.Logger, tel)
	}
	if cfg.Protection.DSN != "" {
		if p.prot, err = newProtector(cfg.Protection, set.Logger, tel); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *isowatch) Start(context.Context, component.Host) error {
	if p.prot != nil {
		if err := p.prot.start(); err != nil {
			return err
		}
	}
	if p.hook != nil {
		p.hook.w.start()
	}
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
				p.tick(now)
			}
		}
	}()
	return nil
}

// tick runs on the evict timer: it measures lost writes, then forgets old
// transactions.
func (p *isowatch) tick(now time.Time) {
	p.measureLostWrites(context.Background())
	if p.checker.Evict(now) > 0 && p.lostBase > 0 {
		// Some lost writes may have left the window with the transactions
		// that were just forgotten. Start counting from what is left.
		p.lostBase = p.checker.LostWrites()
	}
}

// measureLostWrites adds new lost writes to isowatch_lost_writes and to the
// issue they belong to.
//
// The checker counts lost writes over all transactions in its window, and
// that count is expensive, so it is only taken after an anomaly was found.
// The increase since the last measurement is the number of new lost writes.
// It is labelled with the table when all issues seen since the last
// measurement are on one table ("several" otherwise), and added to the
// issue when there was only one. If an ignored anomaly happened in the same
// interval, the increase may belong to an ignored table, so it is not
// counted. This can make the number a little low, never high.
func (p *isowatch) measureLostWrites(ctx context.Context) {
	sup := p.checker.Stats().Suppressed
	dirty := p.lostDirty.Swap(false)
	if !dirty && sup == p.lastSup {
		return
	}
	lost := p.checker.LostWrites()
	active := p.issues.takeActive()
	if dirty && sup == p.lastSup && lost > p.lostBase && len(active) > 0 {
		n := int64(lost - p.lostBase)
		tables := map[string]bool{}
		for _, is := range active {
			for _, t := range is.Tables {
				tables[t] = true
			}
		}
		label := "several"
		if len(tables) == 1 {
			for t := range tables {
				label = t
			}
		}
		p.tel.lost(ctx, label, n)
		if len(active) == 1 {
			p.issues.addLostWrites(active[0].ID, n)
		}
	}
	p.lostBase = lost
	p.lastSup = sup
}

func (p *isowatch) Shutdown(ctx context.Context) error {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.wg.Wait()
	if p.hook != nil {
		p.hook.w.shutdown(ctx)
	}
	if p.prot != nil {
		p.prot.shutdown(ctx)
	}
	p.tel.shutdown()
	return nil
}

func (p *isowatch) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// found pairs an anomaly with the span it is reported under.
type found struct {
	anomaly checker.Anomaly
	issue   seen
	trace   pcommon.TraceID
	parent  pcommon.SpanID
}

func (p *isowatch) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var all []found
	var txns, bad int64
	drop := p.cfg.DropTxnSpans
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			hasTxn := false
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Name() != history.SpanName {
					continue
				}
				hasTxn = true
				txn, err := decode(span)
				if err != nil {
					bad++
					p.logger.Debug("skipping bad isowatch span", zap.Error(err))
					continue
				}
				// The anomaly span is a child of the isowatch.txn span. If
				// that span is dropped, it hangs from the request span that
				// ran the transaction instead, and links point there too.
				parent := span.SpanID()
				if drop {
					parent = span.ParentSpanID()
					txn.SpanID = ""
					if !parent.IsEmpty() {
						txn.SpanID = parent.String()
					}
				}
				txns++
				for _, a := range p.checker.Add(txn) {
					s := p.observe(ctx, a)
					if s.span {
						all = append(all, found{anomaly: a, issue: s, trace: span.TraceID(), parent: parent})
					}
				}
			}
			if drop && hasTxn {
				spans.RemoveIf(func(s ptrace.Span) bool { return s.Name() == history.SpanName })
			}
		}
		if drop {
			sss.RemoveIf(func(ss ptrace.ScopeSpans) bool { return ss.Spans().Len() == 0 })
		}
	}
	if drop {
		rss.RemoveIf(func(rs ptrace.ResourceSpans) bool { return rs.ScopeSpans().Len() == 0 })
	}
	if txns > 0 {
		p.tel.transactions.Add(ctx, txns)
	}
	if bad > 0 {
		p.tel.decodeErrors.Add(ctx, bad)
	}
	if len(all) > 0 {
		p.appendAnomalies(td, all)
	}
	if td.ResourceSpans().Len() == 0 {
		return nil // only isowatch.txn spans, all dropped
	}
	return p.next.ConsumeTraces(ctx, td)
}

// observe handles one anomaly: it updates its issue and the metrics, and
// queues the webhook and protection work. It reports whether the anomaly
// gets a span.
func (p *isowatch) observe(ctx context.Context, a checker.Anomaly) seen {
	g := signature(a)
	s := p.issues.observe(g, p.now())
	p.tel.anomaly(ctx, g)
	p.lostDirty.Store(true)
	if p.prot != nil {
		p.prot.protect(ctx, a, g.id)
	}
	if p.hook != nil && s.notify {
		p.hook.notify(ctx, s, a)
	}
	return s
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
		if !f.parent.IsEmpty() {
			span.SetParentSpanID(f.parent)
		}
		span.SetSpanID(newSpanID())
		span.SetStartTimestamp(now)
		span.SetEndTimestamp(now)
		span.SetKind(ptrace.SpanKindInternal)
		span.Status().SetCode(ptrace.StatusCodeError)
		span.Status().SetMessage(a.Summary)

		attrs := span.Attributes()
		is := f.issue.issue
		attrs.PutStr("isowatch.issue.id", is.ID)
		attrs.PutInt("isowatch.issue.count", is.Count)
		attrs.PutStr("isowatch.issue.first_seen", is.FirstSeen.UTC().Format(time.RFC3339Nano))
		attrs.PutBool("isowatch.issue.new", f.issue.isNew)
		if is.Unreported > 0 {
			attrs.PutInt("isowatch.issue.unreported", is.Unreported)
		}
		if is.LostWrites > 0 {
			attrs.PutInt("isowatch.issue.lost_writes", is.LostWrites)
		}
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
			zap.String("issue", is.ID),
			zap.Int64("count", is.Count),
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
