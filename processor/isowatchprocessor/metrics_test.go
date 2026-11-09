package isowatchprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/DCchoudhury15/isowatch/history"
)

// telemetrySettings returns processor settings whose metrics go to a
// manual reader, like the collector's own metrics endpoint would read them.
func telemetrySettings(t *testing.T) (*componenttest.Telemetry, processor.Settings) {
	t.Helper()
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	set := nopSettings()
	set.TelemetrySettings = tel.NewTelemetrySettings()
	return tel, set
}

// sumOf returns the value of a counter for one set of labels (all points
// added together when labels is nil).
func sumOf(t *testing.T, tel *componenttest.Telemetry, name string, labels ...attribute.KeyValue) int64 {
	t.Helper()
	m, err := tel.GetMetric(name)
	if err != nil {
		t.Fatal(err)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is %T, want an int64 sum", name, m.Data)
	}
	want := attribute.NewSet(labels...)
	var total int64
	for _, dp := range sum.DataPoints {
		if labels == nil || dp.Attributes.Equals(&want) {
			total += dp.Value
		}
	}
	return total
}

func gaugeOf(t *testing.T, tel *componenttest.Telemetry, name string) int64 {
	t.Helper()
	m, err := tel.GetMetric(name)
	if err != nil {
		t.Fatal(err)
	}
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok || len(g.DataPoints) != 1 {
		t.Fatalf("%s is %T with unexpected points", name, m.Data)
	}
	return g.DataPoints[0].Value
}

func TestMetricsCountEveryOccurrence(t *testing.T) {
	tel, set := telemetrySettings(t)
	p, sink, _ := startProcessor(t, set, createDefaultConfig())

	for n := 0; n < 3; n++ {
		sendRace(t, p, n, depositRace)
	}
	unnamed := depositRace
	unnamed.nameA, unnamed.nameB = "", ""
	sendRace(t, p, 3, unnamed)

	// One span that cannot be read.
	bad := ptrace.NewTraces()
	span := bad.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName(history.SpanName)
	span.Attributes().PutStr(history.AttrHistory, "{not json")
	if err := p.ConsumeTraces(context.Background(), bad); err != nil {
		t.Fatal(err)
	}

	// per_issue added only 2 spans, but the metrics count all 4 anomalies.
	if n := len(spansNamed(sink, history.AnomalySpan)); n != 2 {
		t.Fatalf("want 2 anomaly spans, got %d", n)
	}
	lost := []attribute.KeyValue{
		attribute.String("kind", "lost-update"), attribute.String("table", "public.acct"),
		attribute.String("txn_name", "deposit")}
	if got := sumOf(t, tel, metricAnomalies, lost...); got != 3 {
		t.Fatalf("isowatch_anomalies{deposit} = %d, want 3", got)
	}
	anon := []attribute.KeyValue{
		attribute.String("kind", "lost-update"), attribute.String("table", "public.acct"),
		attribute.String("txn_name", "unnamed")}
	if got := sumOf(t, tel, metricAnomalies, anon...); got != 1 {
		t.Fatalf("isowatch_anomalies{unnamed} = %d, want 1", got)
	}
	if got := sumOf(t, tel, metricTransactions); got != 8 {
		t.Fatalf("isowatch_transactions = %d, want 8", got)
	}
	if got := sumOf(t, tel, metricDecodeErrors); got != 1 {
		t.Fatalf("isowatch_decode_errors = %d, want 1", got)
	}
	if got := gaugeOf(t, tel, metricWindow); got != 8 {
		t.Fatalf("isowatch_window_transactions = %d, want 8", got)
	}
	if got := gaugeOf(t, tel, metricIssues); got != 2 {
		t.Fatalf("isowatch_issues = %d, want 2", got)
	}

	// After the window, the gauge goes back to 0.
	p.tick(time.Now().Add(time.Hour))
	if got := gaugeOf(t, tel, metricWindow); got != 0 {
		t.Fatalf("isowatch_window_transactions after eviction = %d, want 0", got)
	}
}

func TestLostWritesAreCountedOnce(t *testing.T) {
	tel, set := telemetrySettings(t)
	cfg := createDefaultConfig()
	cfg.Report = "every"
	p, sink, _ := startProcessor(t, set, cfg)

	// Each race loses exactly one write (A's deposit).
	for n := 0; n < 3; n++ {
		sendRace(t, p, n, depositRace)
	}
	p.tick(time.Now())
	table := attribute.String("table", "public.acct")
	if got := sumOf(t, tel, metricLostWrites, table); got != 3 {
		t.Fatalf("isowatch_lost_writes = %d, want 3", got)
	}
	// Measuring again without new races adds nothing.
	p.tick(time.Now())
	if got := sumOf(t, tel, metricLostWrites, table); got != 3 {
		t.Fatalf("isowatch_lost_writes = %d after a quiet tick, want 3", got)
	}
	// Two more races, then the old transactions leave the window: only
	// the new losses are added.
	for n := 3; n < 5; n++ {
		sendRace(t, p, n, depositRace)
	}
	p.tick(time.Now().Add(time.Hour))
	if got := sumOf(t, tel, metricLostWrites, table); got != 5 {
		t.Fatalf("isowatch_lost_writes = %d, want 5", got)
	}
	sendRace(t, p, 5, depositRace)
	p.tick(time.Now().Add(2 * time.Hour))
	if got := sumOf(t, tel, metricLostWrites, table); got != 6 {
		t.Fatalf("isowatch_lost_writes = %d after eviction, want 6", got)
	}

	// The issue knows its lost writes, and the next span carries them.
	sendRace(t, p, 6, depositRace)
	spans := spansNamed(sink, history.AnomalySpan)
	if lw := intAttr(t, spans[len(spans)-1], "isowatch.issue.lost_writes"); lw != 6 {
		t.Fatalf("issue lost_writes = %d, want 6", lw)
	}
}

func TestLostWritesOnIgnoredTablesAreNotCounted(t *testing.T) {
	tel, set := telemetrySettings(t)
	cfg := createDefaultConfig()
	cfg.IgnoreTables = []string{"sessions"}
	p, _, _ := startProcessor(t, set, cfg)

	ignored := depositRace
	ignored.table = "public.sessions"
	sendRace(t, p, 0, ignored)
	p.tick(time.Now())
	sendRace(t, p, 1, depositRace)
	p.tick(time.Now())
	if got := sumOf(t, tel, metricLostWrites); got != 1 {
		t.Fatalf("isowatch_lost_writes = %d, want 1 (the sessions loss is ignored)", got)
	}
}

func TestAnomalyLabelsAreBounded(t *testing.T) {
	tel, set := telemetrySettings(t)
	p, _, _ := startProcessor(t, set, createDefaultConfig())

	// An app that puts an id into the transaction name.
	for n := 0; n < maxSeries+20; n++ {
		r := depositRace
		r.nameA = fmt.Sprintf("deposit-%d", n)
		r.nameB = r.nameA
		sendRace(t, p, n, r)
	}
	m, err := tel.GetMetric(metricAnomalies)
	if err != nil {
		t.Fatal(err)
	}
	points := m.Data.(metricdata.Sum[int64]).DataPoints
	if len(points) != maxSeries+1 {
		t.Fatalf("want %d series (%d + other), got %d", maxSeries+1, maxSeries, len(points))
	}
	other := []attribute.KeyValue{
		attribute.String("kind", "lost-update"), attribute.String("table", otherLabel),
		attribute.String("txn_name", otherLabel)}
	if got := sumOf(t, tel, metricAnomalies, other...); got != 20 {
		t.Fatalf("other = %d, want 20", got)
	}
	if got := sumOf(t, tel, metricAnomalies); got != maxSeries+20 {
		t.Fatalf("sum of all series = %d, want %d", got, maxSeries+20)
	}
}
