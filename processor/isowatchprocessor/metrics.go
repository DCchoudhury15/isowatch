package isowatchprocessor

import (
	"context"
	"errors"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metric names. The collector's own Prometheus endpoint (port 8888) serves
// them exactly like this, without a "_total" suffix (checked with collector
// v0.162). Other exporters may add one.
const (
	metricAnomalies    = "isowatch_anomalies"
	metricLostWrites   = "isowatch_lost_writes"
	metricTransactions = "isowatch_transactions"
	metricDecodeErrors = "isowatch_decode_errors"
	metricWindow       = "isowatch_window_transactions"
	metricIssues       = "isowatch_issues"
	metricDropped      = "isowatch_notifications_dropped"
)

// maxSeries limits how many label combinations isowatch_anomalies can
// have. Table and transaction names come from app code, so the real number
// is small. If a bug in an app puts ids into transaction names, the extra
// combinations are counted under "other" instead of growing without limit.
const maxSeries = 500

const otherLabel = "other"

type telemetry struct {
	anomalies    metric.Int64Counter
	lostWrites   metric.Int64Counter
	transactions metric.Int64Counter
	decodeErrors metric.Int64Counter
	dropped      metric.Int64Counter
	reg          metric.Registration

	mu     sync.Mutex
	series map[string]bool
}

func newTelemetry(mp metric.MeterProvider, p *isowatch) (*telemetry, error) {
	m := mp.Meter("github.com/DCchoudhury15/isowatch/processor/isowatchprocessor")
	t := &telemetry{series: map[string]bool{}}
	var errs, err error
	t.anomalies, err = m.Int64Counter(metricAnomalies,
		metric.WithDescription("Isolation anomalies found, by kind, tables and transaction names."),
		metric.WithUnit("{anomaly}"))
	errs = errors.Join(errs, err)
	t.lostWrites, err = m.Int64Counter(metricLostWrites,
		metric.WithDescription("Writes whose effect was overwritten by a transaction that never saw them."),
		metric.WithUnit("{write}"))
	errs = errors.Join(errs, err)
	t.transactions, err = m.Int64Counter(metricTransactions,
		metric.WithDescription("Transactions read from isowatch.txn spans and checked."),
		metric.WithUnit("{transaction}"))
	errs = errors.Join(errs, err)
	t.decodeErrors, err = m.Int64Counter(metricDecodeErrors,
		metric.WithDescription("isowatch.txn spans that could not be read."),
		metric.WithUnit("{span}"))
	errs = errors.Join(errs, err)
	t.dropped, err = m.Int64Counter(metricDropped,
		metric.WithDescription("Webhook messages or protection writes dropped because the queue was full."),
		metric.WithUnit("{notification}"))
	errs = errors.Join(errs, err)
	window, err := m.Int64ObservableGauge(metricWindow,
		metric.WithDescription("Transactions kept in the checking window (requests, with group_by: trace)."),
		metric.WithUnit("{transaction}"))
	errs = errors.Join(errs, err)
	known, err := m.Int64ObservableGauge(metricIssues,
		metric.WithDescription("Issues kept in memory."),
		metric.WithUnit("{issue}"))
	errs = errors.Join(errs, err)
	if errs != nil {
		return nil, errs
	}
	t.reg, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(window, int64(p.checker.Stats().Live))
		o.ObserveInt64(known, int64(p.issues.len()))
		return nil
	}, window, known)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// anomaly counts one anomaly. The table label lists the tables in the
// cycle and txn_name the transaction names, each sorted and joined with
// ",", so one anomaly adds exactly 1 and sum() is the number of anomalies.
func (t *telemetry) anomaly(ctx context.Context, g sig) {
	table, names := labelsFor(g)
	key := string(g.kind) + "\x00" + table + "\x00" + names
	t.mu.Lock()
	if !t.series[key] {
		if len(t.series) >= maxSeries {
			table, names = otherLabel, otherLabel
		} else {
			t.series[key] = true
		}
	}
	t.mu.Unlock()
	t.anomalies.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", string(g.kind)),
		attribute.String("table", table),
		attribute.String("txn_name", names)))
}

func labelsFor(g sig) (table, names string) {
	table = strings.Join(g.tables, ",")
	names = strings.Join(g.names, ",")
	if names == "" {
		names = "unnamed"
	}
	return table, names
}

func (t *telemetry) lost(ctx context.Context, table string, n int64) {
	t.lostWrites.Add(ctx, n, metric.WithAttributes(attribute.String("table", table)))
}

func (t *telemetry) droppedNotification(ctx context.Context, target string) {
	t.dropped.Add(ctx, 1, metric.WithAttributes(attribute.String("target", target)))
}

func (t *telemetry) shutdown() {
	if t.reg != nil {
		_ = t.reg.Unregister()
	}
}
