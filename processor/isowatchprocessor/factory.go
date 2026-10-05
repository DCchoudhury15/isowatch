// Package isowatchprocessor is an OpenTelemetry Collector processor. It reads
// the "isowatch.txn" spans sent by the IsoWatch capture library, checks them
// for isolation anomalies, and adds an "isowatch.anomaly" span to the trace
// of the request that completed each anomaly.
package isowatchprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("isowatch")

// NewFactory returns the processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		func() component.Config { return createDefaultConfig() },
		processor.WithTraces(createTraces, component.StabilityLevelAlpha),
	)
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newProcessor(set.Logger, cfg.(*Config), next), nil
}
