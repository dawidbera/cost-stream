// Package telemetry provides OpenTelemetry tracing initialization and Prometheus metric instrumentation.
package telemetry

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	// IngestedTotal tracks the total number of telemetry records ingested, partitioned by status and tenant.
	IngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "coststream",
			Subsystem: "telemetry",
			Name:      "ingested_total",
			Help:      "Total number of telemetry records processed, partitioned by status and tenant.",
		},
		[]string{"status", "tenant_id"},
	)

	// BatchFlushDuration tracks the duration in seconds required to flush a batch to storage.
	BatchFlushDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coststream",
			Subsystem: "workerpool",
			Name:      "batch_flush_duration_seconds",
			Help:      "Latency of batch flush operations to the persistence layer.",
			Buckets:   prometheus.DefBuckets,
		},
	)

	// BatchSize tracks the distribution of batch sizes flushed to the storage layer.
	BatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coststream",
			Subsystem: "workerpool",
			Name:      "batch_size",
			Help:      "Distribution of flushed batch sizes.",
			Buckets:   []float64{1, 10, 50, 100, 250, 500, 1000, 2000},
		},
	)

	// WorkerPoolQueueDepth gauges the current number of queued records waiting in the ingestion buffer.
	WorkerPoolQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "coststream",
			Subsystem: "workerpool",
			Name:      "queue_depth",
			Help:      "Current count of records buffered in the worker pool channel.",
		},
	)
)

// RecordIngestion updates the ingestion counter for a given status and tenant ID.
func RecordIngestion(status, tenantID string, count int) {
	if count <= 0 {
		return
	}
	IngestedTotal.WithLabelValues(status, tenantID).Add(float64(count))
}

// RecordBatchFlush records the duration and item count of a flushed batch.
func RecordBatchFlush(d time.Duration, size int) {
	BatchFlushDuration.Observe(d.Seconds())
	BatchSize.Observe(float64(size))
}

// SetQueueDepth updates the worker pool channel depth gauge.
func SetQueueDepth(depth int) {
	WorkerPoolQueueDepth.Set(float64(depth))
}

// InitTracer initializes OpenTelemetry tracing provider and returns a shutdown function.
func InitTracer(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the standard OpenTelemetry tracer for this package.
func Tracer() trace.Tracer {
	return otel.Tracer("cost-stream")
}
