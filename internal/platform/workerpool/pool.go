// Package workerpool provides a concurrent, buffered batch processing worker pool for high-throughput ingestion.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/platform/telemetry"
)

// ErrPoolClosed indicates an attempt to submit data to an already stopped worker pool.
var ErrPoolClosed = errors.New("worker pool is closed")

// BatchHandlerFunc is a callback function invoked by worker goroutines when a batch is ready to be persisted.
type BatchHandlerFunc func(ctx context.Context, batch []domain.TelemetryRecord) error

// Config holds runtime configuration options for the worker pool.
type Config struct {
	// WorkerCount specifies the number of parallel consumer goroutines.
	WorkerCount int
	// QueueCapacity defines the buffer size of the ingestion channel.
	QueueCapacity int
	// MaxBatchSize is the maximum number of records accumulated before triggering a flush.
	MaxBatchSize int
	// FlushInterval defines the maximum time window a batch may remain unwritten under low traffic.
	FlushInterval time.Duration
	// FlushTimeout is the per-flush operation execution timeout.
	FlushTimeout time.Duration
}

// DefaultConfig returns recommended production defaults for the worker pool.
func DefaultConfig() Config {
	return Config{
		WorkerCount:   8,
		QueueCapacity: 50000,
		MaxBatchSize:  500,
		FlushInterval: 200 * time.Millisecond,
		FlushTimeout:  10 * time.Second,
	}
}

// Pool orchestrates multiple worker goroutines accumulating and flushing records in micro-batches.
type Pool struct {
	cfg     Config
	handler BatchHandlerFunc
	logger  *slog.Logger

	ch      chan domain.TelemetryRecord
	wg      sync.WaitGroup
	closed  atomic.Bool
	stopCtx context.Context
	cancel  context.CancelFunc
}

// New creates a new worker pool instance with the given configuration, batch persistence handler, and logger.
func New(cfg Config, handler BatchHandlerFunc, logger *slog.Logger) *Pool {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 10000
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		cfg:     cfg,
		handler: handler,
		logger:  logger,
		ch:      make(chan domain.TelemetryRecord, cfg.QueueCapacity),
		stopCtx: ctx,
		cancel:  cancel,
	}
}

// Start spawns the configured number of worker goroutines.
func (p *Pool) Start() {
	p.logger.Info("Starting ingestion worker pool",
		"workers", p.cfg.WorkerCount,
		"capacity", p.cfg.QueueCapacity,
		"batch_size", p.cfg.MaxBatchSize,
		"flush_interval", p.cfg.FlushInterval,
	)

	for i := 0; i < p.cfg.WorkerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// Submit attempts to enqueue a single telemetry record into the worker pool.
// If the buffer is full, it immediately returns domain.ErrBufferSaturated to indicate backpressure.
func (p *Pool) Submit(ctx context.Context, record domain.TelemetryRecord) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.ch <- record:
		telemetry.SetQueueDepth(len(p.ch))
		return nil
	default:
		return domain.ErrBufferSaturated
	}
}

// SubmitBatch enqueues multiple records into the worker pool.
// Returns the number of successfully accepted records before buffer saturation or context cancellation.
func (p *Pool) SubmitBatch(ctx context.Context, records []domain.TelemetryRecord) (int, error) {
	if p.closed.Load() {
		return 0, ErrPoolClosed
	}

	accepted := 0
	for _, rec := range records {
		select {
		case <-ctx.Done():
			return accepted, ctx.Err()
		case p.ch <- rec:
			accepted++
			telemetry.SetQueueDepth(len(p.ch))
		default:
			return accepted, domain.ErrBufferSaturated
		}
	}

	return accepted, nil
}

// QueueDepth returns the current number of elements waiting in the buffer channel.
func (p *Pool) QueueDepth() int {
	return len(p.ch)
}

// Stop gracefully shuts down the worker pool, draining all pending records and waiting for workers to flush.
func (p *Pool) Stop(ctx context.Context) error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // already closed
	}

	p.logger.Info("Stopping ingestion worker pool, draining pending records...")
	close(p.ch)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cancel()
		p.logger.Info("Ingestion worker pool shut down successfully")
		return nil
	case <-ctx.Done():
		p.cancel()
		return fmt.Errorf("timeout draining worker pool: %w", ctx.Err())
	}
}

// workerLoop is the execution loop for each worker goroutine.
func (p *Pool) workerLoop(workerID int) {
	defer p.wg.Done()

	batch := make([]domain.TelemetryRecord, 0, p.cfg.MaxBatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()
		flushCtx, flushCancel := context.WithTimeout(context.Background(), p.cfg.FlushTimeout)
		defer flushCancel()

		err := p.handler(flushCtx, batch)
		duration := time.Since(start)

		if err != nil {
			p.logger.Error("Failed to persist telemetry batch",
				"worker_id", workerID,
				"batch_size", len(batch),
				"duration", duration,
				"error", err,
			)
		} else {
			telemetry.RecordBatchFlush(duration, len(batch))
			p.logger.Debug("Successfully flushed telemetry batch",
				"worker_id", workerID,
				"batch_size", len(batch),
				"duration", duration,
			)
		}

		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= p.cfg.MaxBatchSize {
				flush()
				ticker.Reset(p.cfg.FlushInterval)
			}

		case <-ticker.C:
			flush()

		case <-p.stopCtx.Done():
			flush()
			return
		}
	}
}
