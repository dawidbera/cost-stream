package workerpool_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

// TestWorkerPool_FlushOnBatchSize tests that the worker pool flushes immediately when MaxBatchSize is reached.
func TestWorkerPool_FlushOnBatchSize(t *testing.T) {
	var flushedCount atomic.Int64
	done := make(chan struct{})

	handler := func(ctx context.Context, batch []domain.TelemetryRecord) error {
		flushedCount.Add(int64(len(batch)))
		if flushedCount.Load() >= 10 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	}

	cfg := workerpool.Config{
		WorkerCount:   1,
		QueueCapacity: 50,
		MaxBatchSize:  5,
		FlushInterval: 10 * time.Second, // high interval so flush is triggered by batch size
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err := pool.Submit(ctx, domain.TelemetryRecord{
			EventID:  "evt-size-test",
			TenantID: "tenant-1",
		})
		if err != nil {
			t.Fatalf("unexpected submit error: %v", err)
		}
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for batch flush on size, got count %d", flushedCount.Load())
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop pool: %v", err)
	}
}

// TestWorkerPool_FlushOnInterval tests that items are flushed when the ticker triggers under low traffic.
func TestWorkerPool_FlushOnInterval(t *testing.T) {
	var flushedCount atomic.Int64
	done := make(chan struct{})

	handler := func(ctx context.Context, batch []domain.TelemetryRecord) error {
		flushedCount.Add(int64(len(batch)))
		if flushedCount.Load() >= 2 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	}

	cfg := workerpool.Config{
		WorkerCount:   1,
		QueueCapacity: 50,
		MaxBatchSize:  100, // high batch size so flush is triggered by interval
		FlushInterval: 50 * time.Millisecond,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()

	ctx := context.Background()
	_ = pool.Submit(ctx, domain.TelemetryRecord{EventID: "evt-1"})
	_ = pool.Submit(ctx, domain.TelemetryRecord{EventID: "evt-2"})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for ticker flush, got count %d", flushedCount.Load())
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop pool: %v", err)
	}
}

// TestWorkerPool_BufferSaturated tests that backpressure ErrBufferSaturated is returned when the channel is full.
func TestWorkerPool_BufferSaturated(t *testing.T) {
	blockHandler := make(chan struct{})
	handler := func(ctx context.Context, batch []domain.TelemetryRecord) error {
		<-blockHandler
		return nil
	}

	cfg := workerpool.Config{
		WorkerCount:   1,
		QueueCapacity: 2,
		MaxBatchSize:  10,
		FlushInterval: 10 * time.Second,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Note: do not start pool yet so buffer remains full

	ctx := context.Background()
	_ = pool.Submit(ctx, domain.TelemetryRecord{EventID: "e1"})
	_ = pool.Submit(ctx, domain.TelemetryRecord{EventID: "e2"})

	err := pool.Submit(ctx, domain.TelemetryRecord{EventID: "e3"})
	if err != domain.ErrBufferSaturated {
		t.Fatalf("expected ErrBufferSaturated, got %v", err)
	}

	close(blockHandler)
}

// TestWorkerPool_ConcurrentSubmission tests concurrent submissions with race condition detection.
func TestWorkerPool_ConcurrentSubmission(t *testing.T) {
	var totalFlushed atomic.Int64

	handler := func(ctx context.Context, batch []domain.TelemetryRecord) error {
		totalFlushed.Add(int64(len(batch)))
		return nil
	}

	cfg := workerpool.Config{
		WorkerCount:   4,
		QueueCapacity: 2000,
		MaxBatchSize:  20,
		FlushInterval: 50 * time.Millisecond,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()

	const numGoroutines = 10
	const itemsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				for {
					err := pool.Submit(context.Background(), domain.TelemetryRecord{
						EventID: "evt-concurrent",
					})
					if err == nil {
						break
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(g)
	}

	wg.Wait()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop pool: %v", err)
	}

	expectedTotal := int64(numGoroutines * itemsPerGoroutine)
	if totalFlushed.Load() != expectedTotal {
		t.Fatalf("expected %d flushed items, got %d", expectedTotal, totalFlushed.Load())
	}
}
