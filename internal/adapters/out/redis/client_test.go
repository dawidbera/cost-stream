package redis_test

import (
	"context"
	"testing"
	"time"

	redisAdapter "github.com/dawidbera/cost-stream/internal/adapters/out/redis"
)

// TestRedisClient_NilSafety tests error handling when the underlying redis client is nil.
func TestRedisClient_NilSafety(t *testing.T) {
	client := redisAdapter.NewClient(nil)

	ctx := context.Background()

	// SetIfNotExist on nil client
	_, err := client.SetIfNotExist(ctx, "test-key", time.Minute)
	if err == nil {
		t.Fatalf("expected error on nil redis client, got nil")
	}

	// Ping on nil client
	err = client.Ping(ctx)
	if err == nil {
		t.Fatalf("expected error on nil redis client ping, got nil")
	}
}
