// Package redis provides Redis adapter implementations for idempotency checks and caching.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dawidbera/cost-stream/internal/core/ports"
)

var _ ports.IdempotencyCache = (*Client)(nil)

// Client implements ports.IdempotencyCache using Redis.
type Client struct {
	rdb *redis.Client
}

// NewClient creates a new Redis idempotency adapter instance from a go-redis client.
func NewClient(rdb *redis.Client) *Client {
	return &Client{
		rdb: rdb,
	}
}

// SetIfNotExist sets the idempotency key with the given TTL if it does not already exist.
// Returns true if the key was set (i.e., this is the first time the batch is seen), or false if already present.
func (c *Client) SetIfNotExist(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if c.rdb == nil {
		return false, errors.New("redis client is nil")
	}

	redisKey := fmt.Sprintf("coststream:idempotency:%s", key)
	ok, err := c.rdb.SetNX(ctx, redisKey, "processed", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis SetNX failed for key %s: %w", key, err)
	}

	return ok, nil
}

// Ping verifies that the Redis server is responsive.
func (c *Client) Ping(ctx context.Context) error {
	if c.rdb == nil {
		return errors.New("redis client is nil")
	}
	return c.rdb.Ping(ctx).Err()
}
