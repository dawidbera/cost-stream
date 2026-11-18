// Package domain contains pure business domain entities, value objects, and domain errors.
// It has zero external dependencies, following Clean/Hexagonal Architecture principles.
package domain

import "errors"

// Standard domain error definitions representing business rule violations and system limits.
var (
	// ErrInvalidRecord indicates that a telemetry record has missing or invalid required fields.
	ErrInvalidRecord = errors.New("telemetry record validation failed")

	// ErrDuplicateBatch indicates that a batch with the specified idempotency key has already been processed.
	ErrDuplicateBatch = errors.New("batch idempotency key already processed")

	// ErrBufferSaturated indicates that the ingestion worker channel is full and cannot accept more events.
	ErrBufferSaturated = errors.New("ingestion buffer saturated, apply backpressure")

	// ErrTenantNotFound indicates that the requested tenant was not found or is inactive.
	ErrTenantNotFound = errors.New("tenant not found or inactive")
)
