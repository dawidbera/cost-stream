// Package logger provides structured JSON logging utilities based on the Go standard library log/slog.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const traceIDKey contextKey = "trace_id"

// New creates and configures a new structured slog.Logger writing JSON to stdout.
func New(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(strings.TrimSpace(levelStr)) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	case "INFO":
		fallthrough
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
	})

	return slog.New(handler)
}

// WithTraceID returns a new context containing the specified trace identifier.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext extracts the trace identifier from the context if present.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}
