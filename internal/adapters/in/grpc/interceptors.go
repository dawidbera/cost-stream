// Package grpc provides the gRPC transport adapter implementation for TelemetryService.
package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryLoggingInterceptor logs invocation metadata, duration, and error codes for unary RPC calls.
func UnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		level := slog.LevelInfo
		if st.Code() != codes.OK {
			level = slog.LevelWarn
		}

		logger.Log(ctx, level, "gRPC unary request finished",
			"method", info.FullMethod,
			"code", st.Code().String(),
			"duration", duration,
			"error", st.Message(),
		)

		return resp, err
	}
}

// StreamLoggingInterceptor logs lifecycle and termination status for streaming RPC calls.
func StreamLoggingInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		level := slog.LevelInfo
		if st.Code() != codes.OK {
			level = slog.LevelWarn
		}

		logger.Log(ss.Context(), level, "gRPC stream request finished",
			"method", info.FullMethod,
			"code", st.Code().String(),
			"duration", duration,
			"error", st.Message(),
		)

		return err
	}
}

// UnaryRecoveryInterceptor catches unexpected panics in unary RPCs and returns codes.Internal.
func UnaryRecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error("Panic recovered in gRPC unary handler",
					"method", info.FullMethod,
					"panic", fmt.Sprint(r),
					"stack", stack,
				)
				err = status.Errorf(codes.Internal, "panic recovered: %v", r)
			}
		}()
		return handler(ctx, req)
	}
}

// StreamRecoveryInterceptor catches unexpected panics in streaming RPCs and returns codes.Internal.
func StreamRecoveryInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error("Panic recovered in gRPC stream handler",
					"method", info.FullMethod,
					"panic", fmt.Sprint(r),
					"stack", stack,
				)
				err = status.Errorf(codes.Internal, "panic recovered: %v", r)
			}
		}()
		return handler(srv, ss)
	}
}
