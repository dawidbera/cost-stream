// Package main serves as the primary entry point for the cost-stream microservice.
// It initializes dependencies, starts driving adapters (gRPC & HTTP), and manages graceful shutdown.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	grpcAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/grpc"
	httpAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/http"
	postgresAdapter "github.com/dawidbera/cost-stream/internal/adapters/out/postgres"
	redisAdapter "github.com/dawidbera/cost-stream/internal/adapters/out/redis"
	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/services"
	"github.com/dawidbera/cost-stream/internal/platform/config"
	"github.com/dawidbera/cost-stream/internal/platform/logger"
	"github.com/dawidbera/cost-stream/internal/platform/telemetry"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "service terminated with error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	log.Info("Starting cost-stream microservice",
		"service", cfg.ServiceName,
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"log_level", cfg.LogLevel,
	)

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	// Initialize OpenTelemetry distributed tracing
	shutdownTracer, tracerErr := telemetry.InitTracer(appCtx, cfg.ServiceName)
	if tracerErr != nil {
		log.Warn("Failed to initialize OpenTelemetry tracer", "error", tracerErr)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = shutdownTracer(ctx)
		}()
	}

	// Initialize PostgreSQL connection pool
	pgConfig, parseErr := pgxpool.ParseConfig(cfg.PostgresURL)
	if parseErr != nil {
		return fmt.Errorf("invalid postgresql url: %w", parseErr)
	}
	pgConfig.MaxConns = cfg.PostgresMaxConns
	pgConfig.MinConns = cfg.PostgresMinConns
	pgConfig.MaxConnLifetime = cfg.PostgresMaxConnLife
	pgConfig.MaxConnIdleTime = cfg.PostgresMaxIdleLife

	pgPool, poolErr := pgxpool.NewWithConfig(appCtx, pgConfig)
	if poolErr != nil {
		return fmt.Errorf("failed to create postgresql pool: %w", poolErr)
	}
	defer pgPool.Close()

	if pingErr := pgPool.Ping(appCtx); pingErr != nil {
		log.Warn("PostgreSQL ping failed on startup (will retry on readiness)", "error", pingErr)
	} else {
		log.Info("Connected to PostgreSQL successfully")
	}

	// Initialize Redis client
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer func() {
		_ = rdb.Close()
	}()

	if rdbPingErr := rdb.Ping(appCtx).Err(); rdbPingErr != nil {
		log.Warn("Redis ping failed on startup (will retry on readiness)", "error", rdbPingErr)
	} else {
		log.Info("Connected to Redis successfully")
	}

	// Initialize Driven (Output) Adapters
	pgRepo := postgresAdapter.NewRepository(pgPool)
	redisCache := redisAdapter.NewClient(rdb)

	// Initialize Ingestion Worker Pool
	workerPoolCfg := workerpool.Config{
		WorkerCount:   cfg.WorkerCount,
		QueueCapacity: cfg.QueueCapacity,
		MaxBatchSize:  cfg.MaxBatchSize,
		FlushInterval: cfg.FlushInterval,
		FlushTimeout:  cfg.FlushTimeout,
	}

	batchFlushHandler := func(ctx context.Context, batch []domain.TelemetryRecord) error {
		return pgRepo.BulkInsertRecords(ctx, batch)
	}

	pool := workerpool.New(workerPoolCfg, batchFlushHandler, log)
	pool.Start()

	// Initialize Core Application Services
	ingestionSvc := services.NewIngestionService(pool, redisCache, log, cfg.IdempotencyTTL)
	querySvc := services.NewQueryService(pgRepo)

	// Initialize Driving (Input) Adapters: gRPC Server
	grpcHandler := grpcAdapter.NewHandler(ingestionSvc, querySvc)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcAdapter.UnaryRecoveryInterceptor(log),
			grpcAdapter.UnaryLoggingInterceptor(log),
		),
		grpc.ChainStreamInterceptor(
			grpcAdapter.StreamRecoveryInterceptor(log),
			grpcAdapter.StreamLoggingInterceptor(log),
		),
	)
	coststreamv1.RegisterTelemetryServiceServer(grpcServer, grpcHandler)
	reflection.Register(grpcServer)

	grpcLis, lisErr := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if lisErr != nil {
		return fmt.Errorf("failed to bind grpc port: %w", lisErr)
	}

	go func() {
		log.Info("gRPC server listening", "port", cfg.GRPCPort)
		if serveErr := grpcServer.Serve(grpcLis); serveErr != nil && serveErr != grpc.ErrServerStopped {
			log.Error("gRPC server encountered fatal error", "error", serveErr)
		}
	}()

	// Initialize Driving (Input) Adapters: HTTP REST Gateway & Probes
	healthHandler := httpAdapter.NewHealthHandler(pgRepo, redisCache)
	httpGateway, gwErr := httpAdapter.NewServer(
		appCtx,
		fmt.Sprintf(":%d", cfg.HTTPPort),
		grpcHandler,
		healthHandler,
		cfg.SwaggerPath,
		log,
	)
	if gwErr != nil {
		return fmt.Errorf("failed to initialize http gateway: %w", gwErr)
	}

	go func() {
		if startErr := httpGateway.Start(); startErr != nil {
			log.Error("HTTP gateway server error", "error", startErr)
		}
	}()

	// Listen for OS termination signals (SIGINT, SIGTERM)
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownSig
	log.Info("Shutdown signal received, initiating graceful termination...", "signal", sig.String())

	// Execute orderly shutdown with strict deadline
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// 1. Stop HTTP gateway
	if stopErr := httpGateway.Shutdown(shutdownCtx); stopErr != nil {
		log.Error("HTTP gateway shutdown error", "error", stopErr)
	}

	// 2. Stop accepting new gRPC requests
	grpcServer.GracefulStop()
	log.Info("gRPC server stopped cleanly")

	// 3. Drain and flush background worker pool
	if poolStopErr := pool.Stop(shutdownCtx); poolStopErr != nil {
		log.Error("Worker pool drain error", "error", poolStopErr)
	}

	log.Info("Shutdown complete, exiting.")
	return nil
}
