// Package config loads and validates application configuration from environment variables.
package config

import (
	"os"
	"runtime"
	"strconv"
	"time"
)

// Config aggregates all operational settings for the cost-stream microservice.
type Config struct {
	ServiceName     string
	LogLevel        string
	GRPCPort        int
	HTTPPort        int
	ShutdownTimeout time.Duration

	// Database settings
	PostgresURL         string
	PostgresMaxConns    int32
	PostgresMinConns    int32
	PostgresMaxConnLife time.Duration
	PostgresMaxIdleLife time.Duration

	// Redis settings
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	IdempotencyTTL time.Duration

	// Worker pool settings
	WorkerCount   int
	QueueCapacity int
	MaxBatchSize  int
	FlushInterval time.Duration
	FlushTimeout  time.Duration

	// Path to Swagger documentation
	SwaggerPath string
}

// Load reads configuration from the environment, applying sensible production defaults.
func Load() Config {
	numCPU := runtime.NumCPU()
	if numCPU < 2 {
		numCPU = 2
	}

	return Config{
		ServiceName:     getEnv("SERVICE_NAME", "cost-stream"),
		LogLevel:        getEnv("LOG_LEVEL", "INFO"),
		GRPCPort:        getEnvInt("GRPC_PORT", 50051),
		HTTPPort:        getEnvInt("HTTP_PORT", 8080),
		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),

		PostgresURL:         getEnv("DATABASE_URL", "postgres://coststream:coststream_password@localhost:5432/coststream?sslmode=disable"),
		PostgresMaxConns:    int32(getEnvInt("DATABASE_MAX_CONNS", 25)),
		PostgresMinConns:    int32(getEnvInt("DATABASE_MIN_CONNS", 5)),
		PostgresMaxConnLife: getEnvDuration("DATABASE_MAX_CONN_LIFETIME", time.Hour),
		PostgresMaxIdleLife: getEnvDuration("DATABASE_MAX_CONN_IDLE_TIME", 30*time.Minute),

		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  getEnv("REDIS_PASSWORD", ""),
		RedisDB:        getEnvInt("REDIS_DB", 0),
		IdempotencyTTL: getEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour),

		WorkerCount:   getEnvInt("WORKER_COUNT", numCPU*2),
		QueueCapacity: getEnvInt("QUEUE_CAPACITY", 50000),
		MaxBatchSize:  getEnvInt("MAX_BATCH_SIZE", 500),
		FlushInterval: getEnvDuration("FLUSH_INTERVAL", 200*time.Millisecond),
		FlushTimeout:  getEnvDuration("FLUSH_TIMEOUT", 5*time.Second),

		SwaggerPath: getEnv("SWAGGER_PATH", "api/openapi/api/proto/coststream/v1/telemetry.swagger.json"),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := time.ParseDuration(val); err == nil {
			return parsed
		}
	}
	return defaultVal
}
