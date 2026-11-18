# Makefile for cost-stream microservice.
# Provides automated targets for building, linting, testing, and generating artifacts.

export PATH := $(PATH):$(shell go env GOPATH)/bin

.PHONY: all build test test-race lint generate docker-build clean

# Default target builds the server binary
all: generate build

# Compiles the application server binary into the bin directory
build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/server cmd/server/main.go

# Executes unit tests across all packages
test:
	go test -v ./...

# Executes unit and race condition tests across all packages
test-race:
	go test -v -race ./...

# Runs static code analysis using golangci-lint
lint:
	golangci-lint run ./...

# Generates Go code from Protobuf definitions and SQL schema
generate:
	buf generate
	sqlc generate

# Builds minimal multi-stage Docker container image
docker-build:
	docker build -t cost-stream:latest .

# Cleans up build artifacts and temporary files
clean:
	rm -rf bin
	rm -rf api/proto/coststream/v1/*.pb.go
	rm -rf api/proto/coststream/v1/*.pb.gw.go
	rm -rf internal/adapters/out/postgres/db/*.go
