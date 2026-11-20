# Multi-stage Dockerfile for cost-stream microservice.
# Produces a minimal, secure, non-root distroless container image.

# --- Build Stage ---
FROM golang:alpine AS builder

WORKDIR /app

ENV GOTOOLCHAIN=auto

# Install security certificates and git
RUN apk add --no-cache ca-certificates git

# Cache Go dependency layers
COPY go.mod go.sum ./
RUN go mod download

# Copy source repository code
COPY . .

# Compile static stripped binary with zero external CGO dependencies
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/coststream \
    cmd/server/main.go

# --- Final Runtime Stage ---
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Copy binary from builder stage
COPY --from=builder /bin/coststream /coststream

# Copy OpenAPI documentation for serving Swagger JSON
COPY --from=builder /app/api/openapi /api/openapi

# Expose gRPC service and HTTP REST gateway ports
EXPOSE 50051 8080

# Run with non-root security context
USER nonroot:nonroot

ENTRYPOINT ["/coststream"]
