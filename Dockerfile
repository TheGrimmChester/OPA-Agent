# Build stage
FROM golang:1.21-alpine AS builder

# Install git and ca-certificates (needed for Go to download dependencies)
RUN apk --no-cache add git ca-certificates

WORKDIR /app

# Set Go environment variables for optimized module downloads
# GOPROXY uses official proxy with fallback to direct for resilience
# GOSUMDB enables checksum verification for security
ENV GOPROXY=https://proxy.golang.org,direct
ENV GOSUMDB=sum.golang.org

# Copy dependency files first for better Docker layer caching
# This allows dependencies to be cached separately from source code changes
COPY go.mod go.sum ./

# Download dependencies (cached layer if go.mod/go.sum unchanged)
RUN go mod download

# Copy all Go source files
COPY *.go ./

# Generate go.sum and verify dependencies (with retry for network issues)
RUN go mod tidy || (sleep 5 && go mod tidy) || (sleep 10 && go mod tidy)

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o opa-agent .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates wget

WORKDIR /root/

COPY --from=builder /app/opa-agent .

EXPOSE 2112 8080

CMD ["./opa-agent"]

