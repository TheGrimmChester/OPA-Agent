# Build stage
FROM golang:1.21-alpine AS builder

# Install git and ca-certificates (needed for Go to download dependencies)
RUN apk --no-cache add git ca-certificates

WORKDIR /app

# Set Go environment variables to handle network issues
# GOSUMDB=off disables checksum verification (use with caution)
# GOPROXY=direct bypasses proxy and fetches directly
ENV GOPROXY=direct
ENV GOSUMDB=off

# Copy go.mod and all Go source files
COPY go.mod ./
COPY *.go ./

# Download dependencies, this will automatically resolve and download all required packages
RUN go mod download

# Generate go.sum and verify dependencies
RUN go mod tidy

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o opa-agent .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates wget

WORKDIR /root/

COPY --from=builder /app/opa-agent .

EXPOSE 2112 8080

CMD ["./opa-agent"]

