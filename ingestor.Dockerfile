# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM golang:1.24-alpine AS builder

# Install git if needed for private module fetching
RUN apk add --no-cache git

WORKDIR /app

# Copy go.mod and go.sum first for caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source tree
COPY . .

# Build only the ingestor subpackage
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o caelus_ingestor ./src/ingestor

# ---------- Final stage ----------
FROM alpine:3.20

# Add non-root user
RUN adduser -D caelus

WORKDIR /app

COPY --from=builder /app/caelus_ingestor .
COPY config.yaml .

RUN chown -R caelus:caelus /app

USER caelus

ENTRYPOINT ["./caelus_ingestor"]
