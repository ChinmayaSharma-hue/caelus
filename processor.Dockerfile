# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the repository
COPY . .

# Build the processor binary from its subpackage
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o caelus_processor ./src/processor

# ---------- Final stage ----------
FROM alpine:3.20

RUN adduser -D caelus

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/caelus_processor .
COPY config.yaml .
COPY prompt .

RUN chown -R caelus:caelus /app

USER caelus

ENTRYPOINT ["./caelus_processor"]
