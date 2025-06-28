# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire repo (or at least src/ and configs)
COPY . .

# Build the feeder binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o caelus_feeder ./src/feeder

# ---------- Final stage ----------
FROM alpine:3.20

RUN adduser -D caelus

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/caelus_feeder .
COPY config.yaml .

RUN chown -R caelus:caelus /app

USER caelus

ENTRYPOINT ["./caelus_feeder"]
CMD ["-newConfig=config.yaml"]
