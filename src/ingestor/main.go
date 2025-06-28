package main

import (
	"context"
	"flag"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"log/slog"
	"os"
)

func main() {
	// create a new context
	ctx := context.Background()

	// create a new logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx = context.WithValue(ctx, "logger", logger)

	// getting the newConfig
	configPath := flag.String("newConfig", "config.yaml", "Path to configuration file")
	newConfig, err := config.NewConfig(*configPath)
	if err != nil {
		logger.Error("Error reading configuration file", "error", err)
		return
	}

	// getting the ingestion manager
	manager, err := NewIngestionManager(ctx, newConfig)
	if err != nil {
		logger.Error("Error creating ingestion manager", "error", err)
		return
	}

	err = manager.Run(ctx)
	if err != nil {
		logger.Error("Error running ingestion manager", "error", err)
		return
	}
}
