package main

import (
	"context"
	"flag"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"log"
	"log/slog"
	"os"
)

func main() {
	// create a new context
	ctx := context.Background()

	// create a new logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx = context.WithValue(ctx, "logger", logger)

	// todo: decide how to do logging
	// getting the newConfig
	configPath := flag.String("newConfig", "config.yaml", "Path to configuration file")
	newConfig, err := config.NewConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	// getting the ingestion manager
	manager, err := NewIngestionManager(ctx, newConfig.Ingestor)
	if err != nil {
		log.Fatal(err)
	}

	err = manager.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
