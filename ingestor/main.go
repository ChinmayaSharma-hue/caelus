package ingestor

import (
	"context"
	"flag"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"log"
)

func main() {
	// create a new context
	ctx := context.Background()

	// todo: decide how to do logging
	// getting the newConfig
	configPath := flag.String("newConfig", "newConfig.json", "Path to configuration file")
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
