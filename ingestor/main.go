package ingestor

import (
	"context"
	"flag"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/source"
	"log"
	"sync"
)

func main() {
	// create a new context
	ctx := context.Background()
	var wg sync.WaitGroup

	// todo: decide how to do logging
	// getting the newConfig
	configPath := flag.String("newConfig", "newConfig.json", "Path to configuration file")
	newConfig, err := config.NewConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	// creating all the sources specified in the newConfig
	for _, sourceConfig := range newConfig.Ingestor.Sources {
		newSource, err := source.NewSource(ctx, sourceConfig)
		if err != nil {
			log.Fatal(err)
		}

		metadataList, err := newSource.GetMetadata()
		if err != nil {
			log.Fatal(err)
		}

		errors := make(chan error, 0)
		for i := 0; i < Routines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				errors <- ingest(ctx, newSource, metadataList)
			}()
		}
	}

	// division of functionalities
	// communication with source: get a list of mails, get each mail
	// ingestion manager that does the following,
	// use the source to ingest metadata
	// push the generic metadata in a bulk insert to the database
	// spawn goroutines to ingest actual data using source and push the data to the vector DB using source
}
