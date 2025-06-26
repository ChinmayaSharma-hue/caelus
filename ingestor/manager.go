package ingestor

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/engine"
	"github.com/ChinmayaSharma-hue/caelus/core/source"
	"github.com/ChinmayaSharma-hue/caelus/core/vectorstore"
	"sync"
)

type IngestionManager interface {
	Run(ctx context.Context) error
}

type ingestionManager struct {
	sources []source.Source
	engine  engine.Engine
	sinks   []vectorstore.VectorStore
}

func NewIngestionManager(ctx context.Context, config config.Ingestor) (IngestionManager, error) {
	newEngine, err := engine.NewEngine(ctx, config)
	if err != nil {
		return nil, err
	}

	var sources []source.Source
	var sinks []vectorstore.VectorStore
	for _, sourceConfig := range config.Sources {
		newSource, err := source.NewSource(ctx, sourceConfig)
		if err != nil {
			return nil, err
		}
		sources = append(sources, newSource)
	}
	for _, sinkConfig := range config.Sinks {
		newSink, err := vectorstore.NewVectorStore(ctx, sinkConfig, newEngine)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, newSink)
	}

	return ingestionManager{
		sources: sources,
		engine:  newEngine,
		sinks:   sinks,
	}, nil
}

func (ingestionManager ingestionManager) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for _, ingestionSource := range ingestionManager.sources {
		metadataList, err := ingestionSource.GetMetadata()
		if err != nil {
			return err
		}
		spread := len(metadataList) / Routines

		index := 0
		for _, ingestionSink := range ingestionManager.sinks {
			errors := make(chan error)
			for i := 0; i < Routines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					errors <- ingest(ctx, ingestionSource, ingestionSink, metadataList[index:index+spread:len(metadataList)])
				}()
			}
			if errors != nil {
				close(errors)
			}
		}
	}
	wg.Wait()
	return nil
}

func ingest(ctx context.Context, source source.Source, sink vectorstore.VectorStore, metadataList []core.Metadata) error {
	// todo: push metadata in a bulk insert to the metadata DB
	// get an embedding for each of the messages
	data, err := source.GetData(metadataList)
	if err != nil {
		return err
	}

	// push the embedding to the vector DB
	err = sink.Upsert(ctx, data, source.GetCollection(), EmbbeddingSize)
	if err != nil {
		return err
	}

	return nil
}
