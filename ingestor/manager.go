package main

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/engine"
	"github.com/ChinmayaSharma-hue/caelus/core/source"
	"github.com/ChinmayaSharma-hue/caelus/core/vectorstore"
	"log/slog"
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
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating a new engine", slog.String("component", "IngestionManager"))
	newEngine, err := engine.NewEngine(ctx, config.Engine)
	if err != nil {
		return nil, err
	}

	var sources []source.Source
	var sinks []vectorstore.VectorStore
	for _, sourceConfig := range config.Sources {
		logger.Info("creating a new source", slog.String("component", "IngestionManager"), slog.String("ingestionSourceType", sourceConfig.Type))
		newSource, err := source.NewSource(ctx, sourceConfig)
		if err != nil {
			return nil, err
		}
		sources = append(sources, newSource)
	}
	for _, sinkConfig := range config.Sinks {
		logger.Info("creating a new sink", slog.String("component", "IngestionManager"), slog.String("ingestionSinkType", sinkConfig.Type))
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
		metadataList, err := ingestionSource.GetMetadata(ctx)
		if err != nil {
			return err
		}

		chunkSize := (len(metadataList) + Routines - 1) / Routines // ceil division
		for _, ingestionSink := range ingestionManager.sinks {
			for i := 0; i < Routines; i++ {
				start := i * chunkSize
				end := start + chunkSize
				if end > len(metadataList) {
					end = len(metadataList)
				}
				if start >= len(metadataList) {
					break // no more data
				}
				batch := metadataList[start:end]

				wg.Add(1)
				go func(batch []core.Metadata) {
					defer wg.Done()

					ingest(ctx, ingestionSource, ingestionSink, batch)
				}(batch)
			}
		}
	}
	wg.Wait()
	return nil
}

func ingest(ctx context.Context, source source.Source, sink vectorstore.VectorStore, metadataList []core.Metadata) {
	// todo: push metadata in a bulk insert to the metadata DB
	// get an embedding for each of the messages
	data, err := source.GetData(ctx, metadataList)
	if err != nil {
		return
	}

	// push the embedding to the vector DB
	err = sink.Upsert(ctx, data, source.GetCollection(ctx), EmbbeddingSize)
	if err != nil {
		return
	}

	return
}
