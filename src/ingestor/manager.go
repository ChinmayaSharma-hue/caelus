package main

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/src/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/data"
	"github.com/ChinmayaSharma-hue/caelus/src/core/engine"
	"github.com/ChinmayaSharma-hue/caelus/src/core/sink"
	"github.com/ChinmayaSharma-hue/caelus/src/core/source"
	"log/slog"
	"sync"
)

type IngestionManager interface {
	Run(ctx context.Context) error
}

type ingestionManager struct {
	sources       []source.Source
	engine        engine.Engine
	buffer        buffer.Buffer
	sinks         []sink.Sink
	routines      int
	embeddingSize int
}

func NewIngestionManager(ctx context.Context, config *config.Config) (IngestionManager, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating a new ingestion manager", slog.String("component", "ingestionManager"))
	logger.Info("creating a new engine", slog.String("component", "ingestionManager"))
	newEngine, err := engine.NewEngine(ctx, config.Engine)
	if err != nil {
		return nil, err
	}

	var sources []source.Source
	var sinks []sink.Sink
	for _, sourceConfig := range config.Sources {
		logger.Info("creating a new source", slog.String("component", "ingestionManager"), slog.String("ingestionSourceType", sourceConfig.Type))
		newSource, err := source.NewSource(ctx, sourceConfig)
		if err != nil {
			return nil, err
		}
		sources = append(sources, newSource)
	}
	logger.Info("creating a new buffer", slog.String("component", "ingestionMana"), slog.String("ingestionBufferType", config.Buffer.Type))
	newBuffer, err := buffer.NewBuffer(ctx, config.Buffer)
	if err != nil {
		return nil, err
	}
	for _, sinkConfig := range config.Sinks {
		logger.Info("creating a new sink", slog.String("component", "IngestionManager"), slog.String("ingestionSinkType", sinkConfig.Type))
		newSink, err := sink.NewSink(ctx, sinkConfig, newEngine)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, newSink)
	}

	return ingestionManager{
		sources:       sources,
		engine:        newEngine,
		buffer:        newBuffer,
		sinks:         sinks,
		routines:      config.Application.IngestionRoutines,
		embeddingSize: config.Application.EmbeddingSize,
	}, nil
}

func (ingestionManager ingestionManager) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for _, ingestionSource := range ingestionManager.sources {
		// getting the metadata
		metadataList, err := ingestionSource.GetMetadata(ctx)
		if err != nil {
			return err
		}

		// batching the metadata into different goroutines to fetch the data
		chunkSize := (len(metadataList) + ingestionManager.routines - 1) / ingestionManager.routines // ceil division
		for _, ingestionSink := range ingestionManager.sinks {
			for i := 0; i < ingestionManager.routines; i++ {
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
				go func(batch []data.Metadata) {
					defer wg.Done()

					ingest(ctx, ingestionSource, ingestionManager.buffer, ingestionSink, ingestionManager.embeddingSize, batch)
				}(batch)
			}
		}
	}
	wg.Wait()
	return nil
}

func ingest(ctx context.Context, source source.Source, buffer buffer.Buffer, sink sink.Sink, embeddingSize int, metadataList []data.Metadata) {
	// get an embedding for each of the messages
	ingestedData, err := source.GetData(ctx, metadataList)
	if err != nil {
		return
	}

	// push the embedding to the vector DB
	metadataList, err = sink.Upsert(ctx, ingestedData, embeddingSize)
	if err != nil {
		return
	}

	// push metadata in a bulk insert to the buffer
	err = buffer.EnqueueBatch(ctx, metadataList)
	if err != nil {
		return
	}

	return
}
