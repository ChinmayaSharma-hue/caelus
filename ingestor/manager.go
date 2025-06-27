package main

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/data"
	"github.com/ChinmayaSharma-hue/caelus/core/engine"
	"github.com/ChinmayaSharma-hue/caelus/core/sink"
	"github.com/ChinmayaSharma-hue/caelus/core/source"
	"log/slog"
	"sync"
)

type IngestionManager interface {
	Run(ctx context.Context) error
}

type ingestionManager struct {
	sources []source.Source
	engine  engine.Engine
	buffers []buffer.Buffer
	sinks   []sink.Sink
}

func NewIngestionManager(ctx context.Context, config config.Ingestor) (IngestionManager, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating a new engine", slog.String("component", "IngestionManager"))
	newEngine, err := engine.NewEngine(ctx, config.Engine)
	if err != nil {
		return nil, err
	}

	var sources []source.Source
	var buffers []buffer.Buffer
	var sinks []sink.Sink
	for _, sourceConfig := range config.Sources {
		logger.Info("creating a new source", slog.String("component", "IngestionManager"), slog.String("ingestionSourceType", sourceConfig.Type))
		newSource, err := source.NewSource(ctx, sourceConfig)
		if err != nil {
			return nil, err
		}
		sources = append(sources, newSource)
	}
	for _, bufferConfig := range config.Buffers {
		logger.Info("creating a new buffer", slog.String("component", "IngestionManager"), slog.String("ingestionBufferType", bufferConfig.Type))
		newBuffer, err := buffer.NewBuffer(ctx, bufferConfig)
		if err != nil {
			return nil, err
		}
		buffers = append(buffers, newBuffer)
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
		sources: sources,
		engine:  newEngine,
		buffers: buffers,
		sinks:   sinks,
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

		for _, ingestionBuffer := range ingestionManager.buffers {
			// batching the metadata into different goroutines to fetch the data
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
					go func(batch []data.Metadata) {
						defer wg.Done()

						ingest(ctx, ingestionSource, ingestionBuffer, ingestionSink, batch)
					}(batch)
				}
			}
		}
	}
	wg.Wait()
	return nil
}

func ingest(ctx context.Context, source source.Source, buffer buffer.Buffer, sink sink.Sink, metadataList []data.Metadata) {
	// push metadata in a bulk insert to the buffer
	err := buffer.EnqueueBatch(ctx, metadataList)
	if err != nil {
		return
	}

	// get an embedding for each of the messages
	ingestedData, err := source.GetData(ctx, metadataList)
	if err != nil {
		return
	}

	// push the embedding to the vector DB
	err = sink.Upsert(ctx, ingestedData, source.GetCollection(ctx), EmbbeddingSize)
	if err != nil {
		return
	}

	return
}
