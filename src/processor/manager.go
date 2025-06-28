package main

import (
	"context"
	"errors"
	"github.com/ChinmayaSharma-hue/caelus/src/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/sink"
	"github.com/ChinmayaSharma-hue/caelus/src/core/storage"
	"log/slog"
)

type ProcessorManager interface {
	Run(ctx context.Context)
}

type processorManager struct {
	sinks              []sink.Sink
	storages           []storage.Storage
	preprocessedBuffer buffer.Buffer
	processedBuffer    buffer.Buffer
	maxPromptTokens    int
}

func NewProcessorManager(ctx context.Context, appConfig *config.Config) (ProcessorManager, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating a new processor manager", slog.String("component", "processorManager"))
	var sinks []sink.Sink
	var storages []storage.Storage
	for _, sinkConfig := range appConfig.Sinks {
		logger.Info("creating a new sink", slog.String("component", "processorManager"),
			slog.String("ingestionSinkType", sinkConfig.Type))
		newSink, err := sink.NewSink(ctx, sinkConfig, nil)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, newSink)
	}
	for _, storageConfig := range appConfig.Storage {
		if storageConfig.Kind == "prompts" {
			logger.Info("creating a new storage",
				slog.String("component", "processorManager"),
				slog.String("storageType", storageConfig.Type))
			newStorage, err := storage.NewStorage(ctx, storageConfig)
			if err != nil {
				return nil, err
			}
			storages = append(storages, newStorage)
		}
	}
	logger.Info("creating a new buffer to fetch sink metadata",
		slog.String("component", "processorManager"),
	)
	preprocessedBuffer, err := buffer.NewBuffer(ctx, appConfig.Buffer)
	if err != nil {
		return nil, err
	}
	natsConfig, ok := appConfig.Buffer.Value.(config.NatsConfig)
	if !ok {
		logger.Error("could not parse nats config",
			slog.String("component", "processorManager"))
		return nil, errors.New("nats config is not configured")
	}
	logger.Info("creating a new buffer to store prompt IDs",
		slog.String("component", "processorManager"))
	processedBuffer, err := buffer.NewBuffer(ctx, config.RawBuffer{
		Type: "nats",
		Value: config.NatsConfig{
			Host: natsConfig.Host,
			Port: natsConfig.Port,
			Name: "prompts",
		},
	})
	if err != nil {
		return nil, err
	}

	return processorManager{
		sinks:              sinks,
		storages:           storages,
		preprocessedBuffer: preprocessedBuffer,
		processedBuffer:    processedBuffer,
		maxPromptTokens:    appConfig.Application.MaxPromptTokens,
	}, nil
}

func (p processorManager) Run(ctx context.Context) {
	numWorkers := maxWorkers
	workers := make([]*worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		for _, processSink := range p.sinks {
			for _, promptStorage := range p.storages {
				wctx, wcancel := context.WithCancel(ctx)
				workers[i] = &worker{
					preprocessedBuffer: p.preprocessedBuffer,
					processedBuffer:    p.processedBuffer,
					sink:               processSink,
					ctx:                wctx,
					storage:            promptStorage,
					cancel:             wcancel,
					maxPromptTokens:    p.maxPromptTokens,
				}
				go workers[i].Start()
			}
		}
	}

	// Graceful shutdown
	// listen for signals
	<-ctx.Done()
	for _, w := range workers {
		w.cancel()
	}
}
