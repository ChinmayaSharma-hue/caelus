package main

import (
	"context"
	"errors"
	"github.com/ChinmayaSharma-hue/caelus/src/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/storage"
	"github.com/sashabaranov/go-openai"
	"log/slog"
)

type FeederManager interface {
	Run(ctx context.Context)
}

type feederManager struct {
	responseStorages []storage.Storage
	promptStorage    storage.Storage
	processedBuffer  buffer.Buffer
	client           *openai.Client
	model            string
	tokensUsed       *int64
	tokenLimit       int64
}

func NewFeederManager(ctx context.Context, appConfig *config.Config, maxAllowedTokens int64) (FeederManager, error) {
	logger := ctx.Value("logger").(*slog.Logger)
	logger.Info("creating a new feeder manager", slog.String("component", "feederManager"))
	var responseStorages []storage.Storage
	var promptStorage storage.Storage
	for _, storageConfig := range appConfig.Storage {
		if storageConfig.Kind == "responses" {
			newStorage, err := storage.NewStorage(ctx, storageConfig)
			if err != nil {
				return nil, err
			}
			responseStorages = append(responseStorages, newStorage)
		}
		if storageConfig.Kind == "prompts" {
			newStorage, err := storage.NewStorage(ctx, storageConfig)
			if err != nil {
				return nil, err
			}
			promptStorage = newStorage
		}
	}
	natsConfig, ok := appConfig.Buffer.Value.(config.NatsConfig)
	if !ok {
		return nil, errors.New("nats config is not configured")
	}
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
	llmConfig, ok := appConfig.LLM.Value.(config.OpenAIConfig)
	if !ok {
		return nil, errors.New("llm config is not configured")
	}
	tokensUsed := int64(0)
	return feederManager{
		responseStorages: responseStorages,
		promptStorage:    promptStorage,
		processedBuffer:  processedBuffer,
		client:           openai.NewClient(llmConfig.APIKey),
		model:            llmConfig.Model,
		tokensUsed:       &tokensUsed,
		tokenLimit:       maxAllowedTokens,
	}, nil
}

func (f feederManager) Run(ctx context.Context) {
	numWorkers := maxWorkers
	workers := make([]*worker, 0)
	ctx, cancel := context.WithCancel(ctx)
	for i := 0; i < numWorkers; i++ {
		for _, storage := range f.responseStorages {
			wctx, wcancel := context.WithCancel(ctx)
			w := &worker{
				client:          f.client,
				model:           f.model,
				processedBuffer: f.processedBuffer,
				promptStorage:   f.promptStorage,
				responseStorage: storage,
				ctx:             wctx,
				cancel:          wcancel,
				tokensUsed:      f.tokensUsed,
				tokenLimit:      f.tokenLimit,
			}
			workers = append(workers, w)
			go w.Start()
		}
	}

	// Graceful shutdown
	<-ctx.Done()
	for _, w := range workers {
		w.cancel()
	}
	cancel()
}
