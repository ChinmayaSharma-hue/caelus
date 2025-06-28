package sink

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/data"
	"github.com/ChinmayaSharma-hue/caelus/src/core/engine"
	"log/slog"
)

type Sink interface {
	Upsert(ctx context.Context, data []data.Data, size int) error
	Fetch(ctx context.Context, filters map[string]string) (map[string]data.Data, error)
	MarkConsumed(ctx context.Context, ids []string) error
	GetCollection(ctx context.Context) string
}

func NewSink(ctx context.Context, sinkConfig config.RawSink, generator engine.Engine) (Sink, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating vector store", slog.String("component", "Sink"))
	switch sinkConfig.Type {
	case "qdrant":
		qdrantConfig, ok := sinkConfig.Value.(config.QdrantConfig)
		if !ok {
			logger.Error("failed to cast qdrant config", slog.String("type", sinkConfig.Type), slog.String("component", "Sink"))
			return nil, fmt.Errorf("database config is not a qdrant config")
		}
		qdrantConnector, err := NewQdrantConnector(ctx, qdrantConfig, generator)
		if err != nil {
			return nil, err
		}
		return qdrantConnector, nil
	default:
		return nil, fmt.Errorf("database config is not a qdrant config")
	}
}
