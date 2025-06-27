package buffer

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/data"
	"log/slog"
)

type Buffer interface {
	EnqueueBatch(ctx context.Context, metadata []data.Metadata) error
	Dequeue(ctx context.Context) (interface{}, error)
	MarkConsumed(ctx context.Context, message interface{}) error
}

func NewBuffer(ctx context.Context, bufferConfig config.Buffer) (Buffer, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	rawBuffer, ok := bufferConfig.(config.RawBuffer)
	if !ok {
		logger.Error("could not convert buffer config to raw buffer",
			slog.String("component", "buffer"))
	}
	switch rawBuffer.Type {
	case "nats":
		natsConfig, ok := rawBuffer.Value.(config.NatsConfig)
		if !ok {
			logger.Error("could not convert nats config to nats buffer",
				slog.String("component", "buffer"),
				slog.String("type", fmt.Sprintf("%T", rawBuffer.Type)))
		}
		logger.Debug("creating nats buffer", slog.String("component", "buffer"), slog.String("type", fmt.Sprintf("%T", rawBuffer.Type)))
		streamingBuffer, err := NewNATSStreamingBuffer(ctx, natsConfig)
		if err != nil {
			return nil, err
		}
		return streamingBuffer, nil
	default:
		logger.Error("could not create buffer", slog.String("type", fmt.Sprintf("%T", rawBuffer.Type)))
		return nil, fmt.Errorf("unknown buffer type: %s", rawBuffer.Type)
	}
}
