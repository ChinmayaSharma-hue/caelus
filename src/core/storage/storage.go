package storage

import (
	"context"
	"errors"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"log/slog"
)

type Storage interface {
	Upload(ctx context.Context, key string, data string) error
	Download(ctx context.Context, key string) (string, error)
}

func NewStorage(ctx context.Context, storage config.Storage) (Storage, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	rawStorage, ok := storage.(config.RawStorage)
	if !ok {
		logger.Error("could not cast storage to raw storage",
			slog.String("component", "storage"))
		return nil, errors.New("could not cast storage to raw storage")
	}
	switch rawStorage.Type {
	case "minio":
		minioConfig, ok := rawStorage.Value.(config.MinioConfig)
		if !ok {
			logger.Error("could not cast minio config to minio storage",
				slog.String("component", "storage"),
				slog.String("type", rawStorage.Type))
		}
		logger.Info("creating minio storage",
			slog.String("component", "storage"),
			slog.String("type", rawStorage.Type))
		newMinioConnector, err := NewMinioConnector(ctx, minioConfig)
		if err != nil {
			return nil, err
		}
		return newMinioConnector, nil
	default:
		logger.Error("could not find storage type",
			slog.String("component", "storage"),
			slog.String("type", rawStorage.Type))
		return nil, errors.New("could not find storage type")
	}
}
