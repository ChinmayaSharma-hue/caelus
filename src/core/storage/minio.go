package storage

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io/ioutil"
	"log/slog"
	"strings"
)

type minioConnector struct {
	Client *minio.Client
	Bucket string
}

func NewMinioConnector(ctx context.Context, minioConfig config.MinioConfig) (Storage, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	endpoint := minioConfig.Host + ":" + minioConfig.Port
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(minioConfig.AccessKey, minioConfig.SecretKey, ""),
	})
	if err != nil {
		logger.Error("could not create minio client",
			slog.String("endpoint", endpoint),
			slog.String("error", err.Error()))
		return nil, err
	}

	// Check if bucket exists
	exists, err := minioClient.BucketExists(ctx, minioConfig.Bucket)
	if err != nil {
		logger.Error("error checking if bucket exists",
			slog.String("component", "storage"),
			slog.String("error", err.Error()),
			slog.String("bucket", minioConfig.Bucket))
		return nil, err
	}

	if !exists {
		err = minioClient.MakeBucket(ctx, minioConfig.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			logger.Error("error creating bucket",
				slog.String("component", "storage"),
				slog.String("error", err.Error()),
				slog.String("bucket", minioConfig.Bucket))
			return nil, err
		}
		logger.Info("created new bucket",
			slog.String("component", "storage"),
			slog.String("bucket", minioConfig.Bucket))
	} else {
		logger.Info("bucket already exists",
			slog.String("component", "storage"),
			slog.String("bucket", minioConfig.Bucket))
	}

	return &minioConnector{Client: minioClient, Bucket: minioConfig.Bucket}, nil
}

func (s *minioConnector) Upload(ctx context.Context, key string, data string) error {
	logger := ctx.Value("logger").(*slog.Logger)

	reader := strings.NewReader(data)
	size := int64(len(data))

	_, err := s.Client.PutObject(ctx, s.Bucket, key, reader, size, minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		logger.Error("error uploading file",
			slog.String("component", "storage"),
			slog.String("error", err.Error()),
			slog.String("key", key))
		return err
	}

	return nil
}

func (s *minioConnector) Download(ctx context.Context, key string) (string, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	reader, err := s.Client.GetObject(ctx, s.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		logger.Error("error downloading file",
			slog.String("component", "storage"),
			slog.String("error", err.Error()),
			slog.String("key", key))
	}
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		logger.Error("error downloading file",
			slog.String("component", "storage"),
			slog.String("error", err.Error()),
			slog.String("key", key))
	}

	return string(data), nil
}
