package source

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/data"
	"google.golang.org/api/gmail/v1"
	"log/slog"
)

type Source interface {
	GetMetadata(ctx context.Context) ([]data.Metadata, error)
	GetData(ctx context.Context, metadataList []data.Metadata) ([]data.Data, error)
	GetCollection(ctx context.Context) string
}

func NewSource(ctx context.Context, sourceConfig config.Source) (Source, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	rawSource, ok := sourceConfig.(config.RawSource)
	if !ok {
		logger.Error("could not cast source", slog.String("component", "Source"))
		return nil, fmt.Errorf("source config is not a raw source")
	}
	switch rawSource.Type {
	case "gmail":
		gmailConfig, ok := rawSource.Value.(config.GmailConfig)
		if !ok {
			logger.Error("could not cast gmail config", slog.String("component", "Source"), slog.String("type", rawSource.Type))
			return nil, fmt.Errorf("source config is not a gmail config")
		}
		logger.Info("creating a new gmail source", slog.String("component", "Source"), slog.String("type", rawSource.Type))
		gmailSource, err := NewGmailConnector(ctx, gmailConfig, rawSource.Collection)
		if err != nil {
			return nil, err
		}
		return gmailSource, nil
	default:
		logger.Error("could not find source type", slog.String("component", "Source"), slog.String("type", rawSource.Type))
		return nil, fmt.Errorf("source type %s is not supported", rawSource.Type)
	}
}

func extractMessageBody(payload *gmail.MessagePart) (string, error) {
	if payload == nil {
		return "", fmt.Errorf("nil payload")
	}
	if payload.MimeType == "text/plain" || payload.MimeType == "text/html" {
		data := payload.Body.Data
		if data == "" {
			return "", fmt.Errorf("no data in part")
		}
		decodedData, err := base64.URLEncoding.DecodeString(data)
		if err != nil {
			decodedData, err = base64.StdEncoding.DecodeString(data)
			if err != nil {
				return "", fmt.Errorf("error decoding body: %w", err)
			}
		}
		return string(decodedData), nil
	}
	for _, part := range payload.Parts {
		body, err := extractMessageBody(part)
		if err == nil && body != "" {
			return body, nil
		}
	}
	return "", fmt.Errorf("no body found")
}
