package source

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/qdrant/go-client/qdrant"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"log/slog"
	"time"
)

type mailMetadata struct {
	id       string
	threadID string
}

type mailData struct {
	metadata mailMetadata
	sender   string
	date     time.Time
	data     string
}

type Source interface {
	GetMetadata(ctx context.Context) ([]core.Metadata, error)
	GetData(ctx context.Context, metadataList []core.Metadata) ([]core.Data, error)
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
		gmailSource, err := NewGmailSource(ctx, gmailConfig, rawSource.Collection)
		if err != nil {
			return nil, err
		}
		return gmailSource, nil
	default:
		logger.Error("could not find source type", slog.String("component", "Source"), slog.String("type", rawSource.Type))
		return nil, fmt.Errorf("source type %s is not supported", rawSource.Type)
	}
}

type GmailSource struct {
	config     config.GmailConfig
	client     *gmail.Service
	collection string
}

func NewGmailSource(ctx context.Context, cfg config.GmailConfig, collection string) (*GmailSource, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	oauthConfig := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
		Scopes:       []string{gmail.GmailReadonlyScope},
		Endpoint:     google.Endpoint,
	}

	token := &oauth2.Token{
		RefreshToken: cfg.RefreshToken,
	}

	// Get token source using the refresh token
	tokenSource := oauthConfig.TokenSource(ctx, token)

	// Create Gmail API client
	svc, err := gmail.NewService(ctx, option.WithTokenSource(tokenSource))
	if err != nil {
		logger.Error("could not create gmail source", slog.String("component", "Source"), slog.String("error", err.Error()))
		return nil, err
	}

	return &GmailSource{
		config:     cfg,
		client:     svc,
		collection: collection,
	}, nil
}

func (s *GmailSource) GetMetadata(ctx context.Context) ([]core.Metadata, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	user := "me"
	// TODO: make the duration configurable
	logger.Info("fetching metadata", slog.String("user", user))
	request := s.client.Users.Messages.List(user).Q("newer_than:1d")
	response, err := request.Do()
	if err != nil {
		logger.Error("could not list users", slog.String("component", "Source"), slog.String("error", err.Error()))
		return nil, err
	}

	metadataList := make([]core.Metadata, 0)
	for _, message := range response.Messages {
		metadataList = append(metadataList, mailMetadata{
			id:       message.Id,
			threadID: message.ThreadId,
		})
	}
	return metadataList, nil
}

func (s *GmailSource) GetData(ctx context.Context, metadataList []core.Metadata) ([]core.Data, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("ingestion of data from the metadata", slog.String("component", "Source"))
	dataList := make([]core.Data, 0)
	user := "me"
	for _, metadataInterfaceComponent := range metadataList {
		metadataComponent, ok := metadataInterfaceComponent.(mailMetadata)
		if !ok {
			logger.Error("could not cast metadata", slog.String("component", "Source"))
			return nil, fmt.Errorf("mailMetadata interface component is not of type Metadata")
		}
		logger.Info("fetching the mail", slog.String("id", metadataComponent.id))
		request := s.client.Users.Messages.Get(user, metadataComponent.id).Format("full")
		response, err := request.Do()
		if err != nil {
			return nil, err
		}
		sender := ""
		date := time.Time{}
		for _, header := range response.Payload.Headers {
			switch header.Name {
			case "Sender":
				sender = header.Value
			case "Date":
				dateStr := header.Value
				if len(dateStr) >= 31 {
					dateStr = dateStr[:31] // Keep only "Sun, 15 Jun 2025 13:49:35"
				}
				layout := "Mon, 02 Jan 2006 15:04:05 -0700" // matches the format
				date, err = time.Parse(layout, dateStr)
				if err != nil {
					logger.Error("could not parse date", slog.String("component", "Source"), slog.String("error", err.Error()), slog.String("date", dateStr))
					// TODO: Find a dump or a solution for unparsable dates
					return nil, err
				}
			}
		}
		body, err := extractMessageBody(response.Payload)
		if err != nil {
			logger.Error("could not extract message body", slog.String("component", "Source"), slog.String("error", err.Error()), slog.String("id", metadataComponent.id))
			return nil, err
		}
		dataList = append(dataList, mailData{data: body, metadata: metadataComponent, sender: sender, date: date})
	}
	return dataList, nil
}

func (s *GmailSource) GetCollection(ctx context.Context) string {
	logger := ctx.Value("logger").(*slog.Logger)
	logger.Info("fetching collection", slog.String("collection", s.collection))
	return s.collection
}

func (md mailData) QdrantPayload() map[string]*qdrant.Value {
	return map[string]*qdrant.Value{
		"id":        {Kind: &qdrant.Value_StringValue{StringValue: md.metadata.id}},
		"thread_id": {Kind: &qdrant.Value_StringValue{StringValue: md.metadata.threadID}},
		"sender":    {Kind: &qdrant.Value_StringValue{StringValue: md.sender}},
		"date":      {Kind: &qdrant.Value_DoubleValue{DoubleValue: float64(md.date.Unix())}},
		"data":      {Kind: &qdrant.Value_StringValue{StringValue: md.data}},
	}
}

func (md mailData) String() string {
	return md.data
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
