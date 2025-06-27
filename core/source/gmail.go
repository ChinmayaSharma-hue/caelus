package source

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/data"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"log/slog"
	"time"
)

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

func (s *GmailSource) GetMetadata(ctx context.Context) ([]data.Metadata, error) {
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

	metadataList := make([]data.Metadata, 0)
	for _, message := range response.Messages {
		metadataList = append(metadataList, data.MailMetadata{
			Id:       message.Id,
			ThreadID: message.ThreadId,
		})
	}
	return metadataList, nil
}

func (s *GmailSource) GetData(ctx context.Context, metadataList []data.Metadata) ([]data.Data, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("ingestion of data from the metadata", slog.String("component", "Source"))
	dataList := make([]data.Data, 0)
	user := "me"
	for _, metadataInterfaceComponent := range metadataList {
		metadataComponent, ok := metadataInterfaceComponent.(data.MailMetadata)
		if !ok {
			logger.Error("could not cast metadata", slog.String("component", "Source"))
			return nil, fmt.Errorf("mailMetadata interface component is not of type Metadata")
		}
		logger.Info("fetching the mail", slog.String("id", metadataComponent.Id))
		request := s.client.Users.Messages.Get(user, metadataComponent.Id).Format("full")
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
			logger.Error("could not extract message body", slog.String("component", "Source"), slog.String("error", err.Error()), slog.String("id", metadataComponent.Id))
			return nil, err
		}
		dataList = append(dataList, data.MailData{Data: body, Metadata: metadataComponent, Sender: sender, Date: date})
	}
	return dataList, nil
}

func (s *GmailSource) GetCollection(ctx context.Context) string {
	logger := ctx.Value("logger").(*slog.Logger)
	logger.Info("fetching collection", slog.String("collection", s.collection))
	return s.collection
}
