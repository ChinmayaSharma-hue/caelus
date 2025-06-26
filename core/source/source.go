package source

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"log"
	"time"
)

// Metadata is a list of mailMetadata
type Metadata interface{}

// Data is a list of components
type Data interface{}

type mailMetadata struct {
	id       string
	threadID string
	sender   string
	date     string
}

type mailData struct {
	data string
}

type Source interface {
	GetMetadata() ([]Metadata, error)
	GetData(metadataList []Metadata) ([]Data, error)
}

type GmailSource struct {
	config config.GmailConfig
	client *gmail.Service
}

func NewGmailSource(ctx context.Context, cfg config.GmailConfig) *GmailSource {
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
		log.Fatalf("Failed to create Gmail client: %v", err)
	}

	return &GmailSource{
		config: cfg,
		client: svc,
	}
}

func (s *GmailSource) GetMetadata() ([]Metadata, error) {
	user := "me"
	// TODO: make the duration configurable
	request := s.client.Users.Messages.List(user).Q("newer_than:1d")
	response, err := request.Do()
	if err != nil {
		return nil, err
	}

	metadataList := make([]Metadata, 0)
	for _, message := range response.Messages {
		metadataComponent := mailMetadata{
			id:       message.Id,
			threadID: message.ThreadId,
		}
		for _, header := range message.Payload.Headers {
			switch header.Name {
			case "Sender":
				metadataComponent.sender = header.Value
			case "Date":
				dateStr := header.Value
				if len(dateStr) >= 31 {
					dateStr = dateStr[:31] // Keep only "Sun, 15 Jun 2025 13:49:35"
				}
				layout := "Mon, 02 Jan 2006 15:04:05 -0700" // matches the format
				date, err := time.Parse(layout, dateStr)
				if err != nil {
					// TODO: Find a dump or a solution for unparsable dates
					return nil, err
				}
				metadataComponent.date = date.Format("2006-01-02")
			}
		}
		metadataList = append(metadataList, metadataComponent)
	}
	return metadataList, nil
}

func (s *GmailSource) GetData(metadataList []Metadata) (Data, error) {
	dataList := make([]Data, 0)
	user := "me"
	for _, metadataInterfaceComponent := range metadataList {
		metadataComponent, ok := metadataInterfaceComponent.(mailMetadata)
		if !ok {
			return nil, fmt.Errorf("mailMetadata interface component is not of type Metadata")
		}
		message := s.client.Users.Messages.Get(user, metadataComponent.id).Format("full")
		response, err := message.Do()
		if err != nil {
			return nil, err
		}
		dataList = append(dataList, mailData{data: response.Payload.Body.Data})
	}
	return dataList, nil
}
