package source

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/qdrant/go-client/qdrant"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"time"
)

type mailMetadata struct {
	id       string
	threadID string
	sender   string
	date     time.Time
}

type mailData struct {
	metadata mailMetadata
	data     string
}

type Source interface {
	GetMetadata() ([]core.Metadata, error)
	GetData(metadataList []core.Metadata) ([]core.Data, error)
}

func NewSource(ctx context.Context, sourceConfig config.Source) (Source, error) {
	rawSource, ok := sourceConfig.(config.RawSource)
	if !ok {
		return nil, fmt.Errorf("source config is not a raw source")
	}
	switch rawSource.Type {
	case "gmail":
		gmailConfig, ok := rawSource.Value.(config.GmailConfig)
		if !ok {
			return nil, fmt.Errorf("source config is not a gmail config")
		}
		gmailSource, err := NewGmailSource(ctx, gmailConfig)
		if err != nil {
			return nil, err
		}
		return gmailSource, nil
	default:
		return nil, fmt.Errorf("source type %s is not supported", rawSource.Type)
	}
}

type GmailSource struct {
	config config.GmailConfig
	client *gmail.Service
}

func NewGmailSource(ctx context.Context, cfg config.GmailConfig) (*GmailSource, error) {
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
		return nil, err
	}

	return &GmailSource{
		config: cfg,
		client: svc,
	}, nil
}

func (s *GmailSource) GetMetadata() ([]core.Metadata, error) {
	user := "me"
	// TODO: make the duration configurable
	request := s.client.Users.Messages.List(user).Q("newer_than:1d")
	response, err := request.Do()
	if err != nil {
		return nil, err
	}

	metadataList := make([]core.Metadata, 0)
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
				metadataComponent.date = date
			}
		}
		metadataList = append(metadataList, metadataComponent)
	}
	return metadataList, nil
}

func (s *GmailSource) GetData(metadataList []core.Metadata) ([]core.Data, error) {
	dataList := make([]core.Data, 0)
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

func (md mailData) QdrantPayload() map[string]*qdrant.Value {
	return map[string]*qdrant.Value{
		"id":        {Kind: &qdrant.Value_StringValue{StringValue: md.metadata.id}},
		"thread_id": {Kind: &qdrant.Value_StringValue{StringValue: md.metadata.threadID}},
		"sender":    {Kind: &qdrant.Value_StringValue{StringValue: md.metadata.sender}},
		"date":      {Kind: &qdrant.Value_DoubleValue{DoubleValue: float64(md.metadata.date.Unix())}},
		"data":      {Kind: &qdrant.Value_StringValue{StringValue: md.data}},
	}
}
