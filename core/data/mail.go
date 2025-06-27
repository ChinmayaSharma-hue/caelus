package data

import (
	"github.com/qdrant/go-client/qdrant"
	"time"
)

type MailMetadata struct {
	Id       string
	ThreadID string
}

type MailData struct {
	Metadata MailMetadata
	Sender   string
	Date     time.Time
	Data     string
}

func (mmd MailMetadata) String() string {
	return mmd.Id
}

func (md MailData) QdrantPayload() map[string]*qdrant.Value {
	return map[string]*qdrant.Value{
		"id":        {Kind: &qdrant.Value_StringValue{StringValue: md.Metadata.Id}},
		"thread_id": {Kind: &qdrant.Value_StringValue{StringValue: md.Metadata.ThreadID}},
		"sender":    {Kind: &qdrant.Value_StringValue{StringValue: md.Sender}},
		"date":      {Kind: &qdrant.Value_DoubleValue{DoubleValue: float64(md.Date.Unix())}},
		"data":      {Kind: &qdrant.Value_StringValue{StringValue: md.Data}},
	}
}

func (md MailData) String() string {
	return md.Data
}
