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
		"mail_id":   {Kind: &qdrant.Value_StringValue{StringValue: md.Metadata.Id}},
		"thread_id": {Kind: &qdrant.Value_StringValue{StringValue: md.Metadata.ThreadID}},
		"sender":    {Kind: &qdrant.Value_StringValue{StringValue: md.Sender}},
		"date":      {Kind: &qdrant.Value_DoubleValue{DoubleValue: float64(md.Date.Unix())}},
		"data":      {Kind: &qdrant.Value_StringValue{StringValue: md.Data}},
	}
}

func (md MailData) String() string {
	return md.Data
}

func (md MailData) GetMetadata() Metadata {
	return md.Metadata
}

func FromQdrantPayload(payload map[string]*qdrant.Value) Data {
	// todo: send back date
	return MailData{
		Data: payload["data"].Kind.(*qdrant.Value_StringValue).StringValue,
		Metadata: MailMetadata{
			Id:       payload["mail_id"].Kind.(*qdrant.Value_StringValue).StringValue,
			ThreadID: payload["thread_id"].Kind.(*qdrant.Value_StringValue).StringValue,
		},
		Sender: payload["sender"].Kind.(*qdrant.Value_StringValue).StringValue,
	}
}
