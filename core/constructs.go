package core

import "github.com/qdrant/go-client/qdrant"

// Metadata is a list of mailMetadata
type Metadata interface{}

// Data is a list of components
type Data interface {
	String() string
	QdrantPayload() map[string]*qdrant.Value
}
