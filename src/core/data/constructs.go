package data

import "github.com/qdrant/go-client/qdrant"

// Metadata is a list of mailMetadata
type Metadata interface {
	String() string
}

// Data is a list of components
type Data interface {
	String() string
	QdrantPayload() map[string]*qdrant.Value
	GetMetadata() Metadata
}
