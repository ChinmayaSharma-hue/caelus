package vectorstore

import (
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"google.golang.org/grpc"
)

type VectorStore interface {
	Upsert(data []core.Data) error
}

type QdrantConnector struct {
	config         config.QdrantConfig
	grpcConnection *grpc.ClientConn
}

func NewQdrantConnector(config config.QdrantConfig) (*QdrantConnector, error) {
	url := config.Host + ":" + config.Port
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &QdrantConnector{config: config, grpcConnection: conn}, nil
}

func (q *QdrantConnector) Upsert(data []core.Data) error {
	for _, d := range data {
		_ = d.QdrantPayload()
	}
	return nil
}
