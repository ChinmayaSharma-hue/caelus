package vectorstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/engine"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VectorStore interface {
	Upsert(ctx context.Context, data []core.Data, collection string, size int) error
}

func NewVectorStore(ctx context.Context, dbConfig config.Database, generator engine.Engine) (VectorStore, error) {
	rawDatabase, ok := dbConfig.(config.RawDatabase)
	if !ok {
		return nil, errors.New("database config is not a raw database config")
	}
	switch rawDatabase.Type {
	case "qdrant":
		qdrantConfig, ok := rawDatabase.Value.(config.QdrantConfig)
		if !ok {
			return nil, fmt.Errorf("database config is not a qdrant config")
		}
		qdrantConnector, err := NewQdrantConnector(qdrantConfig, generator)
		if err != nil {
			return nil, err
		}
		return qdrantConnector, nil
	default:
		return nil, fmt.Errorf("database config is not a qdrant config")
	}
}

type QdrantConnector struct {
	config         config.QdrantConfig
	grpcConnection *grpc.ClientConn
	generator      engine.Engine
}

func NewQdrantConnector(config config.QdrantConfig, generator engine.Engine) (*QdrantConnector, error) {
	url := config.Host + ":" + config.Port
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &QdrantConnector{config: config, grpcConnection: conn, generator: generator}, nil
}

func (q *QdrantConnector) Upsert(ctx context.Context, data []core.Data, collection string, size int) error {
	// creating the collection
	collectionsClient := qdrant.NewCollectionsClient(q.grpcConnection)
	_, err := collectionsClient.Get(ctx, &qdrant.GetCollectionInfoRequest{
		CollectionName: collection,
	})
	if err != nil {
		// If not found, create it
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			_, err = collectionsClient.Create(context.Background(), &qdrant.CreateCollection{
				CollectionName: collection,
				VectorsConfig: &qdrant.VectorsConfig{
					Config: &qdrant.VectorsConfig_Params{
						Params: &qdrant.VectorParams{
							Size:     uint64(size),
							Distance: qdrant.Distance_Cosine,
						},
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create collection: %w", err)
			}
		} else {
			return fmt.Errorf("failed to check collection: %w", err)
		}
	}

	// creating the point
	points := make([]*qdrant.PointStruct, 0, len(data))
	for _, d := range data {
		embedding, err := q.generator.Embed(d.String())
		if err != nil {
			return err
		}
		if len(embedding) == 0 {
			// todo: log this if it happens
			continue
		}

		uuidStr := uuid.New().String()
		point := &qdrant.PointStruct{
			Id:      &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: uuidStr}},
			Payload: d.QdrantPayload(),
			Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: embedding}}},
		}
		points = append(points, point)
	}

	// upserting the points
	pointsClient := qdrant.NewPointsClient(q.grpcConnection)
	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Points:         points,
	})
	if err != nil {
		return fmt.Errorf("failed to upsert points: %w", err)
	}

	return nil
}
