package sink

import (
	"context"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/ChinmayaSharma-hue/caelus/core/data"
	"github.com/ChinmayaSharma-hue/caelus/core/engine"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"strconv"
)

type QdrantConnector struct {
	config         config.QdrantConfig
	grpcConnection *grpc.ClientConn
	generator      engine.Engine
}

func NewQdrantConnector(ctx context.Context, config config.QdrantConfig, generator engine.Engine) (*QdrantConnector, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	url := config.Host + ":" + config.Port
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		logger.Error("failed to connect to qdrant", slog.String("url", url), slog.String("component", "Sink"))
		return nil, err
	}
	return &QdrantConnector{config: config, grpcConnection: conn, generator: generator}, nil
}

func (q *QdrantConnector) Upsert(ctx context.Context, data []data.Data, collection string, size int) error {
	logger := ctx.Value("logger").(*slog.Logger)

	// creating the collection
	collectionsClient := qdrant.NewCollectionsClient(q.grpcConnection)
	_, err := collectionsClient.Get(ctx, &qdrant.GetCollectionInfoRequest{
		CollectionName: collection,
	})
	if err != nil {
		// If not found, create it
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			logger.Info("creating collection", slog.String("collection", collection), slog.String("component", "Sink"))
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
				logger.Error("failed to create collection", slog.String("collection", collection), slog.String("component", "Sink"), slog.Any("error", err))
				return fmt.Errorf("failed to create collection: %w", err)
			}
		} else {
			logger.Error("failed to get collection", slog.String("collection", collection), slog.String("component", "Sink"), slog.Any("error", err))
			return fmt.Errorf("failed to check collection: %w", err)
		}
	}

	// creating the point
	logger.Info("creating the points", slog.String("collection", collection), slog.String("component", "Sink"))
	points := make([]*qdrant.PointStruct, 0, len(data))
	for _, d := range data {
		embedding, err := q.generator.Embed(ctx, d.String())
		if err != nil {
			return err
		}
		if len(embedding) == 0 {
			// todo: log this if it happens
			continue
		}

		uuidStr := uuid.New().String()
		payload := d.QdrantPayload()
		// adding a consumed flag for smarter fetch based on this filter
		payload["consumed"] = &qdrant.Value{Kind: &qdrant.Value_BoolValue{BoolValue: false}}
		point := &qdrant.PointStruct{
			Id:      &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: uuidStr}},
			Payload: payload,
			Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: &qdrant.Vector{Data: embedding}}},
		}
		points = append(points, point)
	}

	// upserting the points
	logger.Info("upserting the points", slog.String("collection", collection), slog.String("component", "Sink"))
	pointsClient := qdrant.NewPointsClient(q.grpcConnection)
	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Points:         points,
	})
	if err != nil {
		logger.Error("could not upsert the points", slog.String("collection", collection), slog.String("component", "Sink"), slog.Any("error", err))
		return fmt.Errorf("failed to upsert points: %w", err)
	}

	logger.Info("successfully upserted the points", slog.String("collection", collection), slog.String("count", strconv.Itoa(len(points))))

	return nil
}
