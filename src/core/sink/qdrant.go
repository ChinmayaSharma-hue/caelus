package sink

import (
	"context"
	"errors"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	data2 "github.com/ChinmayaSharma-hue/caelus/src/core/data"
	"github.com/ChinmayaSharma-hue/caelus/src/core/engine"
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
	collection     string
}

func NewQdrantConnector(ctx context.Context, config config.QdrantConfig, generator engine.Engine) (*QdrantConnector, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	url := config.Host + ":" + config.Port
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		logger.Error("failed to connect to qdrant", slog.String("url", url), slog.String("component", "Sink"))
		return nil, err
	}
	return &QdrantConnector{config: config, grpcConnection: conn, generator: generator, collection: config.Collection}, nil
}

func (q *QdrantConnector) Upsert(ctx context.Context, data []data2.Data, size int) error {
	logger := ctx.Value("logger").(*slog.Logger)

	// creating the collection
	collectionsClient := qdrant.NewCollectionsClient(q.grpcConnection)
	_, err := collectionsClient.Get(ctx, &qdrant.GetCollectionInfoRequest{
		CollectionName: q.config.Collection,
	})
	if err != nil {
		// If not found, create it
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			logger.Info("creating collection", slog.String("collection", q.config.Collection), slog.String("component", "Sink"))
			_, err = collectionsClient.Create(context.Background(), &qdrant.CreateCollection{
				CollectionName: q.config.Collection,
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
				logger.Error("failed to create collection", slog.String("collection", q.config.Collection), slog.String("component", "Sink"), slog.Any("error", err))
				return fmt.Errorf("failed to create collection: %w", err)
			}
		} else {
			logger.Error("failed to get collection", slog.String("collection", q.config.Collection), slog.String("component", "Sink"), slog.Any("error", err))
			return fmt.Errorf("failed to check collection: %w", err)
		}
	}

	// creating the point
	logger.Info("creating the points", slog.String("collection", q.config.Collection), slog.String("component", "Sink"))
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
	logger.Info("upserting the points", slog.String("collection", q.config.Collection), slog.String("component", "Sink"))
	pointsClient := qdrant.NewPointsClient(q.grpcConnection)
	_, err = pointsClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: q.config.Collection,
		Points:         points,
	})
	if err != nil {
		logger.Error("could not upsert the points", slog.String("collection", q.config.Collection), slog.String("component", "Sink"), slog.Any("error", err))
		return fmt.Errorf("failed to upsert points: %w", err)
	}

	logger.Info("successfully upserted the points", slog.String("collection", q.config.Collection), slog.String("count", strconv.Itoa(len(points))))

	return nil
}

func (q *QdrantConnector) Fetch(ctx context.Context, filters map[string]string) (map[string]data2.Data, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	// getting all the filters
	collection, ok := filters["collection"]
	if !ok {
		logger.Error("no collection specified", slog.String("component", "sink"))
		return nil, errors.New("no collection specified")
	}
	countStr, ok := filters["count"]
	if !ok {
		logger.Error("no count specified", slog.String("component", "sink"))
		return nil, errors.New("missing 'count' in filters")
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		logger.Error("invalid 'count' in filters", slog.String("component", "sink"), slog.Any("error", err), slog.String("count", filters["count"]))
		return nil, errors.New("invalid 'count' in filters")
	}
	id, ok := filters["id"]
	if !ok {
		logger.Error("no id specified", slog.String("component", "sink"))
		return nil, fmt.Errorf("missing 'id' in filters")
	}

	// getting the point based on ID
	pointsClient := qdrant.NewPointsClient(q.grpcConnection)
	limit := uint32(1)
	scrollResp, err := pointsClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: collection,
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				{
					ConditionOneOf: &qdrant.Condition_Field{
						Field: &qdrant.FieldCondition{
							Key: "mail_id",
							Match: &qdrant.Match{
								MatchValue: &qdrant.Match_Text{Text: id},
							},
						},
					},
				},
				{
					ConditionOneOf: &qdrant.Condition_Field{
						Field: &qdrant.FieldCondition{
							Key: "consumed",
							Match: &qdrant.Match{
								MatchValue: &qdrant.Match_Boolean{Boolean: false},
							},
						},
					},
				},
			},
		},
		Limit: &limit,
		WithVectors: &qdrant.WithVectorsSelector{
			SelectorOptions: &qdrant.WithVectorsSelector_Enable{Enable: true},
		},
	})
	if err != nil {
		logger.Error("could not fetch reference point by payload id", slog.String("id", id), slog.Any("error", err))
		return nil, fmt.Errorf("failed to fetch reference point: %w", err)
	}
	if len(scrollResp.Result) == 0 || scrollResp.Result[0].Vectors == nil {
		logger.Error("could not find reference point by payload id", slog.String("id", id), slog.Any("error", err), slog.String("component", "sink"))
		return nil, fmt.Errorf("reference point with id %s not found or has no vectors", id)
	}

	// Extract vector
	vectorsOutput := scrollResp.Result[0].Vectors
	vector := vectorsOutput.GetVector().Data

	// Perform search
	searchResp, err := pointsClient.Search(ctx, &qdrant.SearchPoints{
		CollectionName: collection,
		Vector:         vector,
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				{
					ConditionOneOf: &qdrant.Condition_Field{
						Field: &qdrant.FieldCondition{
							Key: "consumed",
							Match: &qdrant.Match{
								MatchValue: &qdrant.Match_Boolean{Boolean: false},
							},
						},
					},
				},
			},
		},
		Limit: uint64(count),
		WithPayload: &qdrant.WithPayloadSelector{
			SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true},
		},
	})
	if err != nil {
		logger.Error("could not search for vectors",
			slog.String("collection", collection),
			slog.String("component", "Sink"),
			slog.Any("error", err),
			slog.String("collection", collection),
			slog.String("id", id))
		return nil, fmt.Errorf("failed to search for vectors: %w", err)
	}

	// convert the vectors to []data.Data
	results := make(map[string]data2.Data)
	for _, point := range searchResp.Result {
		results[point.Id.GetUuid()] = data2.FromQdrantPayload(point.Payload)
	}

	return results, nil
}

func (q *QdrantConnector) MarkConsumed(ctx context.Context, ids []string) error {
	logger := ctx.Value("logger").(*slog.Logger)
	uuids := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		uuids = append(uuids, &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: id}})
	}

	if len(uuids) > 0 {
		pointsClient := qdrant.NewPointsClient(q.grpcConnection)
		_, err := pointsClient.SetPayload(ctx, &qdrant.SetPayloadPoints{
			CollectionName: q.config.Collection,
			PointsSelector: &qdrant.PointsSelector{
				PointsSelectorOneOf: &qdrant.PointsSelector_Points{Points: &qdrant.PointsIdsList{Ids: uuids}},
			},
			Payload: map[string]*qdrant.Value{
				"consumed": {Kind: &qdrant.Value_BoolValue{BoolValue: true}},
			},
		})
		if err != nil {
			logger.Error("failed to mark points as consumed", slog.Any("error", err), slog.String("collection", q.config.Collection), slog.String("component", "sink"))
			return fmt.Errorf("failed to mark points as consumed: %w", err)
		}
	}
	return nil
}

func (q *QdrantConnector) GetCollection(ctx context.Context) string {
	return q.config.Collection
}
