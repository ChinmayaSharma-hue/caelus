package buffer

import (
	"context"
	"errors"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"github.com/ChinmayaSharma-hue/caelus/src/core/data"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
)

type natsStreamingBuffer struct {
	client   *nats.Conn
	producer jetstream.JetStream
	consumer jetstream.Consumer
	name     string
}

type natsMessage struct {
	message jetstream.Msg
}

func NewNATSStreamingBuffer(ctx context.Context, config config.NatsConfig) (Buffer, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	url := config.Host + ":" + config.Port
	client, err := nats.Connect(url)
	if err != nil {
		logger.Error("could not connect to NATS",
			slog.String("component", "buffer"),
			slog.String("host", config.Host),
			slog.String("port", config.Port),
			slog.String("error", err.Error()))
		return nil, err
	}

	producer, err := jetstream.New(client)
	if err != nil {
		logger.Error("could not create JetStream producer",
			slog.String("component", "buffer"),
			slog.String("host", config.Host),
			slog.String("port", config.Port),
			slog.String("error", err.Error()))
	}

	stream, err := producer.CreateStream(ctx, jetstream.StreamConfig{
		Name:     config.Name,
		Subjects: []string{fmt.Sprintf("%s.new", config.Name)},
	})
	if err != nil {
		logger.Error("could not create JetStream stream",
			slog.String("component", "buffer"),
			slog.String("host", config.Host),
			slog.String("port", config.Port),
			slog.String("error", err.Error()))
		return nil, err
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		logger.Error("could not create JetStream consumer",
			slog.String("component", "buffer"),
			slog.String("host", config.Host),
			slog.String("port", config.Port),
			slog.String("error", err.Error()))
		return nil, err
	}

	return &natsStreamingBuffer{
		client:   client,
		producer: producer,
		consumer: consumer,
		name:     config.Name,
	}, nil
}

func (buffer *natsStreamingBuffer) EnqueueBatch(ctx context.Context, metadata []data.Metadata) error {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("pushing the metadata in a batch to the buffer",
		slog.String("component", "buffer"),
		slog.String("name", buffer.name))
	for _, m := range metadata {
		logger.Info("pushing metadata to the buffer",
			slog.String("metadata", m.String()),
			slog.String("component", "buffer"),
			slog.String("name", buffer.name))
		_, err := buffer.producer.Publish(ctx, fmt.Sprintf("%s.new", buffer.name), []byte(m.String()))
		if err != nil {
			logger.Error("could not enqueue metadata",
				slog.String("name", buffer.name),
				slog.String("error", err.Error()),
				slog.String("metadata", m.String()),
				slog.String("component", "buffer"))
			return err
		}
	}

	return nil
}

func (buffer *natsStreamingBuffer) Enqueue(ctx context.Context, metadata data.Metadata) error {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("pushing the metadata to the buffer",
		slog.String("component", "buffer"),
		slog.String("metadata", metadata.String()),
		slog.String("name", buffer.name))
	_, err := buffer.producer.Publish(ctx, fmt.Sprintf("%s.new", buffer.name), []byte(metadata.String()))
	if err != nil {
		logger.Error("could not enqueue metadata",
			slog.String("component", "buffer"),
			slog.String("name", buffer.name),
			slog.String("error", err.Error()),
			slog.String("metadata", metadata.String()),
			slog.String("component", "buffer"))
		return err
	}

	return nil
}

func (buffer *natsStreamingBuffer) Dequeue(ctx context.Context) (Message, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("dequeuing the buffer",
		slog.String("component", "buffer"),
		slog.String("name", buffer.name))
	batch, err := buffer.consumer.Fetch(1)
	if err != nil {
		logger.Error("could not dequeue metadata",
			slog.String("name", buffer.name),
			slog.String("error", err.Error()),
			slog.String("component", "buffer"))
		return nil, err
	}

	for message := range batch.Messages() {
		return natsMessage{message: message}, nil
	}

	return nil, errors.New("empty buffer")
}

func (buffer *natsStreamingBuffer) MarkConsumed(ctx context.Context, message Message) error {
	logger := ctx.Value("logger").(*slog.Logger)

	castNatsMessage := message.(natsMessage)
	err := castNatsMessage.message.Ack()
	if err != nil {
		logger.Error("could not mark message as consumed",
			slog.String("name", buffer.name),
			slog.String("error", err.Error()),
			slog.String("component", "buffer"))
		return err
	}

	return nil
}

func (msg natsMessage) GetMessageData() string {
	return string(msg.message.Data())
}
