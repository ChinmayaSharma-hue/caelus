package main

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/src/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/src/core/sink"
	"github.com/ChinmayaSharma-hue/caelus/src/core/storage"
	"github.com/google/uuid"
	tiktoken "github.com/pkoukk/tiktoken-go"
	"log"
	"log/slog"
	"os"
	"strconv"
)

// worker listens to the preprocessedBuffer, fetches vectors from the sink and then constructs the prompts and stores it in storage
type worker struct {
	preprocessedBuffer buffer.Buffer
	processedBuffer    buffer.Buffer
	sink               sink.Sink
	storage            storage.Storage
	ctx                context.Context
	cancel             context.CancelFunc
	maxPromptTokens    int
}

type promptID struct {
	ID string `json:"id"`
}

func (pid promptID) String() string {
	return pid.ID
}

func (w *worker) Start() {
	logger := w.ctx.Value("logger").(*slog.Logger)
	logger.Info("starting a new worker to construct prompts", slog.String("component", "processor"))
	for {
		select {
		case <-w.ctx.Done():
			logger.Info("stopping a new worker to construct prompts", slog.String("component", "processor"))
			return
		default:
			// fetch a message from the preprocessedBuffer
			message, err := w.preprocessedBuffer.Dequeue(w.ctx)
			if err != nil {
				logger.Error("could not dequeue message",
					slog.String("component", "processor"),
					slog.Any("error", err))
				continue
			}

			// process the message
			err = w.processMessage(message)
			if err != nil {
				continue
			}

			// mark the message as consumed
			// todo: for cases where the data was consumed from collection, mark those as consumed in buffer too
			err = w.preprocessedBuffer.MarkConsumed(w.ctx, message)
			if err != nil {
				continue
			}
		}
	}
}

func (w *worker) processMessage(message buffer.Message) error {
	logger := w.ctx.Value("logger").(*slog.Logger)

	// get the dataMap in the message
	id := message.GetMessageData()

	// fetch all the vectors from the database that are closest to this message
	filters := map[string]string{
		"collection": w.sink.GetCollection(w.ctx),
		"id":         id,
		"count":      strconv.Itoa(maxVectorFetch),
	}
	dataMap, err := w.sink.Fetch(w.ctx, filters)
	if err != nil {
		return err
	}

	// trim the vectors based on the total number of tokens
	promptBytes, err := os.ReadFile("prompt")
	if err != nil {
		logger.Error("could not read prompt file",
			slog.String("component", "processor"),
			slog.String("id", id),
			slog.Any("error", err))
		return err
	}
	consumedUUIDS := make([]string, 0)
	prompt := string(promptBytes)
	for u, data := range dataMap {
		// constructing a temporary prompt by using the next data
		temporaryPrompt := prompt
		temporaryPrompt += data.String()

		// count the tokens for temporary prompt
		enc, err := tiktoken.EncodingForModel("gpt-4o-preview")
		if err != nil {
			log.Fatal(err)
		}
		tokens := enc.Encode(temporaryPrompt, nil, nil)
		tokenCount := len(tokens)

		// decide whether to keep the prompt or not
		if tokenCount > w.maxPromptTokens {
			continue
		}
		prompt = temporaryPrompt
		consumedUUIDS = append(consumedUUIDS, u)
	}

	// mark all the vectors that will be used as consumed
	err = w.sink.MarkConsumed(w.ctx, consumedUUIDS)
	if err != nil {
		return err
	}

	// store the prompt in storage along with UUID
	objectKey := uuid.New().String()
	err = w.storage.Upload(w.ctx, objectKey, prompt)
	if err != nil {
		return err
	}

	// store the prompt ID in the preprocessedBuffer
	err = w.processedBuffer.Enqueue(w.ctx, promptID{objectKey})
	if err != nil {
		return err
	}

	logger.Info("successfully published a new prompt",
		slog.String("component", "processor"),
		slog.String("id", objectKey))

	return nil
}
