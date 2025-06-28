package main

import (
	"context"
	"github.com/ChinmayaSharma-hue/caelus/src/core/buffer"
	"github.com/ChinmayaSharma-hue/caelus/src/core/storage"
	"github.com/sashabaranov/go-openai"
	"log/slog"
	"sync/atomic"
)

type worker struct {
	client          *openai.Client
	model           string
	processedBuffer buffer.Buffer
	promptStorage   storage.Storage
	responseStorage storage.Storage
	ctx             context.Context
	cancel          context.CancelFunc
	tokensUsed      *int64
	tokenLimit      int64
}

func (w *worker) Start() {
	logger := w.ctx.Value("logger").(*slog.Logger)
	logger.Info("starting a new worker to send prompts", slog.String("component", "processor"))
	for {
		select {
		case <-w.ctx.Done():
			logger.Info("stopping worker", slog.String("component", "processor"))
			return
		default:
			if atomic.LoadInt64(w.tokensUsed) >= w.tokenLimit {
				logger.Info("token limit reached, stopping worker", slog.String("component", "processor"))
				w.cancel()
				return
			}
			message, err := w.processedBuffer.Dequeue(w.ctx)
			if err != nil {
				logger.Error("could not dequeue message", slog.String("component", "processor"), slog.Any("error", err))
				continue
			}
			if err := w.processMessage(message); err != nil {
				continue
			}
			if err := w.processedBuffer.MarkConsumed(w.ctx, message); err != nil {
				continue
			}
		}
	}
}

func (w *worker) processMessage(message buffer.Message) error {
	logger := w.ctx.Value("logger").(*slog.Logger)
	promptID := message.GetMessageData()
	prompt, err := w.promptStorage.Download(w.ctx, promptID)
	if err != nil {
		logger.Error("could not download prompt", slog.String("component", "processor"), slog.Any("error", err), slog.String("id", promptID))
		return err
	}
	response, err := w.client.CreateChatCompletion(w.ctx, openai.ChatCompletionRequest{
		Model: w.model,
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleSystem, Content: "You are an assistant helping me find work in Linux kernel by looking at linux mailing lists."},
			{Role: openai.ChatMessageRoleUser, Content: prompt},
		},
	})
	if err != nil {
		logger.Error("could not create completion message", slog.String("component", "processor"), slog.Any("error", err), slog.String("id", promptID))
		return err
	}
	tokens := int64(response.Usage.TotalTokens)
	atomic.AddInt64(w.tokensUsed, tokens)
	logger.Info("tokens used", slog.String("component", "processor"), slog.Int64("tokens", tokens), slog.Int64("total_tokens", atomic.LoadInt64(w.tokensUsed)))
	if err := w.responseStorage.Upload(w.ctx, promptID, response.Choices[0].Message.Content); err != nil {
		logger.Error("could not upload response", slog.String("component", "processor"), slog.Any("error", err), slog.String("id", promptID))
		return err
	}
	return nil
}
