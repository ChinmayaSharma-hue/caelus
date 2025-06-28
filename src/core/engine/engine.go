package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/src/core/config"
	"io/ioutil"
	"log/slog"
	"net/http"
)

type Engine interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

func NewEngine(ctx context.Context, engineConfig config.RawEngine) (Engine, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	logger.Info("creating new engine", slog.String("component", "engine"))
	switch engineConfig.Type {
	case "ollama":
		ollamaConfig, ok := engineConfig.Value.(config.OllamaConfig)
		if !ok {
			logger.Error("unable to parse ollama config", slog.String("component", "engine"), slog.String("type", engineConfig.Type))
			return nil, fmt.Errorf("engine config is not a ollama config")
		}
		ollamaConnector, err := NewOllamaConnector(ctx, ollamaConfig)
		if err != nil {
			return nil, err
		}
		return ollamaConnector, nil
	default:
		logger.Error("unknown engine", slog.String("component", "engine"), slog.String("type", engineConfig.Type))
		return nil, fmt.Errorf("engine type %s is not supported", engineConfig.Type)
	}
}

type OllamaConnector struct {
	Model    string
	Endpoint string
}

func NewOllamaConnector(ctx context.Context, config config.OllamaConfig) (*OllamaConnector, error) {
	connector := OllamaConnector{Model: config.Model, Endpoint: config.Endpoint}
	err := connector.pullModel(ctx)
	if err != nil {
		return nil, err
	}
	return &connector, nil
}

func (oc *OllamaConnector) pullModel(ctx context.Context) error {
	logger := ctx.Value("logger").(*slog.Logger)

	reqBody := map[string]string{"name": oc.Model}
	jsonData, _ := json.Marshal(reqBody)
	url := oc.Endpoint + "/api/pull"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("creating pull request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	logger.Info("pulling model", slog.String("model", oc.Model), slog.String("endpoint", oc.Endpoint), slog.String("body", string(jsonData)), slog.String("component", "engine"))
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("could not pull the desired model", slog.String("model", oc.Model), slog.String("endpoint", oc.Endpoint), slog.String("component", "engine"))
		return fmt.Errorf("model pull failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		logger.Error("could not pull the desired model", slog.String("model", oc.Model), slog.String("endpoint", oc.Endpoint), slog.String("component", "engine"), slog.String("body", string(body)))
		return fmt.Errorf("model pull failed: %s", body)
	}

	// Stream the response
	buf := make([]byte, 1024)
	for {
		_, err := resp.Body.Read(buf)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			logger.Error("could not pull the desired model", slog.String("model", oc.Model), slog.String("endpoint", oc.Endpoint), slog.String("component", "engine"))
			return fmt.Errorf("error pulling model: %w", err)
		}
	}

	return nil
}

func (oc *OllamaConnector) Embed(ctx context.Context, text string) ([]float32, error) {
	logger := ctx.Value("logger").(*slog.Logger)

	type embedRequest struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
	}
	type embedResponse struct {
		Embedding []float32 `json:"embedding"`
	}

	body, _ := json.Marshal(embedRequest{
		Model:  oc.Model,
		Prompt: text,
	})

	url := oc.Endpoint + "/api/embeddings"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		logger.Error("embedding request failed", slog.String("model", oc.Model), slog.String("component", "engine"), slog.Any("error", err))
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)

	var result embedResponse
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		logger.Error("could not unmarshal embedding response body", slog.String("model", oc.Model), slog.String("component", "engine"), slog.Any("error", err))
		return nil, err
	}

	return result.Embedding, nil
}
