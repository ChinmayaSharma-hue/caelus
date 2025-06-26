package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"io/ioutil"
	"net/http"
)

type Engine interface {
	Embed(text string) ([]float32, error)
}

func NewEngine(ctx context.Context, engineConfig config.Engine) (Engine, error) {
	rawEngine, ok := engineConfig.(config.RawEngine)
	if !ok {
		return nil, fmt.Errorf("engine config is not a raw engine")
	}
	switch rawEngine.Type {
	case "ollama":
		ollamaConfig, ok := rawEngine.Value.(config.OllamaConfig)
		if !ok {
			return nil, fmt.Errorf("engine config is not a ollama config")
		}
		ollamaConnector, err := NewOllamaConnector(ollamaConfig)
		if err != nil {
			return nil, err
		}
		return ollamaConnector, nil
	default:
		return nil, fmt.Errorf("engine type %s is not supported", rawEngine.Type)
	}
}

type OllamaConnector struct {
	Model    string
	Endpoint string
}

func NewOllamaConnector(config config.OllamaConfig) (*OllamaConnector, error) {
	connector := OllamaConnector{Model: config.Model, Endpoint: config.Endpoint}
	err := connector.pullModel()
	if err != nil {
		return nil, err
	}
	return &connector, nil
}

func (oc *OllamaConnector) pullModel() error {
	reqBody := map[string]string{"name": oc.Model}
	jsonData, _ := json.Marshal(reqBody)
	url := oc.Endpoint + "/api/pull"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("creating pull request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("model pull failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
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
			return fmt.Errorf("error pulling model: %w", err)
		}
	}

	return nil
}

func (oc *OllamaConnector) Embed(text string) ([]float32, error) {
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
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)

	var result embedResponse
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		return nil, err
	}

	return result.Embedding, nil
}
