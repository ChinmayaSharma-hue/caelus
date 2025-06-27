package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

type Database interface{}

type Source interface{}

type Engine interface{}

type Sink interface{}

type Config struct {
	Ingestor Ingestor `yaml:"ingestor"`
}

type Ingestor struct {
	Sources []RawSource `yaml:"sources"`
	Engine  RawEngine   `yaml:"engine"`
	Sinks   []RawSink   `yaml:"sinks"`
}

type RawSource struct {
	Type       string    `yaml:"type"`
	Collection string    `yaml:"collection"`
	Config     yaml.Node `yaml:"config"`
	Value      Source    `yaml:"value"`
}

type RawEngine struct {
	Type   string    `yaml:"type"`
	Config yaml.Node `yaml:"config"`
	Value  Engine    `yaml:"value"`
}

type RawSink struct {
	Kind   string    `yaml:"kind"`
	Type   string    `yaml:"type"`
	Config yaml.Node `yaml:"config"`
	Value  Sink      `yaml:"value"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Name     string `yaml:"name"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type QdrantConfig struct {
	Host       string `yaml:"host"`
	Port       string `yaml:"port"`
	Collection string `yaml:"collection"`
	Generator  string `yaml:"generator"`
}

type GmailConfig struct {
	Filters      string `yaml:"filters"`
	ClientID     string `yaml:"clientID"`
	ClientSecret string `yaml:"clientSecret"`
	RefreshToken string `yaml:"refreshToken"`
}

type OllamaConfig struct {
	Model    string `yaml:"model"`
	Endpoint string `yaml:"endpoint"`
}

func (rd *RawSink) UnmarshalYAML(value *yaml.Node) error {
	var tmp struct {
		Kind   string    `yaml:"kind"`
		Type   string    `yaml:"type"`
		Config yaml.Node `yaml:"config"`
	}
	if err := value.Decode(&tmp); err != nil {
		return err
	}

	rd.Kind = tmp.Kind
	rd.Type = tmp.Type
	rd.Config = tmp.Config

	switch tmp.Type {
	case "qdrant":
		var cfg QdrantConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding qdrant config: %w", err)
		}
		rd.Value = cfg

	default:
		return fmt.Errorf("unsupported sink type: %s", tmp.Type)
	}

	return nil
}

func (rs *RawSource) UnmarshalYAML(value *yaml.Node) error {
	var tmp struct {
		Type       string    `yaml:"type"`
		Collection string    `yaml:"collection"`
		Config     yaml.Node `yaml:"config"`
	}
	if err := value.Decode(&tmp); err != nil {
		return err
	}

	rs.Type = tmp.Type
	rs.Collection = tmp.Collection
	rs.Config = tmp.Config

	switch tmp.Type {
	case "gmail":
		var cfg GmailConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding gmail config: %w", err)
		}
		rs.Value = cfg
	default:
		return fmt.Errorf("unsupported source type: %s", tmp.Type)
	}

	return nil
}

func (rs *RawEngine) UnmarshalYAML(value *yaml.Node) error {
	var tmp struct {
		Type   string    `yaml:"type"`
		Config yaml.Node `yaml:"config"`
	}

	if err := value.Decode(&tmp); err != nil {
		return err
	}

	rs.Type = tmp.Type
	rs.Config = tmp.Config

	switch tmp.Type {
	case "ollama":
		var cfg OllamaConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding ollama config: %w", err)
		}
		rs.Value = cfg
	default:
		return fmt.Errorf("unsupported source type: %s", tmp.Type)
	}

	return nil
}
