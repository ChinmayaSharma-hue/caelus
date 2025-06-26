package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

type Database interface{}

type Source interface{}

type Config struct {
	Databases map[string]RawDatabase `yaml:"database"`
	Ingestor  Ingestor               `yaml:"ingestor"`
}

type Ingestor struct {
	Sources []Source `yaml:"sources"`
}

type RawDatabase struct {
	Type   string    `yaml:"type"`
	Config yaml.Node `yaml:"config"`
	Value  Database  `yaml:"value"`
}

type RawSource struct {
	Type   string    `yaml:"type"`
	Config yaml.Node `yaml:"config"`
	Value  Source    `yaml:"value"`
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
}

type GmailConfig struct {
	Filters      string `yaml:"filters"`
	ClientID     string `yaml:"clientID"`
	ClientSecret string `yaml:"clientSecret"`
	RefreshToken string `yaml:"refreshToken"`
}

func (rd *RawDatabase) UnmarshalYAML(value *yaml.Node) error {
	var tmp struct {
		Type   string    `yaml:"type"`
		Config yaml.Node `yaml:"config"`
	}
	if err := value.Decode(&tmp); err != nil {
		return err
	}

	rd.Type = tmp.Type
	rd.Config = tmp.Config

	switch tmp.Type {
	case "postgres":
		var cfg PostgresConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding postgres config: %w", err)
		}
		rd.Value = cfg
	case "qdrant":
		var cfg QdrantConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding qdrant config: %w", err)
		}

	default:
		return fmt.Errorf("unsupported database type: %s", tmp.Type)
	}

	return nil
}

func (rs *RawSource) UnmarshalYAML(value *yaml.Node) error {
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
	case "gmail":
		var cfg GmailConfig
		if err := tmp.Config.Decode(&cfg); err != nil {
			return fmt.Errorf("error decoding gmail config: %w", err)
		}
	default:
		return fmt.Errorf("unsupported source type: %s", tmp.Type)
	}

	return nil
}
