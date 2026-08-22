package rabbitmq

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadConfigFromFile reads and parses a JSON or YAML config file.
// Format is selected by extension: .json, .yaml, or .yml.
func LoadConfigFromFile(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		return LoadConfigFromJSON(f)
	case ".yaml", ".yml":
		return LoadConfigFromYAML(f)
	default:
		return Config{}, fmt.Errorf("%w: unsupported config extension %q", ErrConfigInvalid, ext)
	}
}

// LoadConfigFromJSON decodes JSON configuration from r.
func LoadConfigFromJSON(r io.Reader) (Config, error) {
	var cfg Config
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode json config: %w", err)
	}
	return prepareConfig(cfg)
}

// LoadConfigFromYAML decodes YAML configuration from r.
func LoadConfigFromYAML(r io.Reader) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode yaml config: %w", err)
	}
	return prepareConfig(cfg)
}

func prepareConfig(cfg Config) (Config, error) {
	cfg.ApplyDefaults()
	cfg.ApplyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
