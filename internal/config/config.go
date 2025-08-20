package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

// Config holds all the application's configuration parameters
type Config struct {
	MaxPageSize                  int    `json:"max_page_size"`
	BaseURL                      string `json:"base_url"`
	DefaultRateLimitDelaySeconds int    `json:"default_rate_limit_delay_seconds"`
	KafkaBroker                  string `json:"kafka_broker"`
	KafkaTopic                   string `json:"kafka_topic"`
	TimeoutSeconds               int    `json:"timeout_seconds"`
	MaxRetries                   int    `json:"max_retries"`
	OutputDir                    string `json:"output_dir"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		MaxPageSize:                  20,
		BaseURL:                      "https://newsapi.org/v2/top-headlines",
		DefaultRateLimitDelaySeconds: 60,
		KafkaBroker:                  "localhost:9092",
		KafkaTopic:                   "news_files",
		TimeoutSeconds:               30,
		MaxRetries:                   3,
		OutputDir:                    "/tmp/news_downloads",
	}
}

// LoadConfig reads the configuration from a JSON file
func LoadConfig(filePath string) (*Config, error) {
	if filePath == "" {
		return nil, fmt.Errorf("config file path cannot be empty")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file '%s': %w", filePath, err)
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", filePath, err)
	}

	// Start with default config and override with file values
	cfg := DefaultConfig()
	if err := json.Unmarshal(bytes, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config JSON from '%s': %w", filePath, err)
	}

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration in '%s': %w", filePath, err)
	}

	return cfg, nil
}

// LoadConfigFromEnv loads configuration from environment variables with fallback to defaults
func LoadConfigFromEnv() *Config {
	cfg := DefaultConfig()

	if val := os.Getenv("NEWS_MAX_PAGE_SIZE"); val != "" {
		// Note: In a production app, you'd want proper error handling for parsing
		// For simplicity, we'll keep defaults if parsing fails
		if parsed, err := parseIntFromEnv(val); err == nil && parsed > 0 && parsed <= 100 {
			cfg.MaxPageSize = parsed
		}
	}

	if val := os.Getenv("NEWS_BASE_URL"); val != "" {
		cfg.BaseURL = val
	}

	if val := os.Getenv("NEWS_RATE_LIMIT_DELAY"); val != "" {
		if parsed, err := parseIntFromEnv(val); err == nil && parsed > 0 {
			cfg.DefaultRateLimitDelaySeconds = parsed
		}
	}

	if val := os.Getenv("KAFKA_BROKER"); val != "" {
		cfg.KafkaBroker = val
	}

	if val := os.Getenv("KAFKA_TOPIC"); val != "" {
		cfg.KafkaTopic = val
	}

	if val := os.Getenv("NEWS_TIMEOUT"); val != "" {
		if parsed, err := parseIntFromEnv(val); err == nil && parsed > 0 {
			cfg.TimeoutSeconds = parsed
		}
	}

	if val := os.Getenv("NEWS_MAX_RETRIES"); val != "" {
		if parsed, err := parseIntFromEnv(val); err == nil && parsed >= 0 {
			cfg.MaxRetries = parsed
		}
	}

	if val := os.Getenv("NEWS_OUTPUT_DIR"); val != "" {
		cfg.OutputDir = val
	}

	return cfg
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.MaxPageSize <= 0 || c.MaxPageSize > 100 {
		return fmt.Errorf("max_page_size must be between 1 and 100, got %d", c.MaxPageSize)
	}

	if c.BaseURL == "" {
		return fmt.Errorf("base_url cannot be empty")
	}

	if c.DefaultRateLimitDelaySeconds < 0 {
		return fmt.Errorf("default_rate_limit_delay_seconds cannot be negative, got %d", c.DefaultRateLimitDelaySeconds)
	}

	if c.KafkaBroker == "" {
		return fmt.Errorf("kafka_broker cannot be empty")
	}

	if c.KafkaTopic == "" {
		return fmt.Errorf("kafka_topic cannot be empty")
	}

	if c.TimeoutSeconds <= 0 {
		return fmt.Errorf("timeout_seconds must be positive, got %d", c.TimeoutSeconds)
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries cannot be negative, got %d", c.MaxRetries)
	}

	if c.OutputDir == "" {
		return fmt.Errorf("output_dir cannot be empty")
	}

	return nil
}

// SaveConfig saves the configuration to a JSON file
func (c *Config) SaveConfig(filePath string) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("cannot save invalid config: %w", err)
	}

	bytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config to JSON: %w", err)
	}

	if err := ioutil.WriteFile(filePath, bytes, 0644); err != nil {
		return fmt.Errorf("failed to write config to file '%s': %w", filePath, err)
	}

	return nil
}

// parseIntFromEnv is a helper function to parse integers from environment variables
func parseIntFromEnv(value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("empty value")
	}
	
	// Use strconv.Atoi for proper integer parsing
	// This will reject floats like "42.5" and other invalid formats
	result, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid integer format: %w", err)
	}
	
	return result, nil
}