package config

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	// Test default values
	if cfg.MaxPageSize != 20 {
		t.Errorf("Expected MaxPageSize 20, got %d", cfg.MaxPageSize)
	}

	if cfg.BaseURL != "https://newsapi.org/v2/top-headlines" {
		t.Errorf("Expected default BaseURL, got '%s'", cfg.BaseURL)
	}

	if cfg.DefaultRateLimitDelaySeconds != 60 {
		t.Errorf("Expected DefaultRateLimitDelaySeconds 60, got %d", cfg.DefaultRateLimitDelaySeconds)
	}

	if cfg.KafkaBroker != "localhost:9092" {
		t.Errorf("Expected default KafkaBroker, got '%s'", cfg.KafkaBroker)
	}

	if cfg.KafkaTopic != "news_files" {
		t.Errorf("Expected default KafkaTopic, got '%s'", cfg.KafkaTopic)
	}

	if cfg.TimeoutSeconds != 30 {
		t.Errorf("Expected TimeoutSeconds 30, got %d", cfg.TimeoutSeconds)
	}

	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries 3, got %d", cfg.MaxRetries)
	}

	if cfg.OutputDir != "/tmp/news_downloads" {
		t.Errorf("Expected default OutputDir, got '%s'", cfg.OutputDir)
	}

	// Validate that default config passes validation
	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should be valid, got error: %v", err)
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		configJSON  string
		expectError bool
		errorType   string
	}{
		{
			name: "valid config",
			configJSON: `{
				"max_page_size": 50,
				"base_url": "https://test.newsapi.org/v2/top-headlines",
				"default_rate_limit_delay_seconds": 120,
				"kafka_broker": "test-broker:9092",
				"kafka_topic": "test_topic",
				"timeout_seconds": 45,
				"max_retries": 5,
				"output_dir": "/tmp/test_output"
			}`,
			expectError: false,
		},
		{
			name: "partial config (should merge with defaults)",
			configJSON: `{
				"max_page_size": 75,
				"kafka_topic": "custom_topic"
			}`,
			expectError: false,
		},
		{
			name: "invalid JSON",
			configJSON: `{
				"max_page_size": 50,
				"base_url": "https://test.newsapi.org"
				// missing comma and invalid JSON
			}`,
			expectError: true,
			errorType:   "unmarshal",
		},
		{
			name: "invalid config values",
			configJSON: `{
				"max_page_size": 150,
				"base_url": "",
				"timeout_seconds": -1
			}`,
			expectError: true,
			errorType:   "validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tempFile, err := ioutil.TempFile("", "config_test_*.json")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tempFile.Name())

			// Write test config
			if _, err := tempFile.WriteString(tt.configJSON); err != nil {
				t.Fatalf("Failed to write to temp file: %v", err)
			}
			tempFile.Close()

			// Load config
			cfg, err := LoadConfig(tempFile.Name())

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
					return
				}

				if tt.errorType == "unmarshal" && !containsString(err.Error(), "unmarshal") {
					t.Errorf("Expected unmarshal error, got: %v", err)
				}

				if tt.errorType == "validation" && !containsString(err.Error(), "invalid configuration") {
					t.Errorf("Expected validation error, got: %v", err)
				}

				return
			}

			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
				return
			}

			if cfg == nil {
				t.Fatal("LoadConfig returned nil config")
			}

			// Verify specific test cases
			if tt.name == "valid config" {
				if cfg.MaxPageSize != 50 {
					t.Errorf("Expected MaxPageSize 50, got %d", cfg.MaxPageSize)
				}
				if cfg.KafkaTopic != "test_topic" {
					t.Errorf("Expected KafkaTopic 'test_topic', got '%s'", cfg.KafkaTopic)
				}
			}

			if tt.name == "partial config (should merge with defaults)" {
				if cfg.MaxPageSize != 75 {
					t.Errorf("Expected overridden MaxPageSize 75, got %d", cfg.MaxPageSize)
				}
				if cfg.KafkaTopic != "custom_topic" {
					t.Errorf("Expected overridden KafkaTopic 'custom_topic', got '%s'", cfg.KafkaTopic)
				}
				// Should still have defaults for non-overridden values
				if cfg.TimeoutSeconds != 30 {
					t.Errorf("Expected default TimeoutSeconds 30, got %d", cfg.TimeoutSeconds)
				}
			}
		})
	}
}

func TestLoadConfigFileErrors(t *testing.T) {
	tests := []struct {
		name        string
		filePath    string
		expectError bool
	}{
		{
			name:        "empty file path",
			filePath:    "",
			expectError: true,
		},
		{
			name:        "non-existent file",
			filePath:    "/non/existent/path/config.json",
			expectError: true,
		},
		{
			name:        "directory instead of file",
			filePath:    os.TempDir(),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(tt.filePath)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for file path '%s', got nil", tt.filePath)
				}
				if cfg != nil {
					t.Errorf("Expected nil config for error case, got: %v", cfg)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for file path '%s', got: %v", tt.filePath, err)
				}
			}
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"NEWS_MAX_PAGE_SIZE",
		"NEWS_BASE_URL",
		"NEWS_RATE_LIMIT_DELAY",
		"KAFKA_BROKER",
		"KAFKA_TOPIC",
		"NEWS_TIMEOUT",
		"NEWS_MAX_RETRIES",
		"NEWS_OUTPUT_DIR",
	}

	for _, envVar := range envVars {
		originalEnv[envVar] = os.Getenv(envVar)
		os.Unsetenv(envVar)
	}

	// Restore environment after test
	defer func() {
		for envVar, value := range originalEnv {
			if value != "" {
				os.Setenv(envVar, value)
			} else {
				os.Unsetenv(envVar)
			}
		}
	}()

	tests := []struct {
		name     string
		envVars  map[string]string
		expected map[string]interface{}
	}{
		{
			name:    "no environment variables (should return defaults)",
			envVars: map[string]string{},
			expected: map[string]interface{}{
				"MaxPageSize":    20,
				"TimeoutSeconds": 30,
				"MaxRetries":     3,
			},
		},
		{
			name: "valid environment variables",
			envVars: map[string]string{
				"NEWS_MAX_PAGE_SIZE":    "50",
				"NEWS_BASE_URL":         "https://custom.newsapi.org",
				"NEWS_RATE_LIMIT_DELAY": "120",
				"KAFKA_BROKER":          "custom-broker:9092",
				"KAFKA_TOPIC":           "custom_topic",
				"NEWS_TIMEOUT":          "60",
				"NEWS_MAX_RETRIES":      "5",
				"NEWS_OUTPUT_DIR":       "/custom/output",
			},
			expected: map[string]interface{}{
				"MaxPageSize":                  50,
				"BaseURL":                      "https://custom.newsapi.org",
				"DefaultRateLimitDelaySeconds": 120,
				"KafkaBroker":                  "custom-broker:9092",
				"KafkaTopic":                   "custom_topic",
				"TimeoutSeconds":               60,
				"MaxRetries":                   5,
				"OutputDir":                    "/custom/output",
			},
		},
		{
			name: "invalid environment variables (should use defaults)",
			envVars: map[string]string{
				"NEWS_MAX_PAGE_SIZE": "invalid",
				"NEWS_TIMEOUT":       "not-a-number",
				"NEWS_MAX_RETRIES":   "-1",
			},
			expected: map[string]interface{}{
				"MaxPageSize":    20, // Should fall back to default
				"TimeoutSeconds": 30, // Should fall back to default
				"MaxRetries":     3,  // Should fall back to default
			},
		},
		{
			name: "out of range values (should use defaults)",
			envVars: map[string]string{
				"NEWS_MAX_PAGE_SIZE": "150", // Too large
				"NEWS_TIMEOUT":       "0",   // Invalid
				"NEWS_MAX_RETRIES":   "-5",  // Negative
			},
			expected: map[string]interface{}{
				"MaxPageSize":    20, // Should fall back to default
				"TimeoutSeconds": 30, // Should fall back to default
				"MaxRetries":     3,  // Should fall back to default
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			// Clean up environment variables after test
			defer func() {
				for key := range tt.envVars {
					os.Unsetenv(key)
				}
			}()

			cfg := LoadConfigFromEnv()

			if cfg == nil {
				t.Fatal("LoadConfigFromEnv returned nil")
			}

			// Check expected values
			for field, expectedValue := range tt.expected {
				var actualValue interface{}
				switch field {
				case "MaxPageSize":
					actualValue = cfg.MaxPageSize
				case "BaseURL":
					actualValue = cfg.BaseURL
				case "DefaultRateLimitDelaySeconds":
					actualValue = cfg.DefaultRateLimitDelaySeconds
				case "KafkaBroker":
					actualValue = cfg.KafkaBroker
				case "KafkaTopic":
					actualValue = cfg.KafkaTopic
				case "TimeoutSeconds":
					actualValue = cfg.TimeoutSeconds
				case "MaxRetries":
					actualValue = cfg.MaxRetries
				case "OutputDir":
					actualValue = cfg.OutputDir
				default:
					t.Errorf("Unknown field: %s", field)
					continue
				}

				if actualValue != expectedValue {
					t.Errorf("Expected %s to be %v, got %v", field, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "max page size too small",
			config: &Config{
				MaxPageSize:                  0,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "max_page_size must be between 1 and 100",
		},
		{
			name: "max page size too large",
			config: &Config{
				MaxPageSize:                  150,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "max_page_size must be between 1 and 100",
		},
		{
			name: "empty base URL",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "base_url cannot be empty",
		},
		{
			name: "negative rate limit delay",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: -1,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "default_rate_limit_delay_seconds cannot be negative",
		},
		{
			name: "empty kafka broker",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "kafka_broker cannot be empty",
		},
		{
			name: "empty kafka topic",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "kafka_topic cannot be empty",
		},
		{
			name: "invalid timeout",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               0,
				MaxRetries:                   3,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "timeout_seconds must be positive",
		},
		{
			name: "negative max retries",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   -1,
				OutputDir:                    "/tmp",
			},
			wantErr: true,
			errMsg:  "max_retries cannot be negative",
		},
		{
			name: "empty output dir",
			config: &Config{
				MaxPageSize:                  20,
				BaseURL:                      "https://newsapi.org",
				DefaultRateLimitDelaySeconds: 60,
				KafkaBroker:                  "localhost:9092",
				KafkaTopic:                   "news",
				TimeoutSeconds:               30,
				MaxRetries:                   3,
				OutputDir:                    "",
			},
			wantErr: true,
			errMsg:  "output_dir cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected validation error, got nil")
					return
				}

				if !containsString(err.Error(), tt.errMsg) {
					t.Errorf("Expected error message to contain '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestSaveConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPageSize = 75
	cfg.KafkaTopic = "test_save_topic"

	// Create temporary file
	tempDir, err := ioutil.TempDir("", "config_save_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "test_config.json")

	// Save config
	err = cfg.SaveConfig(configPath)
	if err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatalf("Config file was not created")
	}

	// Load config back and verify
	loadedCfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if loadedCfg.MaxPageSize != cfg.MaxPageSize {
		t.Errorf("Expected MaxPageSize %d, got %d", cfg.MaxPageSize, loadedCfg.MaxPageSize)
	}

	if loadedCfg.KafkaTopic != cfg.KafkaTopic {
		t.Errorf("Expected KafkaTopic '%s', got '%s'", cfg.KafkaTopic, loadedCfg.KafkaTopic)
	}

	// Verify all other fields match
	if loadedCfg.BaseURL != cfg.BaseURL {
		t.Errorf("Expected BaseURL '%s', got '%s'", cfg.BaseURL, loadedCfg.BaseURL)
	}
}

func TestSaveConfigInvalidConfig(t *testing.T) {
	// Create invalid config
	cfg := &Config{
		MaxPageSize: 150, // Invalid
		BaseURL:     "",  // Invalid
	}

	tempDir, err := ioutil.TempDir("", "config_save_invalid_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "invalid_config.json")

	// Save should fail due to validation
	err = cfg.SaveConfig(configPath)
	if err == nil {
		t.Errorf("Expected SaveConfig to fail for invalid config, got nil")
	}

	if !containsString(err.Error(), "cannot save invalid config") {
		t.Errorf("Expected validation error, got: %v", err)
	}
}

func TestSaveConfigInvalidPath(t *testing.T) {
	cfg := DefaultConfig()

	// Try to save to invalid path
	invalidPath := "/non/existent/directory/config.json"

	err := cfg.SaveConfig(invalidPath)
	if err == nil {
		t.Errorf("Expected SaveConfig to fail for invalid path, got nil")
	}

	if !containsString(err.Error(), "failed to write config to file") {
		t.Errorf("Expected file write error, got: %v", err)
	}
}

func TestParseIntFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int
		expectError bool
	}{
		{
			name:        "valid positive integer",
			input:       "42",
			expected:    42,
			expectError: false,
		},
		{
			name:        "valid zero",
			input:       "0",
			expected:    0,
			expectError: false,
		},
		{
			name:        "valid negative integer",
			input:       "-10",
			expected:    -10,
			expectError: false,
		},
		{
			name:        "invalid string",
			input:       "not-a-number",
			expected:    0,
			expectError: true,
		},
		{
			name:        "empty string",
			input:       "",
			expected:    0,
			expectError: true,
		},
		{
			name:        "float number",
			input:       "42.5",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseIntFromEnv(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for input '%s', got nil", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for input '%s', got: %v", tt.input, err)
				}

				if result != tt.expected {
					t.Errorf("Expected result %d for input '%s', got %d", tt.expected, tt.input, result)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}