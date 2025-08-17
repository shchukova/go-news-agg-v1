package newsapi

import (
    "io/ioutil"
    "os"
    "testing"
    "time"

	"path/filepath"
	"testing"
	"time"
)

// TestLoadConfig tests that the configuration is loaded correctly from a JSON file.
func TestLoadConfig(t *testing.T) {
    // Create a temporary config file for testing.
    tempConfigFile, err := ioutil.TempFile("", "config_*.json")
    if err != nil {
        t.Fatalf("Failed to create temp file: %v", err)
    }
    defer os.Remove(tempConfigFile.Name()) // Clean up the file after the test.

    // Write a known good configuration to the temporary file.
    configContent := `{
        "max_page_size": 50,
        "base_url": "https://test.newsapi.org",
        "default_rate_limit_delay_seconds": 10,
        "kafka_broker": "localhost:9092",
        "kafka_topic": "test_topic"
    }`
    if _, err := tempConfigFile.WriteString(configContent); err != nil {
        t.Fatalf("Failed to write to temp file: %v", err)
    }
    tempConfigFile.Close()

    // Load the configuration from the temporary file.
    cfg, err := LoadConfig(tempConfigFile.Name())
    if err != nil {
        t.Fatalf("LoadConfig returned an error: %v", err)
    }

    // Verify that the loaded values match the expected values.
    if cfg.MaxPageSize != 50 {
        t.Errorf("Expected MaxPageSize to be 50, but got %d", cfg.MaxPageSize)
    }
    if cfg.BaseURL != "https://test.newsapi.org" {
        t.Errorf("Expected BaseURL to be 'https://test.newsapi.org', but got '%s'", cfg.BaseURL)
    }
    if cfg.DefaultRateLimitDelaySeconds != 10 {
        t.Errorf("Expected DefaultRateLimitDelaySeconds to be 10, but got %d", cfg.DefaultRateLimitDelaySeconds)
    }
    if cfg.KafkaBroker != "localhost:9092" {
        t.Errorf("Expected KafkaBroker to be 'localhost:9092', but got '%s'", cfg.KafkaBroker)
    }
    if cfg.KafkaTopic != "test_topic" {
        t.Errorf("Expected KafkaTopic to be 'test_topic', but got '%s'", cfg.KafkaTopic)
    }
}


// TestGenerateJSONFilePath tests that the file path is generated correctly with a fixed timestamp.
func TestGenerateJSONFilePath(t *testing.T) {
    // 1. Arrange: Define your test inputs and expected outputs.
    baseOutputDir := "/tmp/test_news"
    country := "us"
    page := 2

    // 2. Act: Mock the timeNow variable.
    // Store the original function to restore it later.
    originalTimeNow := timeNow
    defer func() { timeNow = originalTimeNow }() // Ensure the original function is restored.

    // Set timeNow to a fixed time for the test.
    fixedTime := time.Date(2025, time.August, 15, 12, 0, 0, 0, time.UTC)
    timeNow = func() time.Time { return fixedTime }

    expectedDir := filepath.Join(baseOutputDir, "2025", "08")
    expectedPath := filepath.Join(expectedDir, "2025-08-15_12-00-00_us_page2.json")

    // 3. Act: Call the function with the mocked dependency.
    fullOutputDir, fullJSONPath := generateJSONFilePath(baseOutputDir, country, page)

    // 4. Assert: Check if the actual output matches the expected output.
    if fullOutputDir != expectedDir {
        t.Errorf("Expected output directory '%s', but got '%s'", expectedDir, fullOutputDir)
    }
    if fullJSONPath != expectedPath {
        t.Errorf("Expected full path '%s', but got '%s'", expectedPath, fullJSONPath)
    }
}