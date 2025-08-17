package newsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go-news-agg/config"
	"go-news-agg/kafka_producer"
	"go-news-agg/utils"
)

// Test configuration loading
func TestLoadConfig(t *testing.T) {
	// Create a temporary config file for testing
	tempConfigFile, err := ioutil.TempFile("", "config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempConfigFile.Name())

	// Write a known good configuration to the temporary file
	configContent := `{
		"max_page_size": 50,
		"base_url": "https://test.newsapi.org",
		"default_rate_limit_delay_seconds": 10,
		"kafka_broker": "localhost:9092",
		"kafka_topic": "test_topic",
		"timeout_seconds": 30,
		"max_retries": 3,
		"output_dir": "/tmp/test_news"
	}`
	if _, err := tempConfigFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempConfigFile.Close()

	// Load the configuration from the temporary file
	cfg, err := config.LoadConfig(tempConfigFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig returned an error: %v", err)
	}

	// Verify that the loaded values match the expected values
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

// Test configuration validation
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     config.DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid page size",
			cfg: &config.Config{
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
		},
		{
			name: "empty base URL",
			cfg: &config.Config{
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test file path generation
func TestGenerateJSONFilePath(t *testing.T) {
	// Create a mock time provider
	fixedTime := time.Date(2025, time.August, 15, 12, 0, 0, 0, time.UTC)
	mockTimeProvider := utils.NewMockTimeProvider(fixedTime)
	generator := utils.NewFilePathGenerator(mockTimeProvider)

	baseOutputDir := "/tmp/test_news"
	country := "us"
	page := 2

	expectedDir := filepath.Join(baseOutputDir, "2025", "08")
	expectedPath := filepath.Join(expectedDir, "2025-08-15_12-00-00_us_page2.json")

	fullOutputDir, fullJSONPath := generator.GenerateJSONFilePath(baseOutputDir, country, page)

	if fullOutputDir != expectedDir {
		t.Errorf("Expected output directory '%s', but got '%s'", expectedDir, fullOutputDir)
	}
	if fullJSONPath != expectedPath {
		t.Errorf("Expected full path '%s', but got '%s'", expectedPath, fullJSONPath)
	}
}

// Test download request validation
func TestDownloadRequestValidation(t *testing.T) {
	tests := []struct {
		name    string
		req     *DownloadRequest
		wantErr bool
	}{
		{
			name: "valid request",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  20,
				StartPage: 1,
				SortBy:    "publishedAt",
			},
			wantErr: false,
		},
		{
			name: "empty API key",
			req: &DownloadRequest{
				APIKey:    "",
				Country:   "us",
				PageSize:  20,
				StartPage: 1,
			},
			wantErr: true,
		},
		{
			name: "invalid page size",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  150,
				StartPage: 1,
			},
			wantErr: true,
		},
		{
			name: "invalid sort by",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  20,
				StartPage: 1,
				SortBy:    "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadRequest.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test NewsAPI client
func TestNewsAPIClient(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BaseURL = "https://test-api.newsapi.org"

	// Create mock HTTP client
	mockClient := NewMockHTTPClient()
	
	// Setup mock response
	mockAPIResponse := NewsAPIResponse{
		Status:       "ok",
		TotalResults: 1,
		Articles: []Article{
			{
				Title:       "Test Article",
				Description: "Test Description",
				URL:         "https://example.com",
			},
		},
	}
	
	responseBody, _ := json.Marshal(mockAPIResponse)
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewBuffer(responseBody)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("X-RateLimit-Limit", "100")
	mockResponse.Header.Set("X-RateLimit-Remaining", "99")
	mockResponse.Header.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(1*time.Hour).Unix(), 10))

	mockClient.SetResponse("*", mockResponse)

	// Create client with mock HTTP client
	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// Test fetch
	req := &DownloadRequest{
		APIKey:   "test-api-key",
		Country:  "us",
		PageSize: 20,
	}

	ctx := context.Background()
	resp, limits, err := client.FetchNewsPage(ctx, req, 1)

	if err != nil {
		t.Fatalf("FetchNewsPage returned error: %v", err)
	}

	if resp == nil {
		t.Fatal("FetchNewsPage returned nil response")
	}

	if resp.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", resp.Status)
	}

	if len(resp.Articles) != 1 {
		t.Errorf("Expected 1 article, got %d", len(resp.Articles))
	}

	if limits == nil {
		t.Fatal("FetchNewsPage returned nil limits")
	}

	if limits.Limit != 100 {
		t.Errorf("Expected rate limit 100, got %d", limits.Limit)
	}
}

// Test NewsAPI client error handling
func TestNewsAPIClientErrorHandling(t *testing.T) {
	cfg := config.DefaultConfig()
	mockClient := NewMockHTTPClient()

	// Test 404 error
	mockResponse := &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       ioutil.NopCloser(strings.NewReader(`{"status":"error","code":"notFound","message":"Not Found"}`)),
		Header:     make(http.Header),
	}
	mockClient.SetResponse("*", mockResponse)

	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)
	req := &DownloadRequest{
		APIKey:   "test-api-key",
		Country:  "us",
		PageSize: 20,
	}

	ctx := context.Background()
	_, _, err := client.FetchNewsPage(ctx, req, 1)

	if err == nil {
		t.Fatal("Expected error for 404 response, got nil")
	}

	if newsAPIErr, ok := err.(*NewsAPIError); ok {
		if newsAPIErr.StatusCode != 404 {
			t.Errorf("Expected status code 404, got %d", newsAPIErr.StatusCode)
		}
		if newsAPIErr.Code != "notFound" {
			t.Errorf("Expected error code 'notFound', got '%s'", newsAPIErr.Code)
		}
	} else {
		t.Errorf("Expected NewsAPIError, got %T", err)
	}
}

// Test rate limiting
func TestRateLimiting(t *testing.T) {
	cfg := config.DefaultConfig()
	mockClient := NewMockHTTPClient()

	// Setup rate limit response
	mockResponse := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       ioutil.NopCloser(strings.NewReader(`{"status":"error","code":"rateLimited","message":"Rate limit exceeded"}`)),
		Header:     make(http.Header),
	}
	resetTime := time.Now().Add(2 * time.Second)
	mockResponse.Header.Set("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))
	mockClient.SetResponse("*", mockResponse)

	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)
	req := &DownloadRequest{
		APIKey:   "test-api-key",
		Country:  "us",
		PageSize: 20,
	}

	ctx := context.Background()
	_, _, err := client.FetchNewsPage(ctx, req, 1)

	if err == nil {
		t.Fatal("Expected rate limit error, got nil")
	}

	if rateLimitErr, ok := err.(*RateLimitError); ok {
		if rateLimitErr.RetryAfter <= 0 {
			t.Errorf("Expected positive retry after duration, got %v", rateLimitErr.RetryAfter)
		}
	} else {
		t.Errorf("Expected RateLimitError, got %T", err)
	}
}

// Test downloader integration
func TestNewsDownloaderIntegration(t *testing.T) {
	// Create temporary directory for output
	tempDir, err := ioutil.TempDir("", "news_download_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup configuration
	cfg := config.DefaultConfig()
	cfg.OutputDir = tempDir
	cfg.KafkaBroker = "mock://"

	// Create mock HTTP client
	mockClient := NewMockHTTPClient()
	mockAPIResponse := NewsAPIResponse{
		Status:       "ok",
		TotalResults: 1,
		Articles: []Article{
			{
				Title:       "Test Article",
				Description: "Test Description",
				URL:         "https://example.com",
			},
		},
	}
	
	responseBody, _ := json.Marshal(mockAPIResponse)
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewBuffer(responseBody)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("X-RateLimit-Limit", "100")
	mockResponse.Header.Set("X-RateLimit-Remaining", "99")
	mockClient.SetResponse("*", mockResponse)

	// Create mock Kafka publisher
	mockPublisher := kafka_producer.NewMockKafkaPublisher()

	// Create NewsAPI client and downloader
	apiClient := NewNewsAPIClientWithHTTPClient(cfg, mockClient)
	downloader := NewNewsDownloader(apiClient, mockPublisher, cfg)

	// Create download request
	req := NewDownloadRequest("test-api-key", "us")
	req.PageSize = 20

	// Execute download
	ctx := context.Background()
	result, err := downloader.DownloadAllNewsToFile(ctx, req)

	if err != nil {
		t.Fatalf("DownloadAllNewsToFile returned error: %v", err)
	}

	if result == nil {
		t.Fatal("DownloadAllNewsToFile returned nil result")
	}

	if result.TotalArticles != 1 {
		t.Errorf("Expected 1 total article, got %d", result.TotalArticles)
	}

	if result.PagesDownloaded != 1 {
		t.Errorf("Expected 1 page downloaded, got %d", result.PagesDownloaded)
	}

	if len(result.FilePaths) != 1 {
		t.Errorf("Expected 1 file path, got %d", len(result.FilePaths))
	}

	// Verify file was created
	if len(result.FilePaths) > 0 {
		filePath := result.FilePaths[0]
		if !utils.FileExists(filePath) {
			t.Errorf("Expected file to exist at %s", filePath)
		}

		// Verify file content
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			t.Errorf("Failed to read file %s: %v", filePath, err)
		}

		var savedResponse NewsAPIResponse
		if err := json.Unmarshal(content, &savedResponse); err != nil {
			t.Errorf("Failed to unmarshal saved file: %v", err)
		}

		if savedResponse.Status != "ok" {
			t.Errorf("Expected saved status 'ok', got '%s'", savedResponse.Status)
		}
	}

	// Verify Kafka message was published
	publishedMessages := mockPublisher.GetPublishedMessages()
	if len(publishedMessages) != 1 {
		t.Errorf("Expected 1 Kafka message, got %d", len(publishedMessages))
	}

	if len(publishedMessages) > 0 {
		msg := publishedMessages[0]
		if msg.Topic != cfg.KafkaTopic {
			t.Errorf("Expected topic '%s', got '%s'", cfg.KafkaTopic, msg.Topic)
		}
		if msg.Message != result.FilePaths[0] {
			t.Errorf("Expected message '%s', got '%s'", result.FilePaths[0], msg.Message)
		}
	}
}

// Test context cancellation
func TestDownloaderContextCancellation(t *testing.T) {
	cfg := config.DefaultConfig()
	mockClient := NewMockHTTPClient()
	mockPublisher := kafka_producer.NewMockKafkaPublisher()

	// Setup a slow response to test cancellation
	mockClient.SetError("*", fmt.Errorf("request timeout"))

	apiClient := NewNewsAPIClientWithHTTPClient(cfg, mockClient)
	downloader := NewNewsDownloader(apiClient, mockPublisher, cfg)

	req := NewDownloadRequest("test-api-key", "us")

	// Create context with immediate cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := downloader.DownloadAllNewsToFile(ctx, req)

	if err == nil {
		t.Fatal("Expected cancellation error, got nil")
	}

	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("Expected cancellation error, got: %v", err)
	}
}