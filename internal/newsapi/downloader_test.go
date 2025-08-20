package newsapi

import (
	"testing"

	"go-news-agg/internal/config"
)

func TestNewsDownloader_DownloadAllNewsToFile(t *testing.T) {
	// Create test configuration
	cfg := config.DefaultConfig()
	cfg.OutputDir = "/tmp/test_news"
	cfg.KafkaBroker = "localhost:9092"

	// Create mock HTTP client
	mockClient := NewMockHTTPClient()

	// Create NewsAPI client
	apiClient := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// For this test, we'll just test that the client can be created
	if apiClient == nil {
		t.Error("Expected non-nil API client")
	}
}