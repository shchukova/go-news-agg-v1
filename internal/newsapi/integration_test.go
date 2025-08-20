package newsapi

import (
	"testing"

	"go-news-agg/internal/config"
)

func TestConfigIntegration(t *testing.T) {
	cfg := config.DefaultConfig()
	
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should be valid: %v", err)
	}
}

func TestDownloadRequestIntegration(t *testing.T) {
	req := NewDownloadRequest("test-key", "us")
	
	if req == nil {
		t.Fatal("NewDownloadRequest returned nil")
	}

	if err := req.Validate(); err != nil {
		t.Errorf("Valid request should pass validation: %v", err)
	}
}