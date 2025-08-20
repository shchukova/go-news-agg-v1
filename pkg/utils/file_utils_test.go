package utils

import (
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateJSONFilePath(t *testing.T) {
	// Create a mock time provider
	fixedTime := time.Date(2025, time.August, 15, 12, 0, 0, 0, time.UTC)
	mockTimeProvider := NewMockTimeProvider(fixedTime)
	generator := NewFilePathGenerator(mockTimeProvider)

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

func TestValidateFilePath(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		wantErr  bool
	}{
		{
			name:     "valid relative path",
			filePath: "test/file.txt",
			wantErr:  false,
		},
		{
			name:     "valid absolute path",
			filePath: "/tmp/test.txt",
			wantErr:  false,
		},
		{
			name:     "empty path",
			filePath: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFilePath(tt.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileExists(t *testing.T) {
	// Test with a directory that should exist
	if !FileExists("/tmp") {
		t.Error("Expected /tmp to exist")
	}

	// Test with a file that shouldn't exist
	if FileExists("/non/existent/path") {
		t.Error("Expected /non/existent/path to not exist")
	}
}

func TestDefaultGenerator(t *testing.T) {
	// Test the default generator function
	baseOutputDir := "/tmp/test_news"
	country := "us" 
	page := 1

	fullOutputDir, fullJSONPath := GenerateJSONFilePath(baseOutputDir, country, page)

	// Should generate valid paths
	if fullOutputDir == "" {
		t.Error("Expected non-empty output directory")
	}
	if fullJSONPath == "" {
		t.Error("Expected non-empty file path")
	}

	// Should contain expected components
	if !filepath.IsAbs(fullOutputDir) {
		t.Error("Expected absolute output directory path")
	}
	if !filepath.IsAbs(fullJSONPath) {
		t.Error("Expected absolute file path")
	}
}