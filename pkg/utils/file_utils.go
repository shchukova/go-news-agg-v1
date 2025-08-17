package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// TimeProvider defines an interface for getting the current time
// This allows for easy mocking in tests
type TimeProvider interface {
	Now() time.Time
}

// RealTimeProvider implements TimeProvider using the actual system time
type RealTimeProvider struct{}

// Now returns the current system time
func (r *RealTimeProvider) Now() time.Time {
	return time.Now()
}

// MockTimeProvider implements TimeProvider with a fixed time for testing
type MockTimeProvider struct {
	fixedTime time.Time
}

// NewMockTimeProvider creates a new mock time provider with the given fixed time
func NewMockTimeProvider(fixedTime time.Time) *MockTimeProvider {
	return &MockTimeProvider{fixedTime: fixedTime}
}

// Now returns the fixed time
func (m *MockTimeProvider) Now() time.Time {
	return m.fixedTime
}

// SetTime updates the fixed time
func (m *MockTimeProvider) SetTime(t time.Time) {
	m.fixedTime = t
}

// FilePathGenerator handles generation of file paths for news data
type FilePathGenerator struct {
	timeProvider TimeProvider
}

// NewFilePathGenerator creates a new file path generator with the given time provider
func NewFilePathGenerator(timeProvider TimeProvider) *FilePathGenerator {
	if timeProvider == nil {
		timeProvider = &RealTimeProvider{}
	}
	
	return &FilePathGenerator{
		timeProvider: timeProvider,
	}
}

// NewDefaultFilePathGenerator creates a file path generator with real time provider
func NewDefaultFilePathGenerator() *FilePathGenerator {
	return NewFilePathGenerator(&RealTimeProvider{})
}

// GenerateJSONFilePath creates the full path for the JSON file based on the current time,
// the provided base output directory, country, and page number
func (g *FilePathGenerator) GenerateJSONFilePath(baseOutputDir, country string, page int) (string, string) {
	now := g.timeProvider.Now()
	
	yearDir := now.Format("2006")
	monthDir := now.Format("01")
	filename := fmt.Sprintf("%s_%s_page%d.json", 
		now.Format("2006-01-02_15-04-05"), 
		country, 
		page)
	
	fullOutputDir := filepath.Join(baseOutputDir, yearDir, monthDir)
	fullJSONPath := filepath.Join(fullOutputDir, filename)

	return fullOutputDir, fullJSONPath
}

// GenerateJSONFilePathWithTime creates a file path with a specific time (useful for batch processing)
func (g *FilePathGenerator) GenerateJSONFilePathWithTime(baseOutputDir, country string, page int, timestamp time.Time) (string, string) {
	yearDir := timestamp.Format("2006")
	monthDir := timestamp.Format("01")
	filename := fmt.Sprintf("%s_%s_page%d.json", 
		timestamp.Format("2006-01-02_15-04-05"), 
		country, 
		page)
	
	fullOutputDir := filepath.Join(baseOutputDir, yearDir, monthDir)
	fullJSONPath := filepath.Join(fullOutputDir, filename)

	return fullOutputDir, fullJSONPath
}

// ValidateFilePath checks if a file path is valid and safe
func ValidateFilePath(filePath string) error {
	if filePath == "" {
		return fmt.Errorf("file path cannot be empty")
	}

	// Basic validation - you might want to add more security checks
	if filepath.IsAbs(filePath) {
		// Absolute paths are generally OK, but you might want to restrict them
	}

	// Check for path traversal attempts
	cleaned := filepath.Clean(filePath)
	if cleaned != filePath {
		return fmt.Errorf("file path contains invalid characters or path traversal attempts")
	}

	return nil
}

// EnsureDirectoryExists creates a directory if it doesn't exist
func EnsureDirectoryExists(dirPath string) error {
	if err := ValidateFilePath(dirPath); err != nil {
		return fmt.Errorf("invalid directory path: %w", err)
	}

	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist, create it
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory '%s': %w", dirPath, err)
			}
			return nil
		}
		return fmt.Errorf("failed to check directory '%s': %w", dirPath, err)
	}

	if !info.IsDir() {
		return fmt.Errorf("path '%s' exists but is not a directory", dirPath)
	}

	return nil
}

// GetFileSize returns the size of a file in bytes
func GetFileSize(filePath string) (int64, error) {
	if err := ValidateFilePath(filePath); err != nil {
		return 0, err
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for '%s': %w", filePath, err)
	}

	return info.Size(), nil
}

// FileExists checks if a file exists
func FileExists(filePath string) bool {
	if err := ValidateFilePath(filePath); err != nil {
		return false
	}

	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// Global variables for backward compatibility
var (
	defaultGenerator = NewDefaultFilePathGenerator()
	timeNow         = time.Now // Keep for backward compatibility
)

// GenerateJSONFilePath is the legacy function for backward compatibility
func GenerateJSONFilePath(baseOutputDir, country string, page int) (string, string) {
	return defaultGenerator.GenerateJSONFilePath(baseOutputDir, country, page)
}

// SetTimeProvider allows changing the time provider for the default generator (useful for testing)
func SetTimeProvider(provider TimeProvider) {
	defaultGenerator = NewFilePathGenerator(provider)
}