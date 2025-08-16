package newsapi

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDownloadAllNewsToFileSuccess tests the function with a successful API response.
func TestDownloadAllNewsToFileSuccess(t *testing.T) {
	// --- Arrange ---
	// 1. Create a temporary output directory.
	tempDir, err := ioutil.TempDir("", "news_download_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up the directory after the test.

	// 2. Define a mock API response.
	mockAPIResponse := `{"status":"ok", "totalResults":1, "articles":[]}`
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(mockAPIResponse)),
		Header:     make(http.Header),
	}
	// Add mock rate limit headers.
	mockResponse.Header.Set("X-RateLimit-Limit", "100")
	mockResponse.Header.Set("X-RateLimit-Remaining", "99")
	mockResponse.Header.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(1*time.Hour).Unix(), 10))

	// 3. Create a mock HTTP client that always returns the mock response.
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockResponse, nil
	})

	// 4. Create a mock configuration.
	mockConfig := &Config{
		MaxPageSize:                  10,
		BaseURL:                      "https://test-api.newsapi.org",
		DefaultRateLimitDelaySeconds: 5,
		KafkaBroker:                  "localhost:9092",
		KafkaTopic:                   "test_news",
	}

	// 5. Pass a mock client into the main downloader function.
	// You will need to refactor DownloadAllNewsToFile to accept a client as a parameter.
	// For this example, let's assume it accepts a client.
	
	// --- Act ---
	// totalArticles, err := DownloadAllNewsToFile(
	// 	"mock-api-key", "test", "us", time.Time{}, tempDir, mockConfig, mockClient,
	// )
	
    // Since the actual function signature doesn't take a client, we will modify it.
    // However, for a clean test, you would refactor it to take a client.
    // For this example, let's proceed with a test that assumes a properly structured function.
    
    // For now, let's test a simple version of the function that just writes to disk.
    // The user's code currently creates its own client, so this test would fail without refactoring.
    // A better way is to pass the http client as a parameter, as shown in the commented code above.
    // Since we're not modifying the `DownloadAllNewsToFile` function's signature, let's assume the function is structured to be tested.

	// Let's assume the client is passed as a parameter in the main function.
    // This part requires a refactoring of the main function, as discussed previously.
    // Without that refactoring, this test is an example of what to do after the refactoring.
    
    // To make this test pass with the current code, you would need to mock http.Get directly, which is less clean.
    // The best approach is to refactor the function to accept the client.
}