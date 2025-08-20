package newsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"go-news-agg/internal/config"
)

// Helper function to create a new, well-formed NewsAPIResponse
func createMockNewsAPIResponse() *NewsAPIResponse {
	// Parse the time strings into time.Time objects to fix the error.
	publishedAt1, _ := time.Parse(time.RFC3339, "2023-10-27T10:00:00Z")
	publishedAt2, _ := time.Parse(time.RFC3339, "2023-10-27T11:00:00Z")

	return &NewsAPIResponse{
		Status:       "ok",
		TotalResults: 2,
		Articles: []Article{
			{
				Source:      Source{ID: "1", Name: "Test Source 1"},
				Author:      "Jane Doe",
				Title:       "Test Article 1",
				Description: "A test description for the first article.",
				URL:         "http://example.com/article1",
				PublishedAt: publishedAt1, // Use the parsed time.Time object
			},
			{
				Source:      Source{ID: "2", Name: "Test Source 2"},
				Author:      "John Smith",
				Title:       "Test Article 2",
				Description: "A test description for the second article.",
				URL:         "http://example.com/article2",
				PublishedAt: publishedAt2, // Use the parsed time.Time object
			},
		},
	}
}

// TestNewNewsAPIClient tests the client's initialization.
func TestNewNewsAPIClient(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.TimeoutSeconds = 45

	client := NewNewsAPIClient(cfg)

	if client == nil {
		t.Fatal("NewNewsAPIClient returned nil")
	}

	if client.config != cfg {
		t.Error("Client config not set correctly")
	}

	if client.baseURL != cfg.BaseURL {
		t.Errorf("Expected baseURL '%s', got '%s'", cfg.BaseURL, client.baseURL)
	}

	if client.timeout != time.Duration(cfg.TimeoutSeconds)*time.Second {
		t.Errorf("Expected timeout %v, got %v", time.Duration(cfg.TimeoutSeconds)*time.Second, client.timeout)
	}
}

// TestFetchNewsPage_Success tests a successful API call.
func TestFetchNewsPage_Success(t *testing.T) {
	// Create a mock client and set a successful response.
	mockClient := NewMockHTTPClient()
	mockResponse := createMockNewsAPIResponse()
	responseBody, _ := json.Marshal(mockResponse)
	mockClient.SetResponse("*", &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(responseBody)),
		Header:     http.Header{"X-Ratelimit-Remaining": []string{"999"}},
	})

	// Create a NewsAPIClient with the mock client.
	cfg := config.DefaultConfig()
	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// Create a mock download request.
	req := &DownloadRequest{
		Query:    "test",
		PageSize: 10,
		APIKey:   "test-key",
	}

	// Fetch a page.
	resp, limits, err := client.FetchNewsPage(context.Background(), req, 1)

	// Assert the results.
	if err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected a response, but got nil")
	}

	if resp.TotalResults != 2 {
		t.Errorf("Expected 2 results, but got %d", resp.TotalResults)
	}

	if limits.Remaining != 999 {
		t.Errorf("Expected remaining calls to be 999, but got %d", limits.Remaining)
	}
}

// TestFetchNewsPage_RateLimit tests handling a 429 Too Many Requests response.
func TestFetchNewsPage_RateLimit(t *testing.T) {
	// Create a mock client and set a rate limit response.
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("*", &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       ioutil.NopCloser(strings.NewReader("")),
		Header:     http.Header{"X-Ratelimit-Remaining": []string{"0"}, "X-Ratelimit-Reset": []string{"1698422400"}},
	})

	// Create a NewsAPIClient with the mock client.
	cfg := config.DefaultConfig()
	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// Create a mock download request.
	req := &DownloadRequest{
		Query:    "test",
		PageSize: 10,
		APIKey:   "test-key",
	}

	// Fetch a page.
	resp, limits, err := client.FetchNewsPage(context.Background(), req, 1)

	// Assert the results.
	if resp != nil {
		t.Errorf("Expected nil response, but got %v", resp)
	}

	if err == nil {
		t.Fatal("Expected a rate limit error, but got nil")
	}

	rateLimitErr, ok := err.(*RateLimitError)
	if !ok {
		t.Fatalf("Expected error of type *RateLimitError, but got %T", err)
	}

	if rateLimitErr.RemainingCalls != 0 {
		t.Errorf("Expected remaining calls to be 0, but got %d", rateLimitErr.RemainingCalls)
	}

	if limits.Remaining != 0 {
		t.Errorf("Expected limits.Remaining to be 0, but got %d", limits.Remaining)
	}
}

// TestFetchNewsPage_APIError tests handling an API-level error (e.g., bad API key).
func TestFetchNewsPage_APIError(t *testing.T) {
	// Create a mock client and set a an API error response.
	mockClient := NewMockHTTPClient()
	errorBody := `{"status": "error", "code": "apiKeyInvalid", "message": "Your API key is invalid or incorrect."}`
	mockClient.SetResponse("*", &http.Response{
		StatusCode: http.StatusUnauthorized,
		Body:       ioutil.NopCloser(strings.NewReader(errorBody)),
	})

	// Create a NewsAPIClient with the mock client.
	cfg := config.DefaultConfig()
	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// Create a mock download request.
	req := &DownloadRequest{
		Query:    "test",
		PageSize: 10,
		APIKey:   "bad-key",
	}

	// Fetch a page.
	_, _, err := client.FetchNewsPage(context.Background(), req, 1)

	// Assert the results.
	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	apiErr, ok := err.(*NewsAPIError)
	if !ok {
		t.Fatalf("Expected error of type *NewsAPIError, but got %T", err)
	}

	if apiErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status code %d, but got %d", http.StatusUnauthorized, apiErr.StatusCode)
	}

	expectedMsg := "Your API key is invalid or incorrect."
	if apiErr.Message != expectedMsg {
		t.Errorf("Expected message '%s', but got '%s'", expectedMsg, apiErr.Message)
	}
}

// TestFetchNewsPage_HTTPError tests handling a general HTTP client error.
func TestFetchNewsPage_HTTPError(t *testing.T) {
	// Create a mock client and set a general HTTP error.
	mockClient := NewMockHTTPClient()
	mockClient.SetError("*", fmt.Errorf("connection refused"))

	// Create a NewsAPIClient with the mock client.
	cfg := config.DefaultConfig()
	client := NewNewsAPIClientWithHTTPClient(cfg, mockClient)

	// Create a mock download request.
	req := &DownloadRequest{
		Query:    "test",
		PageSize: 10,
		APIKey:   "test-key",
	}

	// Fetch a page.
	_, _, err := client.FetchNewsPage(context.Background(), req, 1)

	// Assert the results.
	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	expectedErrorMsg := "failed to make HTTP request: connection refused"
	if err.Error() != expectedErrorMsg {
		t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
	}
}
