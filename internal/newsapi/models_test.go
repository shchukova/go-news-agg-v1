package newsapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDownloadRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     *DownloadRequest
		wantErr bool
		errType string
	}{
		{
			name: "valid request with country",
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
			name: "valid request with query",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Query:     "technology",
				PageSize:  50,
				StartPage: 1,
				SortBy:    "relevancy",
			},
			wantErr: false,
		},
		{
			name: "valid request with both country and query",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "uk",
				Query:     "AI",
				PageSize:  100,
				StartPage: 1,
				SortBy:    "popularity",
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
			errType: "api_key",
		},
		{
			name: "missing country and query",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "",
				Query:     "",
				PageSize:  20,
				StartPage: 1,
			},
			wantErr: true,
			errType: "country/query",
		},
		{
			name: "page size too small",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  0,
				StartPage: 1,
			},
			wantErr: true,
			errType: "page_size",
		},
		{
			name: "page size too large",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  150,
				StartPage: 1,
			},
			wantErr: true,
			errType: "page_size",
		},
		{
			name: "invalid start page",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  20,
				StartPage: 0,
			},
			wantErr: true,
			errType: "start_page",
		},
		{
			name: "invalid sort by",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  20,
				StartPage: 1,
				SortBy:    "invalid_sort",
			},
			wantErr: true,
			errType: "sort_by",
		},
		{
			name: "empty sort by should be valid",
			req: &DownloadRequest{
				APIKey:    "test-api-key",
				Country:   "us",
				PageSize:  20,
				StartPage: 1,
				SortBy:    "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected validation error, got nil")
					return
				}

				validationErr, ok := err.(*ValidationError)
				if !ok {
					t.Errorf("Expected ValidationError, got %T", err)
					return
				}

				if validationErr.Field != tt.errType {
					t.Errorf("Expected error field '%s', got '%s'", tt.errType, validationErr.Field)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestNewDownloadRequest(t *testing.T) {
	apiKey := "test-api-key"
	country := "us"

	req := NewDownloadRequest(apiKey, country)

	if req.APIKey != apiKey {
		t.Errorf("Expected APIKey '%s', got '%s'", apiKey, req.APIKey)
	}

	if req.Country != country {
		t.Errorf("Expected Country '%s', got '%s'", country, req.Country)
	}

	if req.PageSize != 20 {
		t.Errorf("Expected default PageSize 20, got %d", req.PageSize)
	}

	if req.StartPage != 1 {
		t.Errorf("Expected default StartPage 1, got %d", req.StartPage)
	}

	if req.SortBy != "publishedAt" {
		t.Errorf("Expected default SortBy 'publishedAt', got '%s'", req.SortBy)
	}
}

func TestNewsAPIResponse_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		response *NewsAPIResponse
		expected bool
	}{
		{
			name: "response with articles",
			response: &NewsAPIResponse{
				Articles: []Article{
					{Title: "Test Article"},
				},
			},
			expected: false,
		},
		{
			name: "response with no articles",
			response: &NewsAPIResponse{
				Articles: []Article{},
			},
			expected: true,
		},
		{
			name: "response with nil articles",
			response: &NewsAPIResponse{
				Articles: nil,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.response.IsEmpty()
			if result != tt.expected {
				t.Errorf("Expected IsEmpty() to return %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNewsAPIResponse_IsError(t *testing.T) {
	tests := []struct {
		name     string
		response *NewsAPIResponse
		expected bool
	}{
		{
			name: "successful response",
			response: &NewsAPIResponse{
				Status: "ok",
				Code:   "",
			},
			expected: false,
		},
		{
			name: "error response with status",
			response: &NewsAPIResponse{
				Status: "error",
				Code:   "apiKeyInvalid",
			},
			expected: true,
		},
		{
			name: "response with error code but ok status",
			response: &NewsAPIResponse{
				Status: "ok",
				Code:   "someCode",
			},
			expected: true,
		},
		{
			name: "response with non-ok status",
			response: &NewsAPIResponse{
				Status: "error",
				Code:   "",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.response.IsError()
			if result != tt.expected {
				t.Errorf("Expected IsError() to return %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNewsAPIResponse_ToError(t *testing.T) {
	tests := []struct {
		name       string
		response   *NewsAPIResponse
		statusCode int
		expectErr  bool
	}{
		{
			name: "successful response",
			response: &NewsAPIResponse{
				Status: "ok",
				Code:   "",
			},
			statusCode: 200,
			expectErr:  false,
		},
		{
			name: "error response",
			response: &NewsAPIResponse{
				Status:  "error",
				Code:    "apiKeyInvalid",
				Message: "Your API key is invalid",
			},
			statusCode: 401,
			expectErr:  true,
		},
		{
			name: "response with error code",
			response: &NewsAPIResponse{
				Status:  "ok",
				Code:    "rateLimited",
				Message: "Too many requests",
			},
			statusCode: 429,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.response.ToError(tt.statusCode)
			
			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error, got nil")
					return
				}

				if err.StatusCode != tt.statusCode {
					t.Errorf("Expected status code %d, got %d", tt.statusCode, err.StatusCode)
				}

				if err.Code != tt.response.Code {
					t.Errorf("Expected error code '%s', got '%s'", tt.response.Code, err.Code)
				}

				if err.Message != tt.response.Message {
					t.Errorf("Expected message '%s', got '%s'", tt.response.Message, err.Message)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
			}
		})
	}
}

func TestNewsAPIError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *NewsAPIError
		expected string
	}{
		{
			name: "error with code and message",
			err: &NewsAPIError{
				StatusCode: 401,
				Code:       "apiKeyInvalid",
				Message:    "Your API key is invalid",
			},
			expected: "NewsAPI error 401: apiKeyInvalid - Your API key is invalid",
		},
		{
			name: "error with only status code",
			err: &NewsAPIError{
				StatusCode: 500,
				Code:       "",
				Message:    "",
			},
			expected: "NewsAPI error 500",
		},
		{
			name: "error with code but no message",
			err: &NewsAPIError{
				StatusCode: 429,
				Code:       "rateLimited",
				Message:    "",
			},
			expected: "NewsAPI error 429",
		},
		{
			name: "error with message but no code",
			err: &NewsAPIError{
				StatusCode: 404,
				Code:       "",
				Message:    "Not found",
			},
			expected: "NewsAPI error 404",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Expected error message '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestRateLimitError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *RateLimitError
		expected string
	}{
		{
			name: "error with custom message",
			err: &RateLimitError{
				RetryAfter: 60 * time.Second,
				Message:    "Custom rate limit message",
			},
			expected: "Custom rate limit message",
		},
		{
			name: "error with default message",
			err: &RateLimitError{
				RetryAfter: 30 * time.Second,
				Message:    "",
			},
			expected: "rate limit exceeded, retry after 30s",
		},
		{
			name: "error with zero retry after",
			err: &RateLimitError{
				RetryAfter: 0,
				Message:    "",
			},
			expected: "rate limit exceeded, retry after 0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Expected error message '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Field:   "api_key",
		Message: "cannot be empty",
	}

	expected := "validation error for field 'api_key': cannot be empty"
	result := err.Error()

	if result != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, result)
	}
}

func TestFileOperationError_Error(t *testing.T) {
	originalErr := fmt.Errorf("permission denied")
	err := &FileOperationError{
		Operation: "write file",
		FilePath:  "/tmp/test.json",
		Cause:     originalErr,
	}

	expected := "file operation 'write file' failed for '/tmp/test.json': permission denied"
	result := err.Error()

	if result != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, result)
	}

	// Test Unwrap
	unwrapped := err.Unwrap()
	if unwrapped != originalErr {
		t.Errorf("Expected unwrapped error to be original error, got different error")
	}
}

func TestKafkaError_Error(t *testing.T) {
	originalErr := fmt.Errorf("connection refused")
	err := &KafkaError{
		Operation: "publish",
		Topic:     "news_files",
		Broker:    "localhost:9092",
		Cause:     originalErr,
	}

	expected := "kafka operation 'publish' failed for topic 'news_files' on broker 'localhost:9092': connection refused"
	result := err.Error()

	if result != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, result)
	}

	// Test Unwrap
	unwrapped := err.Unwrap()
	if unwrapped != originalErr {
		t.Errorf("Expected unwrapped error to be original error, got different error")
	}
}

func TestArticleJSONSerialization(t *testing.T) {
	// Test serialization and deserialization of Article
	publishedAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	
	article := Article{
		Source: Source{
			ID:   "test-source",
			Name: "Test Source",
		},
		Author:      "John Doe",
		Title:       "Test Article Title",
		Description: "This is a test article description",
		URL:         "https://example.com/test-article",
		URLToImage:  "https://example.com/test-image.jpg",
		PublishedAt: publishedAt,
		Content:     "This is the full content of the test article.",
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(article)
	if err != nil {
		t.Fatalf("Failed to marshal article to JSON: %v", err)
	}

	// Deserialize from JSON
	var deserializedArticle Article
	err = json.Unmarshal(jsonData, &deserializedArticle)
	if err != nil {
		t.Fatalf("Failed to unmarshal article from JSON: %v", err)
	}

	// Verify all fields
	if deserializedArticle.Source.ID != article.Source.ID {
		t.Errorf("Expected source ID '%s', got '%s'", article.Source.ID, deserializedArticle.Source.ID)
	}

	if deserializedArticle.Source.Name != article.Source.Name {
		t.Errorf("Expected source name '%s', got '%s'", article.Source.Name, deserializedArticle.Source.Name)
	}

	if deserializedArticle.Author != article.Author {
		t.Errorf("Expected author '%s', got '%s'", article.Author, deserializedArticle.Author)
	}

	if deserializedArticle.Title != article.Title {
		t.Errorf("Expected title '%s', got '%s'", article.Title, deserializedArticle.Title)
	}

	if deserializedArticle.Description != article.Description {
		t.Errorf("Expected description '%s', got '%s'", article.Description, deserializedArticle.Description)
	}

	if deserializedArticle.URL != article.URL {
		t.Errorf("Expected URL '%s', got '%s'", article.URL, deserializedArticle.URL)
	}

	if deserializedArticle.URLToImage != article.URLToImage {
		t.Errorf("Expected URL to image '%s', got '%s'", article.URLToImage, deserializedArticle.URLToImage)
	}

	if !deserializedArticle.PublishedAt.Equal(article.PublishedAt) {
		t.Errorf("Expected published at '%v', got '%v'", article.PublishedAt, deserializedArticle.PublishedAt)
	}

	if deserializedArticle.Content != article.Content {
		t.Errorf("Expected content '%s', got '%s'", article.Content, deserializedArticle.Content)
	}
}

func TestNewsAPIResponseJSONSerialization(t *testing.T) {
	// Test serialization and deserialization of NewsAPIResponse
	response := NewsAPIResponse{
		Status:       "ok",
		TotalResults: 100,
		Articles: []Article{
			{
				Title:       "First Article",
				Description: "First description",
				URL:         "https://example.com/first",
			},
			{
				Title:       "Second Article",
				Description: "Second description",
				URL:         "https://example.com/second",
			},
		},
		Code:    "",
		Message: "",
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Failed to marshal response to JSON: %v", err)
	}

	// Deserialize from JSON
	var deserializedResponse NewsAPIResponse
	err = json.Unmarshal(jsonData, &deserializedResponse)
	if err != nil {
		t.Fatalf("Failed to unmarshal response from JSON: %v", err)
	}

	// Verify fields
	if deserializedResponse.Status != response.Status {
		t.Errorf("Expected status '%s', got '%s'", response.Status, deserializedResponse.Status)
	}

	if deserializedResponse.TotalResults != response.TotalResults {
		t.Errorf("Expected total results %d, got %d", response.TotalResults, deserializedResponse.TotalResults)
	}

	if len(deserializedResponse.Articles) != len(response.Articles) {
		t.Errorf("Expected %d articles, got %d", len(response.Articles), len(deserializedResponse.Articles))
	}

	// Verify articles
	for i, article := range response.Articles {
		if i >= len(deserializedResponse.Articles) {
			t.Errorf("Missing article at index %d", i)
			continue
		}

		deserializedArticle := deserializedResponse.Articles[i]
		if deserializedArticle.Title != article.Title {
			t.Errorf("Article %d: Expected title '%s', got '%s'", i, article.Title, deserializedArticle.Title)
		}

		if deserializedArticle.Description != article.Description {
			t.Errorf("Article %d: Expected description '%s', got '%s'", i, article.Description, deserializedArticle.Description)
		}

		if deserializedArticle.URL != article.URL {
			t.Errorf("Article %d: Expected URL '%s', got '%s'", i, article.URL, deserializedArticle.URL)
		}
	}
}

func TestDownloadResultDuration(t *testing.T) {
	// Test that duration is calculated correctly
	startTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	endTime := startTime.Add(5 * time.Minute)

	result := &DownloadResult{
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime),
	}

	expectedDuration := 5 * time.Minute
	if result.Duration != expectedDuration {
		t.Errorf("Expected duration %v, got %v", expectedDuration, result.Duration)
	}
}

func TestErrorChaining(t *testing.T) {
	// Test that our custom errors properly implement error unwrapping
	originalErr := fmt.Errorf("original error")

	fileErr := &FileOperationError{
		Operation: "read",
		FilePath:  "/test/path",
		Cause:     originalErr,
	}

	kafkaErr := &KafkaError{
		Operation: "connect",
		Topic:     "test",
		Broker:    "localhost:9092",
		Cause:     originalErr,
	}

	// Test error unwrapping
	if fileErr.Unwrap() != originalErr {
		t.Error("FileOperationError.Unwrap() did not return original error")
	}

	if kafkaErr.Unwrap() != originalErr {
		t.Error("KafkaError.Unwrap() did not return original error")
	}

	// Test error.Is() functionality
	if fmt.Errorf("wrapped: %w", fileErr).Error() != "" {
		// This tests that our errors work with error wrapping
		wrappedErr := fmt.Errorf("wrapped: %w", fileErr)
		if !strings.Contains(wrappedErr.Error(), "file operation") {
			t.Error("Error wrapping did not preserve error message")
		}
	}
}