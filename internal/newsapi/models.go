package newsapi

import (
	"fmt"
	"time"
)

// NewsAPIResponse represents the top-level structure of the News API response
type NewsAPIResponse struct {
	Status       string    `json:"status"`
	TotalResults int       `json:"totalResults"`
	Articles     []Article `json:"articles"`
	Code         string    `json:"code"`
	Message      string    `json:"message"`
}

// Article represents a single news article from the API
type Article struct {
	Source      Source    `json:"source"`
	Author      string    `json:"author"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	URL         string    `json:"url"`
	URLToImage  string    `json:"urlToImage"`
	PublishedAt time.Time `json:"publishedAt"`
	Content     string    `json:"content"`
}

// Source represents the source of a news article
type Source struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// NewsAPILimits holds the current rate limit information from NewsAPI response headers
type NewsAPILimits struct {
	Limit     int       `json:"limit"`
	Remaining int       `json:"remaining"`
	Reset     time.Time `json:"reset"`
}

// DownloadRequest represents a request to download news articles
type DownloadRequest struct {
	APIKey    string    `json:"api_key"`
	Query     string    `json:"query"`
	Country   string    `json:"country"`
	From      time.Time `json:"from"`
	To        time.Time `json:"to"`
	Language  string    `json:"language"`
	SortBy    string    `json:"sort_by"`
	PageSize  int       `json:"page_size"`
	StartPage int       `json:"start_page"`
}

// DownloadResult represents the result of a download operation
type DownloadResult struct {
	TotalArticles int           `json:"total_articles"`
	PagesDownloaded int         `json:"pages_downloaded"`
	FilePaths     []string      `json:"file_paths"`
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time"`
	Duration      time.Duration `json:"duration"`
	Errors        []error       `json:"errors,omitempty"`
}

// NewsAPIError represents an error response from the News API
type NewsAPIError struct {
	StatusCode int    `json:"status_code"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	URL        string `json:"url,omitempty"`
}

func (e *NewsAPIError) Error() string {
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("NewsAPI error %d: %s - %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("NewsAPI error %d", e.StatusCode)
}

// RateLimitError represents a rate limiting error
type RateLimitError struct {
	RetryAfter    time.Duration `json:"retry_after"`
	ResetTime     time.Time     `json:"reset_time"`
	RemainingCalls int          `json:"remaining_calls"`
	Message       string        `json:"message"`
}

func (e *RateLimitError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("rate limit exceeded, retry after %v", e.RetryAfter)
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s", e.Field, e.Message)
}

// FileOperationError represents an error during file operations
type FileOperationError struct {
	Operation string `json:"operation"`
	FilePath  string `json:"file_path"`
	Cause     error  `json:"cause"`
}

func (e *FileOperationError) Error() string {
	return fmt.Sprintf("file operation '%s' failed for '%s': %v", e.Operation, e.FilePath, e.Cause)
}

func (e *FileOperationError) Unwrap() error {
	return e.Cause
}

// KafkaError represents an error when publishing to Kafka
type KafkaError struct {
	Operation string `json:"operation"`
	Topic     string `json:"topic"`
	Broker    string `json:"broker"`
	Cause     error  `json:"cause"`
}

func (e *KafkaError) Error() string {
	return fmt.Sprintf("kafka operation '%s' failed for topic '%s' on broker '%s': %v", 
		e.Operation, e.Topic, e.Broker, e.Cause)
}

func (e *KafkaError) Unwrap() error {
	return e.Cause
}

// Validate validates a DownloadRequest
func (r *DownloadRequest) Validate() error {
	if r.APIKey == "" {
		return &ValidationError{Field: "api_key", Message: "cannot be empty"}
	}

	if r.Country == "" && r.Query == "" {
		return &ValidationError{Field: "country/query", Message: "either country or query must be specified"}
	}

	if r.PageSize <= 0 || r.PageSize > 100 {
		return &ValidationError{Field: "page_size", Message: "must be between 1 and 100"}
	}

	if r.StartPage < 1 {
		return &ValidationError{Field: "start_page", Message: "must be >= 1"}
	}

	validSortBy := map[string]bool{
		"relevancy":   true,
		"popularity":  true,
		"publishedAt": true,
	}

	if r.SortBy != "" && !validSortBy[r.SortBy] {
		return &ValidationError{Field: "sort_by", Message: "must be one of: relevancy, popularity, publishedAt"}
	}

	return nil
}

// NewDownloadRequest creates a new DownloadRequest with defaults
func NewDownloadRequest(apiKey, country string) *DownloadRequest {
	return &DownloadRequest{
		APIKey:    apiKey,
		Country:   country,
		PageSize:  20,
		StartPage: 1,
		SortBy:    "publishedAt",
	}
}

// IsEmpty checks if the NewsAPIResponse contains any articles
func (r *NewsAPIResponse) IsEmpty() bool {
	return len(r.Articles) == 0
}

// IsError checks if the NewsAPIResponse contains an error
func (r *NewsAPIResponse) IsError() bool {
	return r.Status != "ok" || r.Code != ""
}

// ToError converts a NewsAPIResponse to a NewsAPIError if it represents an error
func (r *NewsAPIResponse) ToError(statusCode int) *NewsAPIError {
	if !r.IsError() {
		return nil
	}

	return &NewsAPIError{
		StatusCode: statusCode,
		Code:       r.Code,
		Message:    r.Message,
	}
}