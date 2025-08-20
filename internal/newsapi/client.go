package newsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-news-agg/internal/config"
)

// HTTPClient defines the interface for making HTTP requests.
// This interface is used for dependency injection and testing.
type HTTPClient interface {
	Get(url string) (*http.Response, error)
	GetWithContext(ctx context.Context, url string) (*http.Response, error)
}

// defaultHTTPClient is a wrapper around the standard *http.Client
// that implements our HTTPClient interface by adding a GetWithContext method.
type defaultHTTPClient struct {
	client *http.Client
}

// Get implements the HTTPClient interface.
func (c *defaultHTTPClient) Get(url string) (*http.Response, error) {
	// Call GetWithContext with a background context for simplicity.
	return c.GetWithContext(context.Background(), url)
}

// GetWithContext implements the HTTPClient interface by using
// the standard http.Client.Do method, which supports context.
func (c *defaultHTTPClient) GetWithContext(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.client.Do(req)
}

// RateLimiter manages API rate limiting.
type RateLimiter struct {
	remaining int
	resetTime time.Time
	limit     int
	mutex     sync.RWMutex
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		remaining: 1000, // Default conservative value
		resetTime: time.Now().Add(time.Hour),
		limit:     1000,
	}
}

// UpdateFromHeaders updates the rate limiter from HTTP response headers.
func (r *RateLimiter) UpdateFromHeaders(headers http.Header) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if limitStr := headers.Get("X-RateLimit-Limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			r.limit = limit
		}
	}

	if remainingStr := headers.Get("X-RateLimit-Remaining"); remainingStr != "" {
		if remaining, err := strconv.Atoi(remainingStr); err == nil {
			r.remaining = remaining
		}
	}

	if resetStr := headers.Get("X-RateLimit-Reset"); resetStr != "" {
		if resetUnix, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
			r.resetTime = time.Unix(resetUnix, 0)
		}
	}
}

// WaitIfNeeded waits if we're close to hitting rate limits.
func (r *RateLimiter) WaitIfNeeded(ctx context.Context) error {
	r.mutex.RLock()
	remaining := r.remaining
	resetTime := r.resetTime
	r.mutex.RUnlock()

	// If we have plenty of requests remaining, don't wait.
	if remaining > 10 {
		return nil
	}

	// If we're low on requests, wait until reset.
	if remaining <= 5 && time.Now().Before(resetTime) {
		waitDuration := time.Until(resetTime) + time.Second
		select {
		case <-time.After(waitDuration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// GetStatus returns the current rate limit status.
func (r *RateLimiter) GetStatus() (remaining, limit int, resetTime time.Time) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.remaining, r.limit, r.resetTime
}

// NewsAPIClient wraps HTTP client with NewsAPI-specific functionality.
type NewsAPIClient struct {
	httpClient  HTTPClient
	rateLimiter *RateLimiter
	config      *config.Config
	baseURL     string
	timeout     time.Duration
}

// NewNewsAPIClient creates a new NewsAPI client.
// This function now uses the defaultHTTPClient wrapper to satisfy the HTTPClient interface.
func NewNewsAPIClient(cfg *config.Config) *NewsAPIClient {
	// Create a standard HTTP client.
	client := &http.Client{
		Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
	}
	// Wrap it to make it conform to our HTTPClient interface.
	httpClient := &defaultHTTPClient{client: client}

	return &NewsAPIClient{
		httpClient:  httpClient,
		rateLimiter: NewRateLimiter(),
		config:      cfg,
		baseURL:     cfg.BaseURL,
		timeout:     time.Duration(cfg.TimeoutSeconds) * time.Second,
	}
}

// NewNewsAPIClientWithHTTPClient creates a client with a custom HTTP client (useful for testing).
func NewNewsAPIClientWithHTTPClient(cfg *config.Config, httpClient HTTPClient) *NewsAPIClient {
	return &NewsAPIClient{
		httpClient:  httpClient,
		rateLimiter: NewRateLimiter(),
		config:      cfg,
		baseURL:     cfg.BaseURL,
		timeout:     time.Duration(cfg.TimeoutSeconds) * time.Second,
	}
}

// FetchNewsPage fetches a single page of news from the API.
func (c *NewsAPIClient) FetchNewsPage(ctx context.Context, req *DownloadRequest, page int) (*NewsAPIResponse, *NewsAPILimits, error) {
	// Wait for rate limiting if needed.
	if err := c.rateLimiter.WaitIfNeeded(ctx); err != nil {
		return nil, nil, fmt.Errorf("rate limit wait cancelled: %w", err)
	}

	// Build the URL.
	fullURL, err := c.buildURL(req, page)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build URL: %w", err)
	}

	// Make the HTTP request.
	resp, err := c.httpClient.GetWithContext(ctx, fullURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to make HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Update rate limiter from response headers.
	c.rateLimiter.UpdateFromHeaders(resp.Header)

	// Get current rate limits for return.
	limits := c.extractRateLimits(resp.Header)

	// Handle rate limiting.
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := time.Duration(c.config.DefaultRateLimitDelaySeconds) * time.Second
		if time.Now().Before(limits.Reset) {
			retryAfter = time.Until(limits.Reset) + time.Second
		}

		return nil, &limits, &RateLimitError{
			RetryAfter:     retryAfter,
			ResetTime:      limits.Reset,
			RemainingCalls: limits.Remaining,
			Message:        fmt.Sprintf("rate limit exceeded, retry after %v", retryAfter),
		}
	}

	// Read response body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &limits, fmt.Errorf("failed to read response body: %w", err)
	}

	// Handle non-OK status codes.
	if resp.StatusCode != http.StatusOK {
		return c.handleErrorResponse(resp.StatusCode, body, fullURL)
	}

	// Parse the response.
	var newsResp NewsAPIResponse
	if err := json.Unmarshal(body, &newsResp); err != nil {
		return nil, &limits, fmt.Errorf("failed to unmarshal JSON response: %w", err)
	}

	// Check for API-level errors.
	if newsResp.IsError() {
		apiErr := newsResp.ToError(resp.StatusCode)
		apiErr.URL = fullURL
		return nil, &limits, apiErr
	}

	return &newsResp, &limits, nil
}

// buildURL constructs the full URL for the API request.
func (c *NewsAPIClient) buildURL(req *DownloadRequest, page int) (string, error) {
	params := url.Values{}

	if req.Query != "" {
		params.Add("q", req.Query)
	}

	if req.Country != "" {
		params.Add("country", req.Country)
	}

	if req.Language != "" {
		params.Add("language", req.Language)
	}

	if req.SortBy != "" {
		params.Add("sortBy", req.SortBy)
	}

	params.Add("pageSize", strconv.Itoa(req.PageSize))
	params.Add("page", strconv.Itoa(page))
	params.Add("apiKey", req.APIKey)

	if !req.From.IsZero() {
		params.Add("from", req.From.Format("2006-01-02T15:04:05Z"))
	}

	if !req.To.IsZero() {
		params.Add("to", req.To.Format("2006-01-02T15:04:05Z"))
	}

	fullURL := c.baseURL + "?" + params.Encode()
	return fullURL, nil
}

// extractRateLimits extracts rate limit information from response headers.
func (c *NewsAPIClient) extractRateLimits(headers http.Header) NewsAPILimits {
	var limits NewsAPILimits

	if limitStr := headers.Get("X-RateLimit-Limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			limits.Limit = limit
		}
	}

	if remainingStr := headers.Get("X-RateLimit-Remaining"); remainingStr != "" {
		if remaining, err := strconv.Atoi(remainingStr); err == nil {
			limits.Remaining = remaining
		}
	}

	if resetStr := headers.Get("X-RateLimit-Reset"); resetStr != "" {
		if resetUnix, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
			limits.Reset = time.Unix(resetUnix, 0)
		}
	}

	return limits
}

// handleErrorResponse handles non-200 HTTP responses.
func (c *NewsAPIClient) handleErrorResponse(statusCode int, body []byte, url string) (*NewsAPIResponse, *NewsAPILimits, error) {
	var apiErrorResp NewsAPIResponse
	if err := json.Unmarshal(body, &apiErrorResp); err != nil {
		// If we can't parse the error response, return a generic error.
		return nil, nil, &NewsAPIError{
			StatusCode: statusCode,
			Message:    fmt.Sprintf("HTTP %d: %s", statusCode, string(body)),
			URL:        url,
		}
	}

	// Return the parsed error.
	apiErr := apiErrorResp.ToError(statusCode)
	if apiErr != nil {
		apiErr.URL = url
		return nil, nil, apiErr
	}

	// Fallback error.
	return nil, nil, &NewsAPIError{
		StatusCode: statusCode,
		Message:    fmt.Sprintf("HTTP %d", statusCode),
		URL:        url,
	}
}

// GetRateLimitStatus returns the current rate limit status.
func (c *NewsAPIClient) GetRateLimitStatus() (remaining, limit int, resetTime time.Time) {
	return c.rateLimiter.GetStatus()
}

// MockHTTPClient implements HTTPClient for testing.
type MockHTTPClient struct {
	responses map[string]*http.Response
	errors    map[string]error
	callCount map[string]int
	mutex     sync.RWMutex
}

// NewMockHTTPClient creates a new mock HTTP client.
func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		responses: make(map[string]*http.Response),
		errors:    make(map[string]error),
		callCount: make(map[string]int),
	}
}

// SetResponse sets a mock response for a given URL pattern.
func (m *MockHTTPClient) SetResponse(urlPattern string, response *http.Response) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.responses[urlPattern] = response
}

// SetError sets a mock error for a given URL pattern.
func (m *MockHTTPClient) SetError(urlPattern string, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.errors[urlPattern] = err
}

// Get implements HTTPClient.Get.
func (m *MockHTTPClient) Get(url string) (*http.Response, error) {
	return m.GetWithContext(context.Background(), url)
}

// GetWithContext implements HTTPClient.GetWithContext.
func (m *MockHTTPClient) GetWithContext(ctx context.Context, url string) (*http.Response, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Increment call count.
	m.callCount[url]++

	// Check for errors first.
	for pattern, err := range m.errors {
		if pattern == url || pattern == "*" {
			return nil, err
		}
	}

	// Check for responses.
	for pattern, resp := range m.responses {
		if pattern == url || pattern == "*" {
			return resp, nil
		}
	}

	// Default response.
	return &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       ioutil.NopCloser(strings.NewReader("Not Found")),
		Header:     make(http.Header),
	}, nil
}

// GetCallCount returns the number of times a URL was called.
func (m *MockHTTPClient) GetCallCount(url string) int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.callCount[url]
}

// Reset clears all mock data.
func (m *MockHTTPClient) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.responses = make(map[string]*http.Response)
	m.errors = make(map[string]error)
	m.callCount = make(map[string]int)
}
