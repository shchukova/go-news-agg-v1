package newsapi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-news-agg/utils"
	"go-news-agg/kafka_producer"
)

// Config holds all the application's configuration parameters.
type Config struct {
	MaxPageSize                  int    `json:"max_page_size"`
	BaseURL                      string `json:"base_url"`
	DefaultRateLimitDelaySeconds int    `json:"default_rate_limit_delay_seconds"`
	KafkaBroker                  string `json:"kafka_broker"`
	KafkaTopic                   string `json:"kafka_topic"`
}

package newsapi

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "strconv"
    "time"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)

// Config holds all the application's configuration parameters.
type Config struct {
    MaxPageSize                  int    `json:"max_page_size"`
    BaseURL                      string `json:"base_url"`
    DefaultRateLimitDelaySeconds int    `json:"default_rate_limit_delay_seconds"`
    KafkaBroker                  string `json:"kafka_broker"`
    KafkaTopic                   string `json:"kafka_topic"`
}

// NewsAPIResponse represents the top-level structure of the News API response.
type NewsAPIResponse struct {
    Status       string    `json:"status"`
    TotalResults int       `json:"totalResults"`
    Code         string    `json:"code"`
    Message      string    `json:"message"`
}

// NewsAPILimits holds the current rate limit information from NewsAPI response headers.
type NewsAPILimits struct {
    Limit     int
    Remaining int
    Reset     time.Time
}

// timeNow is a variable that holds the current time function.
// This allows it to be mocked in tests.
var timeNow = time.Now

// LoadConfig reads the configuration from a JSON file.
func LoadConfig(filePath string) (*Config, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, fmt.Errorf("failed to open config file: %w", err)
    }
    defer file.Close()

    bytes, err := ioutil.ReadAll(file)
    if err != nil {
        return nil, fmt.Errorf("failed to read config file: %w", err)
    }

    var cfg Config
    if err := json.Unmarshal(bytes, &cfg); err != nil {
        return nil, fmt.Errorf("failed to unmarshal config JSON: %w", err)
    }

    return &cfg, nil
}


// fetchNewsPage sends an HTTP request to the NewsAPI and returns the response body and rate limits.
func fetchNewsPage(client *http.Client, cfg *Config, apiKey, query, country string, page int, from time.Time) ([]byte, NewsAPILimits, error) {
	params := url.Values{}
	if query != "" {
		params.Add("q", query)
	}
	params.Add("country", country)
	params.Add("pageSize", strconv.Itoa(cfg.MaxPageSize))
	params.Add("page", strconv.Itoa(page))
	params.Add("apiKey", apiKey)

	if !from.IsZero() {
		params.Add("from", from.Format("2006-01-02"))
	}

	fullURL := cfg.BaseURL + "?" + params.Encode()
	log.Printf("Downloading page %d from: %s", page, fullURL)

	resp, err := client.Get(fullURL)
	if err != nil {
		return nil, NewsAPILimits{}, fmt.Errorf("failed to make HTTP request to News API for page %d: %w", page, err)
	}
	defer resp.Body.Close()

	var limits NewsAPILimits
	if limitStr := resp.Header.Get("X-RateLimit-Limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			limits.Limit = limit
		}
	}
	if remainingStr := resp.Header.Get("X-RateLimit-Remaining"); remainingStr != "" {
		if remaining, err := strconv.Atoi(remainingStr); err == nil {
			limits.Remaining = remaining
		}
	}
	if resetStr := resp.Header.Get("X-RateLimit-Reset"); resetStr != "" {
		if resetUnix, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
			limits.Reset = time.Unix(resetUnix, 0)
		}
	}

	log.Printf("API Rate Limits: Limit=%d, Remaining=%d, Reset=%s",
		limits.Limit, limits.Remaining, limits.Reset.Format(time.RFC3339))

	if resp.StatusCode == http.StatusTooManyRequests {
		sleepDuration := time.Duration(cfg.DefaultRateLimitDelaySeconds) * time.Second
		if time.Now().Before(limits.Reset) {
			sleepDuration = time.Until(limits.Reset) + 1*time.Second
		}
		log.Printf("Received 429 Too Many Requests. Sleeping for %v until next window.", sleepDuration)
		time.Sleep(sleepDuration)
		// Return an error to signal the outer loop to retry this page.
		return nil, limits, fmt.Errorf("rate limit hit, retrying after sleep")
	} else if resp.StatusCode != http.StatusOK {
		bodyBytes, readErr := ioutil.ReadAll(resp.Body)
		if readErr != nil {
			log.Printf("Warning: Could not read response body for non-OK status on page %d: %v", page, readErr)
		}
		var apiErrorResp NewsAPIResponse
		unmarshalErr := json.Unmarshal(bodyBytes, &apiErrorResp)
		if unmarshalErr != nil {
			log.Printf("Warning: Failed to unmarshal error response JSON for page %d: %v", page, unmarshalErr)
		}

		errorMessage := fmt.Sprintf("News API returned non-OK status for page %d: %d. ", page, resp.StatusCode)
		if apiErrorResp.Code != "" || apiErrorResp.Message != "" {
			errorMessage += fmt.Sprintf("API Error: Code=%s, Message=%s", apiErrorResp.Code, apiErrorResp.Message)
		} else {
			errorMessage += fmt.Sprintf("Response Body: %s", string(bodyBytes))
		}
		return nil, limits, fmt.Errorf(errorMessage)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, limits, fmt.Errorf("failed to read response body for page %d: %w", page, err)
	}

	return body, limits, nil
}

// DownloadAllNewsToFile fetches and saves news articles, and publishes their paths to Kafka.
func DownloadAllNewsToFile(
	apiKey, query, country string,
	from time.Time,
	outputDir string,
	cfg *Config,
) (int, error) {
	if apiKey == "" {
		return 0, fmt.Errorf("API key cannot be empty. Please provide your NewsAPI key")
	}
	if country == "" {
		return 0, fmt.Errorf("country parameter cannot be empty for 'top-headlines' endpoint")
	}
	if cfg.MaxPageSize <= 0 || cfg.MaxPageSize > 100 {
		return 0, fmt.Errorf("pageSize must be between 1 and 100, got %d", cfg.MaxPageSize)
	}

	currentPage := 1
	totalPages := 1
	totalArticlesFound := 0

	client := &http.Client{Timeout: 30 * time.Second}

	for currentPage <= totalPages {
		body, limits, err := fetchNewsPage(client, cfg, apiKey, query, country, currentPage, from)
		if err != nil {
			// If it's a rate limit error, the fetchNewsPage function has already slept.
			// So, we just continue to the next iteration to retry.
			if err.Error() == "rate limit hit, retrying after sleep" {
				continue
			}
			return 0, err
		}

		// Use the new function to get the output directory and full JSON path
		fullOutputDir, fullJSONPath := utils.GenerateJSONFilePath(outputDir, country, currentPage)
		// Create output directory structure if it doesn't exist for the current file
		if err := os.MkdirAll(fullOutputDir, 0755); err != nil {
			return 0, fmt.Errorf("failed to create output directory structure %s: %w", fullOutputDir, err)
		}

		// Save the raw JSON response to file
		err = ioutil.WriteFile(fullJSONPath, body, 0644)
		if err != nil {
			return 0, fmt.Errorf("failed to write JSON to file %s: %w", fullJSONPath, err)
		}
		log.Printf("Saved page %d to %s", currentPage, fullJSONPath)

		// Publish the file path to Kafka using the new module
		log.Printf("Publishing file path to Kafka topic '%s'...", cfg.KafkaTopic)
		if err := kafka_producer.PublishToKafka(cfg.KafkaBroker, cfg.KafkaTopic, fullJSONPath); err != nil {
			log.Printf("Failed to publish file path to Kafka: %v", err)
		}

		var newsResp NewsAPIResponse
		err = json.Unmarshal(body, &newsResp)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal JSON response for pagination on page %d: %w", currentPage, err)
		}

		if newsResp.Status != "ok" {
			return 0, fmt.Errorf("News API response status is not 'ok' for page %d. Code: %s, Message: %s",
				currentPage, newsResp.Code, newsResp.Message)
		}

		if currentPage == 1 {
			totalArticlesFound = newsResp.TotalResults
			totalPages = (totalArticlesFound + cfg.MaxPageSize - 1) / cfg.MaxPageSize
		}

		log.Printf("Total results found: %d, Current page: %d, Estimated total pages: %d",
			totalArticlesFound, currentPage, totalPages)

		currentPage++
		time.Sleep(500 * time.Millisecond)
	}

	return totalArticlesFound, nil
}