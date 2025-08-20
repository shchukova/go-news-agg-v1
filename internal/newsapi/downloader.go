package newsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"go-news-agg/internal/config"
	"go-news-agg/internal/kafka_producer"
	"go-news-agg/pkg/utils"
)

// NewsDownloader handles downloading news articles from NewsAPI
type NewsDownloader struct {
	client    *NewsAPIClient
	publisher kafka_producer.KafkaPublisher
	config    *config.Config
}

// NewNewsDownloader creates a new news downloader with the given dependencies
func NewNewsDownloader(client *NewsAPIClient, publisher kafka_producer.KafkaPublisher, cfg *config.Config) *NewsDownloader {
	return &NewsDownloader{
		client:    client,
		publisher: publisher,
		config:    cfg,
	}
}

// NewNewsDownloaderWithDefaults creates a news downloader with default dependencies
func NewNewsDownloaderWithDefaults(cfg *config.Config) (*NewsDownloader, error) {
	client := NewNewsAPIClient(cfg)
	
	producer, err := kafka_producer.NewProducer(cfg.KafkaBroker)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &NewsDownloader{
		client:    client,
		publisher: producer,
		config:    cfg,
	}, nil
}

// DownloadAllNewsToFile fetches and saves news articles, and publishes their paths to Kafka
func (d *NewsDownloader) DownloadAllNewsToFile(ctx context.Context, req *DownloadRequest) (*DownloadResult, error) {
	startTime := time.Now()
	
	// Validate the request
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid download request: %w", err)
	}

	result := &DownloadResult{
		StartTime:       startTime,
		FilePaths:       make([]string, 0),
		PagesDownloaded: 0,
		Errors:          make([]error, 0),
	}

	currentPage := req.StartPage
	totalPages := 1
	totalArticlesFound := 0

	log.Printf("Starting news download for country=%s, query=%s, from=%s", 
		req.Country, req.Query, req.From.Format("2006-01-02"))

	for currentPage <= totalPages {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("download cancelled: %w", ctx.Err())
		default:
		}

		// Fetch the page
		newsResp, limits, err := d.client.FetchNewsPage(ctx, req, currentPage)
		if err != nil {
			// Handle rate limiting by retrying
			if rateLimitErr, ok := err.(*RateLimitError); ok {
				log.Printf("Rate limit hit, waiting %v before retry", rateLimitErr.RetryAfter)
				
				select {
				case <-time.After(rateLimitErr.RetryAfter):
					continue // Retry the same page
				case <-ctx.Done():
					return result, fmt.Errorf("download cancelled during rate limit wait: %w", ctx.Err())
				}
			}
			
			// For other errors, record and continue or fail depending on severity
			result.Errors = append(result.Errors, fmt.Errorf("page %d: %w", currentPage, err))
			
			// For critical errors, fail immediately
			if _, ok := err.(*NewsAPIError); ok {
				return result, fmt.Errorf("API error on page %d: %w", currentPage, err)
			}
			
			// For other errors, skip this page and continue
			log.Printf("Error on page %d, skipping: %v", currentPage, err)
			currentPage++
			continue
		}

		// Log rate limit status
		if limits != nil {
			log.Printf("API Rate Limits: Limit=%d, Remaining=%d, Reset=%s",
				limits.Limit, limits.Remaining, limits.Reset.Format(time.RFC3339))
		}

		// Save the page to file
		filePath, err := d.savePageToFile(newsResp, req.Country, currentPage)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("failed to save page %d: %w", currentPage, err))
			currentPage++
			continue
		}

		result.FilePaths = append(result.FilePaths, filePath)
		result.PagesDownloaded++

		log.Printf("Saved page %d to %s", currentPage, filePath)

		// Publish file path to Kafka
		if err := d.publishFilePath(ctx, filePath); err != nil {
			// Log the error but don't fail the download
			log.Printf("Failed to publish file path to Kafka: %v", err)
			result.Errors = append(result.Errors, fmt.Errorf("kafka publish for %s: %w", filePath, err))
		}

		// Update totals on first page
		if currentPage == req.StartPage {
			totalArticlesFound = newsResp.TotalResults
			totalPages = (totalArticlesFound + req.PageSize - 1) / req.PageSize
			result.TotalArticles = totalArticlesFound
			
			log.Printf("Total results found: %d, Estimated total pages: %d", 
				totalArticlesFound, totalPages)
		}

		log.Printf("Progress: %d/%d pages completed", currentPage-req.StartPage+1, totalPages)

		currentPage++

		// Add a small delay between requests to be respectful
		if currentPage <= totalPages {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return result, fmt.Errorf("download cancelled: %w", ctx.Err())
			}
		}
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Printf("Download completed: %d articles across %d pages in %v", 
		result.TotalArticles, result.PagesDownloaded, result.Duration)

	return result, nil
}

// savePageToFile saves a news page response to a JSON file
func (d *NewsDownloader) savePageToFile(newsResp *NewsAPIResponse, country string, page int) (string, error) {
	// Generate file path
	fullOutputDir, fullJSONPath := utils.GenerateJSONFilePath(d.config.OutputDir, country, page)

	// Create output directory structure if it doesn't exist
	if err := os.MkdirAll(fullOutputDir, 0755); err != nil {
		return "", &FileOperationError{
			Operation: "create directory",
			FilePath:  fullOutputDir,
			Cause:     err,
		}
	}

	// Marshal the response to JSON
	jsonData, err := json.MarshalIndent(newsResp, "", "  ")
	if err != nil {
		return "", &FileOperationError{
			Operation: "marshal JSON",
			FilePath:  fullJSONPath,
			Cause:     err,
		}
	}

	// Write the JSON to file
	if err := ioutil.WriteFile(fullJSONPath, jsonData, 0644); err != nil {
		return "", &FileOperationError{
			Operation: "write file",
			FilePath:  fullJSONPath,
			Cause:     err,
		}
	}

	return fullJSONPath, nil
}

// publishFilePath publishes a file path to Kafka
func (d *NewsDownloader) publishFilePath(ctx context.Context, filePath string) error {
	if d.publisher == nil {
		return fmt.Errorf("Kafka publisher not initialized")
	}

	log.Printf("Publishing file path to Kafka topic '%s'...", d.config.KafkaTopic)
	
	if err := d.publisher.PublishWithContext(ctx, d.config.KafkaBroker, d.config.KafkaTopic, filePath); err != nil {
		return &KafkaError{
			Operation: "publish",
			Topic:     d.config.KafkaTopic,
			Broker:    d.config.KafkaBroker,
			Cause:     err,
		}
	}

	return nil
}

// Close closes the downloader and releases resources
func (d *NewsDownloader) Close() error {
	if d.publisher != nil {
		return d.publisher.Close()
	}
	return nil
}

// Legacy function for backward compatibility
func DownloadAllNewsToFile(apiKey, query, country string, from time.Time, cfg *config.Config) (int, error) {
	// Create download request
	req := NewDownloadRequest(apiKey, country)
	req.Query = query
	req.From = from
	req.PageSize = cfg.MaxPageSize

	// Create downloader with defaults
	downloader, err := NewNewsDownloaderWithDefaults(cfg)
	if err != nil {
		return 0, err
	}
	defer downloader.Close()

	// Execute download with background context
	ctx := context.Background()
	result, err := downloader.DownloadAllNewsToFile(ctx, req)
	if err != nil {
		return 0, err
	}

	return result.TotalArticles, nil
}