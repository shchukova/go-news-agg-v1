package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-news-agg/config"
	"go-news-agg/newsapi"
)

func main() {
	// Create context that can be cancelled on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Received interrupt signal, shutting down gracefully...")
		cancel()
	}()

	// Load configuration
	cfg, err := loadConfiguration()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Get API key
	apiKey := os.Getenv("NEWSAPI_KEY")
	if apiKey == "" {
		log.Fatalf("Error: NEWSAPI_KEY environment variable not set. Please set your NewsAPI key.")
	}

	// Setup download parameters
	query := os.Getenv("NEWS_QUERY")        // Optional: specific topic to search for
	country := getEnvWithDefault("NEWS_COUNTRY", "us") // Default to US news
	
	// Calculate "yesterday" for the news search
	yesterday := time.Now().AddDate(0, 0, -1)
	yesterdayStartOfDay := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())

	// Create download request
	req := newsapi.NewDownloadRequest(apiKey, country)
	req.Query = query
	req.From = yesterdayStartOfDay
	req.PageSize = cfg.MaxPageSize

	log.Printf("--- Starting News Download ---")
	log.Printf("Query: '%s', Country: '%s', From: '%s'", 
		req.Query, req.Country, req.From.Format("2006-01-02"))
	log.Printf("Output Directory: '%s'", cfg.OutputDir)
	log.Printf("Kafka Broker: '%s', Topic: '%s'", cfg.KafkaBroker, cfg.KafkaTopic)

	// Create news downloader
	downloader, err := newsapi.NewNewsDownloaderWithDefaults(cfg)
	if err != nil {
		log.Fatalf("Failed to create news downloader: %v", err)
	}
	defer func() {
		if closeErr := downloader.Close(); closeErr != nil {
			log.Printf("Error closing downloader: %v", closeErr)
		}
	}()

	// Execute download with context and timeout
	downloadCtx, downloadCancel := context.WithTimeout(ctx, 30*time.Minute)
	defer downloadCancel()

	result, err := downloader.DownloadAllNewsToFile(downloadCtx, req)
	if err != nil {
		log.Fatalf("Failed to download news: %v", err)
	}

	// Display results
	displayResults(result)

	// Check for any errors that occurred during download
	if len(result.Errors) > 0 {
		log.Printf("Download completed with %d errors:", len(result.Errors))
		for i, err := range result.Errors {
			log.Printf("  Error %d: %v", i+1, err)
		}
	}

	fmt.Println("\n--- News Download Completed ---")
	fmt.Println("Pipeline finished successfully!")
}

// loadConfiguration loads the application configuration from file or environment
func loadConfiguration() (*config.Config, error) {
	// Try to load from config file first
	configPath := os.Getenv("CONFIG_PATH")
	if configPath != "" {
		log.Printf("Loading configuration from file: %s", configPath)
		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Printf("Failed to load config from file '%s': %v", configPath, err)
			log.Println("Falling back to environment variables...")
		} else {
			return cfg, nil
		}
	}

	// Fall back to environment variables
	log.Println("Loading configuration from environment variables...")
	cfg := config.LoadConfigFromEnv()
	
	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// displayResults shows a summary of the download results
func displayResults(result *newsapi.DownloadResult) {
	fmt.Printf("\n=== Download Summary ===\n")
	fmt.Printf("Total Articles Found: %d\n", result.TotalArticles)
	fmt.Printf("Pages Downloaded: %d\n", result.PagesDownloaded)
	fmt.Printf("Files Created: %d\n", len(result.FilePaths))
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Duration: %v\n", result.Duration.Round(time.Second))
	
	if len(result.FilePaths) > 0 {
		fmt.Printf("\nFiles created:\n")
		for i, path := range result.FilePaths {
			fmt.Printf("  %d. %s\n", i+1, path)
		}
	}

	if len(result.Errors) > 0 {
		fmt.Printf("\nErrors encountered: %d\n", len(result.Errors))
	}
}

// getEnvWithDefault returns the value of an environment variable or a default value
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Alternative main function for testing with custom config
func runWithConfig(cfg *config.Config, apiKey string) error {
	ctx := context.Background()

	// Setup download parameters
	query := ""
	country := "us"
	yesterday := time.Now().AddDate(0, 0, -1)
	yesterdayStartOfDay := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())

	// Create download request
	req := newsapi.NewDownloadRequest(apiKey, country)
	req.Query = query
	req.From = yesterdayStartOfDay
	req.PageSize = cfg.MaxPageSize

	// Create and run downloader
	downloader, err := newsapi.NewNewsDownloaderWithDefaults(cfg)
	if err != nil {
		return fmt.Errorf("failed to create downloader: %w", err)
	}
	defer downloader.Close()

	_, err = downloader.DownloadAllNewsToFile(ctx, req)
	return err
}