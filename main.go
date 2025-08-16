package main

import (
	"fmt"
	"log"
	"os"
	"time" // Import time package

	// Your existing modules
	"go-news-agg/newsapi"
)


func main() {
	// --- Configuration for News Download ---
	apiKey := os.Getenv("NEWSAPI_KEY")
	if apiKey == "" {
		log.Fatalf("Error: NEWSAPI_KEY environment variable not set. Please set your NewsAPI key.")
	}

	query := ""       // The news topic you want to download (optional for top-headlines)
	country := "us"                     // The country for top-headlines
	pageSize := newsapi.MaxPageSize     // Use the constant from newsapi module
	downloadOutputDir := "/tmp/downloaded_news" // Directory to save the JSON files

	// Calculate "yesterday"
	yesterday := time.Now().AddDate(0, 0, -1) // Subtracts 1 day from today
	// If you want "from" the beginning of yesterday, you can reset time component:
	yesterdayStartOfDay := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())


	log.Printf("--- Starting News Download for query: '%s', country: '%s', from: '%s' ---", query, country, yesterdayStartOfDay.Format("2006-01-02"))

	totalArticlesDownloaded, err := newsapi.DownloadAllNewsToFile(apiKey, query, country, yesterdayStartOfDay, pageSize, downloadOutputDir)
	if err != nil {
		log.Fatalf("Failed to download all news: %v", err)
	}

	log.Printf("Successfully downloaded %d articles (JSON files) into the '%s' directory.", totalArticlesDownloaded, downloadOutputDir)
	fmt.Println("\n--- News Download Completed ---")
	fmt.Println("\nPipeline finished successfully!")
}