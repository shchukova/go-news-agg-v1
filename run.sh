#!/bin/bash
export NEWSAPI_KEY="your-newsapi-key-here"
export NEWS_COUNTRY="us"
export NEWS_OUTPUT_DIR="/tmp/news_downloads"
export KAFKA_BROKER="localhost:9092"
export KAFKA_TOPIC="news_files"

# Build and run
go build -o news-downloader ./cmd/downloader
./news-downloader