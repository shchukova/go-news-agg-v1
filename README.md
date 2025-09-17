# Go News Aggregator v1

A lightweight and efficient RSS feed aggregator built with Go that fetches, parses, and aggregates news content from multiple sources. This service provides a RESTful API for managing RSS feeds and accessing aggregated content in a unified format.

## Features

- 🔄 **Automatic Feed Processing** - Continuously fetches and processes RSS/Atom feeds
- 🚀 **Concurrent Operations** - Handles multiple feeds simultaneously for optimal performance
- 📊 **RESTful API** - Clean HTTP endpoints for feed management and content retrieval
- 🗄️ **Database Storage** - Persistent storage for feeds, posts, and metadata
- 🔍 **Duplicate Detection** - Prevents duplicate content storage
- ⚡ **Fast Response Times** - Optimized for quick content delivery
- 🛡️ **Error Handling** - Robust error handling for unreliable feed sources

## Tech Stack

- **Language**: Go 1.19+
- **HTTP Framework**: Gorilla Mux / Chi Router (or standard library)
- **Database**: PostgreSQL / SQLite
- **RSS Parsing**: gofeed library
- **Environment**: Docker support

## Quick Start

### Prerequisites

- Go 1.19 or higher
- PostgreSQL (or SQLite for development)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/shchukova/go-news-agg-v1.git
   cd go-news-agg-v1
   ```

2. **Install dependencies**
   ```bash
   go mod download
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials and configuration
   ```

4. **Run database migrations**
   ```bash
   go run cmd/migrate/main.go
   ```

5. **Start the server**
   ```bash
   go run cmd/server/main.go
   ```

The server will start on `http://localhost:8080` by default.

## API Endpoints

### Feed Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/feeds` | Add a new RSS feed |
| `GET` | `/api/v1/feeds` | List all feeds |
| `GET` | `/api/v1/feeds/{id}` | Get feed details |
| `DELETE` | `/api/v1/feeds/{id}` | Remove a feed |

### Posts

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/posts` | Get aggregated posts |
| `GET` | `/api/v1/posts/{id}` | Get specific post |
| `GET` | `/api/v1/feeds/{id}/posts` | Get posts from specific feed |

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Service health status |

## Usage Examples

### Add a new RSS feed
```bash
curl -X POST http://localhost:8080/api/v1/feeds \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Go Blog",
    "url": "https://go.dev/blog/feed.atom",
    "description": "Official Go programming language blog"
  }'
```

### Get all posts
```bash
curl http://localhost:8080/api/v1/posts
```

### Get posts with pagination
```bash
curl "http://localhost:8080/api/v1/posts?limit=10&offset=0"
```

## Configuration

The application can be configured using environment variables:

```bash
# Server Configuration
PORT=8080
HOST=localhost

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=news_aggregator

# Feed Processing
FETCH_INTERVAL=300  # seconds between feed updates
MAX_WORKERS=10      # concurrent feed processors
REQUEST_TIMEOUT=30  # HTTP request timeout in seconds

# Logging
LOG_LEVEL=info
```

## Project Structure

```
go-news-agg-v1/
├── cmd/
│   ├── server/         # Main application entry point
│   └── migrate/        # Database migration tools
├── internal/
│   ├── api/           # HTTP handlers and routes
│   ├── config/        # Configuration management
│   ├── database/      # Database models and queries
│   ├── feed/          # RSS feed processing logic
│   └── models/        # Data structures
├── migrations/        # SQL migration files
├── pkg/              # Reusable packages
├── scripts/          # Build and deployment scripts
├── .env.example      # Environment variables template
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Development

### Running Tests
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests with race detection
go test -race ./...
```

### Building
```bash
# Build for current platform
go build -o bin/news-aggregator cmd/server/main.go

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o bin/news-aggregator-linux cmd/server/main.go
```

### Docker

Build and run with Docker:
```bash
# Build image
docker build -t go-news-agg .

# Run with docker-compose
docker-compose up -d
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Go best practices and conventions
- Write tests for new features
- Update documentation as needed
- Use `go fmt` and `go vet` before committing
- Keep commits atomic and descriptive

## Roadmap

- [ ] Web UI for feed management
- [ ] User authentication and authorization
- [ ] Feed categorization and tagging
- [ ] Full-text search functionality
- [ ] Export functionality (JSON, CSV)
- [ ] Webhook notifications
- [ ] Mobile app support
- [ ] AI-powered content summarization
