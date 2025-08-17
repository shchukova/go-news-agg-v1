package kafka_producer

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// MockKafkaPublisher implements KafkaPublisher for testing
type MockKafkaPublisher struct {
	publishedMessages []PublishedMessage
	shouldFail        bool
	failureError      error
}

type PublishedMessage struct {
	Broker  string
	Topic   string
	Message string
}

func NewMockKafkaPublisher() *MockKafkaPublisher {
	return &MockKafkaPublisher{
		publishedMessages: make([]PublishedMessage, 0),
	}
}

func (m *MockKafkaPublisher) Publish(broker, topic, message string) error {
	return m.PublishWithContext(context.Background(), broker, topic, message)
}

func (m *MockKafkaPublisher) PublishWithContext(ctx context.Context, broker, topic, message string) error {
	if m.shouldFail {
		return m.failureError
	}

	m.publishedMessages = append(m.publishedMessages, PublishedMessage{
		Broker:  broker,
		Topic:   topic,
		Message: message,
	})
	return nil
}

func (m *MockKafkaPublisher) Close() error {
	return nil
}

func (m *MockKafkaPublisher) SetShouldFail(shouldFail bool, err error) {
	m.shouldFail = shouldFail
	m.failureError = err
}

func (m *MockKafkaPublisher) GetPublishedMessages() []PublishedMessage {
	return m.publishedMessages
}

func (m *MockKafkaPublisher) Reset() {
	m.publishedMessages = make([]PublishedMessage, 0)
	m.shouldFail = false
	m.failureError = nil
}

// TestNewProducer tests the producer creation
func TestNewProducer(t *testing.T) {
	tests := []struct {
		name      string
		brokerURL string
		wantErr   bool
	}{
		{
			name:      "valid broker URL",
			brokerURL: "mock://",
			wantErr:   false,
		},
		{
			name:      "empty broker URL",
			brokerURL: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(tt.brokerURL)
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewProducer() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("NewProducer() unexpected error: %v", err)
				return
			}

			if producer == nil {
				t.Errorf("NewProducer() returned nil producer")
				return
			}

			// Clean up
			producer.Close()
		})
	}
}

// TestProducerPublish tests the publish functionality
func TestProducerPublish(t *testing.T) {
	// Only test with mock broker for unit tests
	producer, err := NewProducer("mock://")
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	tests := []struct {
		name    string
		topic   string
		message string
		wantErr bool
	}{
		{
			name:    "valid message",
			topic:   "test-topic",
			message: "test message",
			wantErr: false,
		},
		{
			name:    "empty topic",
			topic:   "",
			message: "test message",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := producer.Publish("mock://", tt.topic, tt.message)
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("Publish() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Publish() unexpected error: %v", err)
			}
		})
	}
}

// TestProducerPublishWithContext tests the publish with context functionality
func TestProducerPublishWithContext(t *testing.T) {
	producer, err := NewProducer("mock://")
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	t.Run("publish with context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := producer.PublishWithContext(ctx, "mock://", "test-topic", "test message")
		if err != nil {
			t.Errorf("PublishWithContext() unexpected error: %v", err)
		}
	})

	t.Run("publish with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := producer.PublishWithContext(ctx, "mock://", "test-topic", "test message")
		if err == nil {
			t.Errorf("PublishWithContext() with cancelled context should return error")
		}

		if !strings.Contains(err.Error(), "cancelled") {
			t.Errorf("Expected cancellation error, got: %v", err)
		}
	})
}

// TestProducerClose tests the close functionality
func TestProducerClose(t *testing.T) {
	producer, err := NewProducer("mock://")
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	// Close should not return error
	err = producer.Close()
	if err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}

	// Second close should also not return error
	err = producer.Close()
	if err != nil {
		t.Errorf("Second Close() unexpected error: %v", err)
	}

	// Publishing after close should return error
	err = producer.Publish("mock://", "test-topic", "test message")
	if err == nil {
		t.Errorf("Publish() after Close() should return error")
	}
}

// TestPublishToKafkaSuccess tests the legacy function with a mocked, successful Kafka broker
func TestPublishToKafkaSuccess(t *testing.T) {
	// Restore default log output after test
	originalOutput := os.Stderr
	defer func() {
		if originalOutput != nil {
			// In a real scenario, you might want to capture logs differently
		}
	}()

	err := PublishToKafka("mock://", "test-topic", "test message")
	if err != nil {
		t.Fatalf("PublishToKafka failed with an unexpected error: %v", err)
	}
}

// TestPublishToKafkaInvalidBroker tests the function with an invalid Kafka broker address
func TestPublishToKafkaInvalidBroker(t *testing.T) {
	// Provide an invalid broker address that the producer cannot connect to
	invalidBroker := "localhost:9999"
	topic := "test-topic"
	message := "test message"

	err := PublishToKafka(invalidBroker, topic, message)

	// Expect the function to return a non-nil error
	if err == nil {
		t.Fatalf("Expected an error for an invalid broker, but got nil")
	}

	// Check that the error message contains a specific connection-related phrase
	expectedErrorSubstring := "failed to create Kafka producer"
	if !strings.Contains(err.Error(), expectedErrorSubstring) {
		t.Errorf("Expected error message to contain '%s', but got '%v'", expectedErrorSubstring, err)
	}
}

// TestMockKafkaPublisher tests the mock implementation
func TestMockKafkaPublisher(t *testing.T) {
	mock := NewMockKafkaPublisher()

	// Test successful publish
	err := mock.Publish("test-broker", "test-topic", "test-message")
	if err != nil {
		t.Errorf("Mock publish should not return error: %v", err)
	}

	messages := mock.GetPublishedMessages()
	if len(messages) != 1 {
		t.Errorf("Expected 1 published message, got %d", len(messages))
	}

	if messages[0].Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", messages[0].Topic)
	}

	// Test failure scenario
	mock.SetShouldFail(true, kafka.NewError(kafka.ErrBrokerNotAvailable, "test error", false))
	err = mock.Publish("test-broker", "test-topic", "test-message")
	if err == nil {
		t.Errorf("Mock publish should return error when configured to fail")
	}

	// Reset and test again
	mock.Reset()
	messages = mock.GetPublishedMessages()
	if len(messages) != 0 {
		t.Errorf("Expected 0 published messages after reset, got %d", len(messages))
	}

	err = mock.Publish("test-broker", "test-topic", "test-message")
	if err != nil {
		t.Errorf("Mock publish should not return error after reset: %v", err)
	}
}