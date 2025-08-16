// kafka_producer/producer_test.go
package kafka_producer

import (
	"log"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TestPublishToKafkaSuccess tests the function with a mocked, successful Kafka broker.
func TestPublishToKafkaSuccess(t *testing.T) {
	// --- Arrange ---
	// 1. Create a mock Kafka broker instance.
	mockBroker := "mock://"
	topic := "test-topic"
	message := "test message"

	// 2. Create a buffer to capture logs for verification.
	var logOutput strings.Builder
	log.SetOutput(&logOutput)
	defer func() {
		log.SetOutput(os.Stderr) // Restore the default log output.
	}()

	// --- Act ---
	// 3. Call the function with the mock broker address.
	err := PublishToKafka(mockBroker, topic, message)
	if err != nil {
		t.Fatalf("PublishToKafka failed with an unexpected error: %v", err)
	}

	// Wait a moment for the delivery report goroutine to execute.
	time.Sleep(100 * time.Millisecond)

	// --- Assert ---
	// 4. Check for successful delivery log message.
	if !strings.Contains(logOutput.String(), "Delivered message to topic") {
		t.Errorf("Expected log message for successful delivery, but none was found. Log: %s", logOutput.String())
	}
}

// TestPublishToKafkaInvalidBroker tests the function with an invalid Kafka broker address.
func TestPublishToKafkaInvalidBroker(t *testing.T) {
	// --- Arrange ---
	// Provide an invalid broker address that the producer cannot connect to.
	invalidBroker := "localhost:9999"
	topic := "test-topic"
	message := "test message"

	// --- Act ---
	err := PublishToKafka(invalidBroker, topic, message)

	// --- Assert ---
	// Expect the function to return a non-nil error.
	if err == nil {
		t.Fatalf("Expected an error for an invalid broker, but got nil")
	}

	// Check that the error message contains a specific connection-related phrase.
	expectedErrorSubstring := "failed to create Kafka producer"
	if !strings.Contains(err.Error(), expectedErrorSubstring) {
		t.Errorf("Expected error message to contain '%s', but got '%v'", expectedErrorSubstring, err)
	}
}