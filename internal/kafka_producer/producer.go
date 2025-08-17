package kafka_producer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaPublisher defines the interface for publishing messages to Kafka
type KafkaPublisher interface {
	Publish(broker, topic, message string) error
	PublishWithContext(ctx context.Context, broker, topic, message string) error
	Close() error
}

// Producer wraps the Kafka producer with proper resource management
type Producer struct {
	producer *kafka.Producer
	mutex    sync.Mutex
	closed   bool
}

// NewProducer creates a new Kafka producer with the given configuration
func NewProducer(brokerURL string) (*Producer, error) {
	if brokerURL == "" {
		return nil, fmt.Errorf("broker URL cannot be empty")
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokerURL,
		"acks":             "all",
		"retries":          3,
		"batch.size":       16384,
		"linger.ms":        1,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	p := &Producer{
		producer: producer,
	}

	// Start the delivery report handler
	go p.handleDeliveryReports()

	return p, nil
}

// Publish publishes a message to the specified topic
func (p *Producer) Publish(broker, topic, message string) error {
	return p.PublishWithContext(context.Background(), broker, topic, message)
}

// PublishWithContext publishes a message with context support
func (p *Producer) PublishWithContext(ctx context.Context, broker, topic, message string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return fmt.Errorf("producer is closed")
	}

	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}

	// Use delivery channel for synchronous operation
	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report with context
	select {
	case e := <-deliveryChan:
		if msg, ok := e.(*kafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				return fmt.Errorf("delivery failed: %w", msg.TopicPartition.Error)
			}
			log.Printf("Message delivered to %s [%d] at offset %v",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
		}
	case <-ctx.Done():
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case <-time.After(30 * time.Second):
		return fmt.Errorf("publish timeout after 30 seconds")
	}

	return nil
}

// handleDeliveryReports processes delivery reports in a separate goroutine
func (p *Producer) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
			} else {
				log.Printf("Delivered message to topic %s [%d] at offset %v",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		case kafka.Error:
			log.Printf("Kafka error: %v", ev)
		}
	}
}

// Close closes the producer and releases resources
func (p *Producer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// Wait for all messages to be delivered or fail
	p.producer.Flush(30 * 1000) // 30 seconds timeout
	p.producer.Close()

	return nil
}

// PublishToKafka is the legacy function for backward compatibility
func PublishToKafka(broker, topic, message string) error {
	producer, err := NewProducer(broker)
	if err != nil {
		return err
	}
	defer producer.Close()

	return producer.Publish(broker, topic, message)
}