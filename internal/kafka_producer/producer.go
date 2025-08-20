package kafka_producer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaPublisher interface {
	Publish(broker, topic, message string) error
	PublishWithContext(ctx context.Context, broker, topic, message string) error
	Close() error
}

type Producer struct {
	producer *kafka.Producer
	mutex    sync.Mutex
	closed   bool
}

func NewProducer(brokerURL string) (*Producer, error) {
	if brokerURL == "" {
		return nil, fmt.Errorf("broker URL cannot be empty")
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokerURL,
		"acks":             "all",
		"retries":          3,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	p := &Producer{
		producer: producer,
	}

	go p.handleEvents()
	return p, nil
}

func (p *Producer) handleEvents() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
		}
	}()

	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
			} else {
				log.Printf("Message delivered to %s [%d] at offset %v",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		case kafka.Error:
			log.Printf("Kafka error: %v", ev)
		}
	}
}

func (p *Producer) Publish(broker, topic, message string) error {
	return p.PublishWithContext(context.Background(), broker, topic, message)
}

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

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	select {
	case e := <-deliveryChan:
		if msg, ok := e.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", msg.TopicPartition.Error)
		}
	case <-ctx.Done():
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case <-time.After(30 * time.Second):
		return fmt.Errorf("publish timeout")
	}

	return nil
}

func (p *Producer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	p.producer.Flush(30 * 1000)
	p.producer.Close()
	return nil
}

func PublishToKafka(broker, topic, message string) error {
	producer, err := NewProducer(broker)
	if err != nil {
		return err
	}
	defer producer.Close()
	return producer.Publish(broker, topic, message)
}