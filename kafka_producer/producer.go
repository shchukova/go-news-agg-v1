package kafka_producer

import (
    "fmt"
    "log"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)

// PublishToKafka publishes a message to a specified Kafka topic.
func PublishToKafka(broker, topic, message string) error {
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
    if err != nil {
        return fmt.Errorf("failed to create Kafka producer: %w", err)
    }
    defer p.Close()

    // Delivery report handler for our producer
    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
                } else {
                    log.Printf("Delivered message to topic %s [%d] at offset %v\n",
                        *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
                }
            }
        }
    }()

    // Produce messages to the topic
    if err := p.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          []byte(message),
    }, nil); err != nil {
        return fmt.Errorf("failed to produce message: %w", err)
    }

    // Wait for message deliveries for up to 30 seconds
    p.Flush(30 * 1000)

    return nil
}