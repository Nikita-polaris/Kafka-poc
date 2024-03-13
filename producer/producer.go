package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaProducer implements the MessageProducer interface for Kafka
type KafkaProducer struct {
	Producer *kafka.Producer
	Topic    string
}

// NewKafkaProducer creates a new KafkaProducer instance
func NewKafkaProducer(producer *kafka.Producer, topic string) *KafkaProducer {
	return &KafkaProducer{
		Producer: producer,
		Topic:    topic,
	}
}

// ProduceMessage sends a message to the Kafka topic
func (p *KafkaProducer) ProduceMessage(message []byte) error {
	return p.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)
}
