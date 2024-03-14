package consumer

import "github.com/confluentinc/confluent-kafka-go/kafka"

// MessageHandler defines the function signature for message handlers
type MessageHandler func(message []byte)

// MessageConsumer defines the interface for message consumers
type MessageConsumer interface {
	ConsumeMessages(handler MessageHandler)
	GetKafkaProducer() *kafka.Producer
	// ProduceMessages(message []byte)
	// SeekToBeginning()
}
