package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumer implements the MessageConsumer interface for Kafka
type KafkaConsumer struct {
	Consumer *kafka.Consumer
	Topic    string
}

func NewKafkaConsumer(consumer *kafka.Consumer, topic string) *KafkaConsumer {
	return &KafkaConsumer{

		Consumer: consumer,
		Topic:    topic,
	}
}

// ConsumeMessages starts consuming messages from Kafka and calls the handler for each message
func (c *KafkaConsumer) ConsumeMessages(handler MessageHandler) {
	for {
		ev := c.Consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			handler(e.Value)

		case kafka.Error:
			// Handle Kafka errors
		}
	}
}

// func (c *KafkaConsumer) ProduceMessages(messages [][]byte) error {

//     brokerList := c.Consumer.GetMetadata(false)
//     if brokerList == nil {
//         return errors.New("failed to retrieve broker list")
//     }

//     producer, err := kafka.NewProducer(&kafka.ConfigMap{
//         "bootstrap.servers": *brokerList,
//     })
//     if err != nil {
//         return err
//     }
//     defer producer.Close()

//     // Produce each message
//     for _, message := range messages {
//         err := producer.Produce(&kafka.Message{
//             TopicPartition: kafka.TopicPartition{Topic: &c.Topic, Partition: kafka.PartitionAny},
//             Value:          message,
//         }, nil)

//         if err != nil {
//             return err
//         }
//     }

//	    return nil
//	}
func (kc *KafkaConsumer) GetKafkaProducer() *kafka.Producer {
	// Configuration for Kafka producer
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		// Add more configuration options as needed
	}

	// Create Kafka producer
	producer, err := kafka.NewProducer(config)
	if err != nil {
		// Handle error
		return nil
	}

	return producer
}
