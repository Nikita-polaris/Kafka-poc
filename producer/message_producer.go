package producer

// MessageProducer defines the interface for message producers
type MessageProducer interface {
    ProduceMessage(message []byte) error
}