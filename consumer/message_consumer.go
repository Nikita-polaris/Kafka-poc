package consumer

// MessageHandler defines the function signature for message handlers
type MessageHandler func(message []byte)

// MessageConsumer defines the interface for message consumers
type MessageConsumer interface {
    ConsumeMessages(handler MessageHandler)
	// ProduceMessages(message []byte) 
	// SeekToBeginning()
}