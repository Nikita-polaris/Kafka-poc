package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"poc-kafka/producer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

var (
	kafkaBrokers string
	kafkaTopic   string
)

func init() {

	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Error loading .env file", err)
		os.Exit(1)

	}
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
}

type Message struct {
	SequenceNo     string `json:"SequenceNo"`
	TransactionID  string `json:"transactionId"`
	MtrNumber      string `json:"mtrNmbr"`
	DiscomCode     string `json:"discomCode"`
	FdrCode        string `json:"fdrCode"`
	SourceSystem   string `json:"sourceSystem"`
	SourceType     string `json:"sourceType"`
	SecPerInterval string `json:"secPerInterval"`
	Timestamp      string `json:"timestamp"`
}

func produceNumbers(producer producer.MessageProducer) {
	for i := 1; i <= 1000; i++ {
		message := Message{
			SequenceNo:     fmt.Sprintf("%d", i),
			TransactionID:  "3eb2e983-7442-40e9-bc66-62e5223c0579",
			MtrNumber:      "Sc10240134",
			DiscomCode:     "Ladakh Discom",
			FdrCode:        "LU02",
			SourceSystem:   "Polaris",
			SourceType:     "AMI-SP",
			SecPerInterval: "900",
			Timestamp:      time.Now().Format(time.RFC3339),
		}

		messageJSON, err := json.Marshal(message)
		if err != nil {
			fmt.Printf("Failed to marshal JSON: %v\n", err)
			continue
		}

		fmt.Printf("Producing message: %s\n", messageJSON)
		err = producer.ProduceMessage(messageJSON)
		if err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		}

		time.Sleep(1000 * time.Millisecond) // Sleep for some time between each message
	}
}

func main() {
	// Create a Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		return
	}
	defer p.Close()

	// Create a KafkaProducer instance
	kafkaProducer := producer.NewKafkaProducer(p, kafkaTopic)

	// Produce messages using the KafkaProducer
	produceNumbers(kafkaProducer)

	// Setup signal handler to handle termination gracefully
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	fmt.Println("Producer shutting down...")
}
