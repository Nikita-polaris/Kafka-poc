package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"poc-kafka/consumer"
)

var (
	kafkaBrokers     string
	kafkaTopic       string
	postgresHost     string
	postgresPort     string
	postgresUser     string
	postgresPassword string
	postgresDB       string
	blockSize        int
	async            bool
)

func init() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
		os.Exit(1)
	}

	// Set variables from environment variables
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	postgresHost = os.Getenv("POSTGRES_HOST")
	postgresPort = os.Getenv("POSTGRES_PORT")
	postgresUser = os.Getenv("POSTGRES_USER")
	postgresPassword = os.Getenv("POSTGRES_PASSWORD")
	postgresDB = os.Getenv("POSTGRES_DB")
	blockSize = 100
	async = true // Set to false for synchronous approach
}

func checkPostgresConnection() error {
	// Construct the connection string
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB)

	// Open a connection to the PostgreSQL database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Attempt to ping the database
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping the database: %v", err)
	}

	fmt.Println("Successfully connected to PostgreSQL database.")
	return nil
}

func consumeAndSaveNumbers(resultCh chan<- string, db *sql.DB, consumer consumer.MessageConsumer) {
	var numbers []string

	handler := func(message []byte) {
		num := string(message)
		if num == "NULL" {
			fmt.Println("Received NULL block acknowledgment")
			return
		}

		resultCh <- num
		fmt.Printf("Received number: %s\n", num)

		numbers = append(numbers, num)

		if len(numbers) >= blockSize {
			err := saveBlockToPostgres(db, numbers)
			if err != nil {
				fmt.Printf("Failed to save block to PostgreSQL: %v\n", err)
				return
			}

			fmt.Println("Block saved to PostgreSQL")

			// Mark block as NULL in Kafka
			// combination of sync and async

			if async {
				go markBlockAsNullAsync(consumer)
			} else {
				markBlockAsNullSync(consumer)
			}

			numbers = nil
		}
	}

	consumer.ConsumeMessages(handler)
}

func saveBlockToPostgres(db *sql.DB, numbers []string) error {
	if len(numbers) == 0 {
		fmt.Println("No data to save")
		return nil
	}

	// Check for empty or NULL values
	var values []string
	for _, num := range numbers {
		if num == "" {
			return fmt.Errorf("empty value found in the block")
		}
		values = append(values, fmt.Sprintf("('%s')", num))
	}

	// Construct SQL query
	query := fmt.Sprintf("INSERT INTO schema_number.numbers (value) VALUES %s", strings.Join(values, ","))

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute SQL query
	_, err = tx.Exec(query)
	if err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("Saved block of numbers to PostgreSQL")
	return nil
}

func markBlockAsNullSync(consumer consumer.MessageConsumer) {
	producer := consumer.GetKafkaProducer()
	defer producer.Close()

	for i := 0; i < blockSize; i++ {
		// Produce NULL message to Kafka for each block element
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte("NULL"),
		}, nil)
	}
}

func markBlockAsNullAsync(consumer consumer.MessageConsumer) {
	producer := consumer.GetKafkaProducer()
	defer producer.Close()

	for i := 0; i < blockSize; i++ {
		// Produce NULL message to Kafka for each block element asynchronously
		go producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte("NULL"),
		}, nil)
	}
}
func main() {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB))
	if err != nil {
		fmt.Printf("Failed to connect to PostgreSQL: %v\n", err)
		return
	}
	defer db.Close()

	if err := checkPostgresConnection(); err != nil {
		fmt.Printf("Error connecting to PostgreSQL: %v\n", err)
		return
	}

	// Start Kafka consumer
	consumers, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          "kafka-poc-1",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %v\n", err)
		return
	}
	defer consumers.Close()

	consumers.SubscribeTopics([]string{kafkaTopic}, nil)

	// Channel to receive messages from the consumer
	resultCh := make(chan string)

	// Create a Kafka consumer instance
	kafkaConsumer := consumer.NewKafkaConsumer(consumers, kafkaTopic)

	go consumeAndSaveNumbers(resultCh, db, kafkaConsumer)

	// Print inserted values
	go func() {
		for val := range resultCh {
			fmt.Printf("Accumulated data for save: %s\n", val)
		}
	}()

	// Wait for interrupt signal to gracefully shut down
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	fmt.Println("Consumer shutting down...")
}
