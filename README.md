Kafka POC project:
---
# Kafka POC
This is a proof-of-concept (POC) project demonstrating the integration of Apache Kafka with a Go application for message production and consumption.
## Prerequisites
Before running the project, make sure you have the following installed:

Go programming language
Apache Kafka
PostgreSQL (if using PostgreSQL in the project)
## Setup

Clone the repository to your local machine:
    ```bash
    git clone https://github.com/Nikita-polaris/Kafka-poc.git
    ```

Navigate to the project directory:
    ```bash
    cd kafka-poc
    ```

Install dependencies:
    ```bash
    go mod tidy
    ```

Set up your environment variables by creating a `.env` file in the project root directory. Here's an example of the required environment variables:
    ```
    KAFKA_BROKERS=your_kafka_broker_address
    KAFKA_TOPIC=your_kafka_topic_name
    POSTGRES_HOST=your_postgres_host
    POSTGRES_PORT=your_postgres_port
    POSTGRES_USER=your_postgres_user
    POSTGRES_PASSWORD=your_postgres_password
    POSTGRES_DB=your_postgres_database
    ```

Run the Kafka producer:
    ```bash
    go run producer-main.go
    ```

Run the Kafka consumer:
    ```bash
    go run consumer-main.go
    ```
## Usage
Once the producer and consumer are running, the producer will generate messages and send them to the Kafka topic specified in the environment variables. The consumer will consume these messages and save them to the PostgreSQL database.
