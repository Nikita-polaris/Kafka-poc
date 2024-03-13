package postgresConn

import (
    "database/sql"
    "fmt"

    _ "github.com/lib/pq"
)

const (
    postgresHost     = "localhost"
    postgresPort     = 5432
    postgresUser     = "kafka_user"
    postgresPassword = "password"
    postgresDB       = "kafkadb"
)

func ConnectToPostgres() (*sql.DB, error) {
    // Construct the connection string
    connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB)

    // Open a connection to the PostgreSQL database
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
    }

    // Attempt to ping the database
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to ping the database: %v", err)
    }

    fmt.Println("Successfully connected to PostgreSQL database.")
    return db, nil
}