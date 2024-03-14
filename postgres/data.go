package postgresConn

import (
	"database/sql"
	"fmt"
)

var (
	schemaName = "schema_number"
	blockSize  = 100
)

func SaveBlockToPostgres(db *sql.DB, numbers []string) error {
	if len(numbers) == 0 {
		fmt.Println("No data to save")
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO schema_number.numbers (value) VALUES ($1)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, num := range numbers {
		_, err := stmt.Exec(num)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	fmt.Println("Saved block of numbers to PostgreSQL")
	return nil
}
