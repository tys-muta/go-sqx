package sqlite

import (
	"database/sql"
	"fmt"
	"os"
)

func createDatabase(dbFile string) (*sql.DB, error) {
	if err := os.RemoveAll(dbFile); err != nil {
		return nil, fmt.Errorf("failed to remove db file: %w", err)
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection with database")
	}

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("failed to enable foreign key: %w", err)
	}

	return db, nil
}
