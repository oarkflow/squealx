package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func connectDB() (*squealx.DB, error) {
	connStr := "user=postgres password=postgres dbname=clear_dev port=5432 sslmode=disable"
	db, err := postgres.Open(connStr, "postgres")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func main() {
	db, err := connectDB()
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()
	repo := squealx.New[map[string]any](db, "modifiers", "modifier_id")
	data, err := repo.Count(context.Background(), map[string]any{
		"modifier_id": 2,
		"deleted_at":  nil,
	})
	fmt.Println(data, err)
}
