package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/sqlite"
)

type Event struct {
	ID      int64  `db:"id"`
	Payload string `db:"payload"`
}

func main() {
	db, err := sqlite.Open(":memory:", "cursor-example")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.ApplyPoolConfig(squealx.PoolConfig{MaxOpenConns: 4, MaxIdleConns: 4}); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO events(payload) VALUES ('created'), ('validated'), ('published');
	`); err != nil {
		log.Fatal(err)
	}

	cursor, err := squealx.QueryCursorConfig[Event](
		context.Background(), db,
		squealx.CursorConfig{MaxRows: 10_000},
		"SELECT id, payload FROM events WHERE id > ? ORDER BY id", 0,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close()

	for cursor.Next() {
		event := cursor.Value() // reused O(1)-memory row storage
		fmt.Printf("%d %s\n", event.ID, event.Payload)
	}
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stats: %+v\n", cursor.Stats())
}
