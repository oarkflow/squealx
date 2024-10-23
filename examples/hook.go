package main

import (
	"fmt"
	log2 "log"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/hooks"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear sslmode=disable"
	db, err := postgres.Open(masterDSN, "id")
	if err != nil {
		log2.Fatal(err)
	}
	db.Use(hooks.NewLogger(nil, true, 4*time.Millisecond, func(query string, args []any, latency string) {
		fmt.Println("Slow Query detected:", query, args, latency)
	}))
	callback := func(row map[string]any) error {
		fmt.Println(row)
		return nil
	}
	runner := squealx.LazySelectEach(db, callback, `SELECT * FROM charge_master WHERE client_internal_code LIKE '%763' LIMIT 10`)
	if err := runner(); err != nil {
		log2.Fatal(err)
	}

}
