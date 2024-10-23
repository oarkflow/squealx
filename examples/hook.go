package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/hooks"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear sslmode=disable"
	db, err := postgres.Open(masterDSN, "id")
	if err != nil {
		log.Fatal(err)
	}
	db.Use(hooks.NewLogger(true, 20*time.Millisecond))
	callback := func(row map[string]any) error {
		fmt.Println(row)
		return nil
	}
	runner := squealx.LazySelectEach(db, callback, `SELECT * FROM charge_master WHERE client_internal_code LIKE '%763' LIMIT 10`)
	if err := runner(); err != nil {
		log.Fatal(err)
	}

}
