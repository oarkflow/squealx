package main

import (
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		panic(err)
	}
	loader, err := squealx.LoadFromFile("queries.sql")
	if err != nil {
		panic(err)
	}
	var dest []map[string]any
	err = loader.Select(db, &dest, "list-cpt", map[string]any{
		"work_item_id": 33,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(dest)
}
