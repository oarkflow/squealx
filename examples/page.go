package main

import (
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	var data []map[string]any
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		panic(err)
	}
	sq, err := squealx.LoadFromFile("queries.sql")
	if err != nil {
		panic(err)
	}
	for key, val := range sq.Queries() {
		fmt.Println(key, val)
	}
	rows, err := sq.NamedQuery(db, "list-cpt", map[string]any{
		"work_item_id": "33",
	})
	if err != nil {
		panic(err)
	}
	err = squealx.ScannAll(rows, &data, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
