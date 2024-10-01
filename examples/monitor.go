package main

import (
	"fmt"

	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/monitor"
)

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		panic(err)
	}
	data, err := monitor.GetPostgresStats(db)
	if err != nil {
		panic(err)
	}
	for key, val := range data {
		fmt.Println(key)
		fmt.Println(val)
	}
}
