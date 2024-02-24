package main

import (
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/squealx"
)

func main() {
	var users []map[string]any
	db, err := squealx.Connect("pgx", "host=localhost user=postgres password=postgres dbname=tests sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	pagination, err := squealx.Pages(&squealx.Param{
		DB:    db,
		Query: "SELECT * FROM users",
		Paging: &squealx.Paging{
			Limit: 10,
			Page:  0,
		},
	}, &users)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(pagination)
}
