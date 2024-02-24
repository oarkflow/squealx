package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	var work_items []map[string]any
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear sslmode=disable")
	if err != nil {
		panic(err)
	}
	sql := "SELECT * FROM work_items"
	pg := squealx.Paginate(db, sql, &work_items, squealx.Paging{
		Limit: 1,
		Page:  0,
	})
	if pg.Error != nil {
		log.Fatalln(pg.Error)
	}
	fmt.Println(pg.Items)

	pagination, err := squealx.Pages(&squealx.Param{
		DB:    db,
		Query: sql,
		Paging: &squealx.Paging{
			Limit: 1,
			Page:  0,
		},
	}, &work_items)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(work_items, pagination)
}
