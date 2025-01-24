package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

type Cpt struct {
	ChargeMasterID int    `json:"charge_master_id"`
	ClientProcDesc string `json:"client_proc_desc"`
}

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		panic(err)
	}
	var data []Cpt
	repo := squealx.New[map[string]any](db, "charge_master", "charge_master_id")
	fmt.Println(repo.Paginate(context.Background(), squealx.Paging{Limit: 1}).Items)
	err = db.Select(&data, "SELECT * FROM charge_master WHERE charge_master_id = :id LIMIT 1", map[string]any{
		"id": 943843,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(data)
}
