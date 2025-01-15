package main

import (
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
	data, err := squealx.SelectTyped[Cpt](db, "SELECT * FROM charge_master LIMIT 1")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
