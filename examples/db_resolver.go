package main

import (
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

type ChargeMaster struct {
	ClientProcDesc string `json:"client_proc_desc"`
}

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable"
	db := postgres.MustOpen(masterDSN, "master")
	defer db.Close()
	response := squealx.PaginateTyped[map[string]any](db, "SELECT * FROM charge_master", squealx.Paging{Page: 0, Limit: 5})
	fmt.Println(response.Items)
	fmt.Println(response.Pagination)
}

func callback(data map[string]any) error {
	fmt.Println(data)
	return nil
}
