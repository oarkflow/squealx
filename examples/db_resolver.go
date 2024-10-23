package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear sslmode=disable"
	masterDB := postgres.MustOpen(masterDSN, "master")
	resolver, err := dbresolver.New(dbresolver.WithMasterDBs(masterDB))
	if err != nil {
		panic(err)
	}
	defer resolver.Close()
	cm := resolver.LazySelect("SELECT * FROM charge_master LIMIT 10")
	var data []map[string]any
	err = cm(&data)
	// err = squealx.SelectEach(masterDB, callback, "SELECT * FROM charge_master LIMIT 10")
	// dataResolver := squealx.LazySelect[map[string]any](masterDB, "SELECT * FROM charge_master WHERE cpt_hcpcs_code LIKE :cpt_hcpcs_code LIMIT 10")
	// data, err := dataResolver(map[string]any{"cpt_hcpcs_code": "IC53%"})
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(data)
}

func callback(data map[string]any) error {
	fmt.Println(data)
	return nil
}
