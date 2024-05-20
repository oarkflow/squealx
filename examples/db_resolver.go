package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable"
	replicaDSN := "host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable"
	masterDB := postgres.MustOpen(masterDSN, "master")
	replicaDB := postgres.MustOpen(replicaDSN, "replica")
	masterDBsCfg := &dbresolver.MasterConfig{
		DBs:             []*squealx.DB{masterDB},
		ReadWritePolicy: dbresolver.ReadWrite,
	}
	resolver := dbresolver.MustNewDBResolver(masterDBsCfg, dbresolver.WithReplicaDBs(replicaDB))
	defer resolver.Close()
	var users []map[string]any
	err := resolver.Select(&users, "SELECT * FROM charge_master LIMIT 10")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
