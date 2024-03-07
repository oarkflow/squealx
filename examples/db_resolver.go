package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
	replicaDSN := "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
	masterDB := postgres.MustOpen(masterDSN)
	replicaDB := postgres.MustOpen(replicaDSN)
	masterDBsCfg := &dbresolver.MasterConfig{
		DBs:             []*squealx.DB{masterDB},
		ReadWritePolicy: dbresolver.ReadWrite,
	}
	sq, err := squealx.LoadFromFile("queries.sql")
	if err != nil {
		panic(err)
	}
	resolver := dbresolver.MustNewDBResolver(masterDBsCfg, dbresolver.WithReplicaDBs(replicaDB), dbresolver.WithFileLoader(sq))
	defer resolver.Close()
	var users []map[string]any
	err = resolver.Select(&users, "list-persons", map[string]any{"first_name": []string{"John", "Bin"}})
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
