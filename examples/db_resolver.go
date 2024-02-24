package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {

	// DSNs
	primaryDSN := "host=localhost user=postgres password=postgres dbname=arrrange sslmode=disable"
	secondaryDSN := "host=localhost user=postgres password=postgres dbname=arrrange sslmode=disable"

	// connect to primary
	primaryDB := postgres.MustOpen(primaryDSN)
	// connect to secondary
	secondaryDB := postgres.MustOpen(secondaryDSN)

	primaryDBsCfg := &dbresolver.PrimaryDBsConfig{
		DBs:             []*squealx.DB{primaryDB},
		ReadWritePolicy: dbresolver.ReadWrite,
	}
	resolver := dbresolver.MustNewDBResolver(primaryDBsCfg, dbresolver.WithSecondaryDBs(secondaryDB))
	defer resolver.Close()
	type Person struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
		Email     string
	}
	var users []map[string]any
	err := resolver.Select(&users, `SELECT * FROM person WHERE first_name IN (:first_name)`, map[string]any{"first_name": []string{"John", "Bin"}})
	// err := resolver.SelectContext(context.Background(), &users, `SELECT * FROM users WHERE name = :name`, User{Name: "foo"})
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
