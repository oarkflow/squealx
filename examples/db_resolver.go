package main

import (
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
)

func main() {
	// DSNs
	primaryDSN := "host=localhost user=postgres password=postgres dbname=arrrange sslmode=disable"
	secondaryDSN := "host=localhost user=postgres password=postgres dbname=arrrange sslmode=disable"

	// connect to primary
	primaryDB := squealx.MustOpen("pgx", primaryDSN)
	// connect to secondary
	secondaryDB := squealx.MustOpen("pgx", secondaryDSN)

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
	var users []Person
	err := resolver.NamedSelect(&users, `SELECT * FROM person WHERE first_name = :first_name`, Person{FirstName: "John"})
	// err := resolver.SelectContext(context.Background(), &users, `SELECT * FROM users WHERE name = :name`, User{Name: "foo"})
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
