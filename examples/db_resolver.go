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
	primaryDSN := "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
	secondaryDSN := "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"

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

	type User struct {
		Name string
	}
	var users []User
	resolver.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	err := resolver.Select(&users, `SELECT * FROM users WHERE name = :name`, User{Name: "foo"})
	// err := resolver.SelectContext(context.Background(), &users, `SELECT * FROM users WHERE name = :name`, User{Name: "foo"})
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
