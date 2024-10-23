package main

import (
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/squealx/drivers/postgres"
)

var schema = `
CREATE TABLE person (
    first_name text,
    last_name text,
    email text
);

CREATE TABLE place (
    country text,
    city text NULL,
    telcode integer
)`

type Person struct {
	ID        int
	FirstName string
	LastName  string
	Email     string
}

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=sujit sslmode=disable", "test")
	if err != nil {
		log.Fatalln(err)
	}
	row := map[string]any{
		"first_name": "Anita",
		"last_name":  "Baker",
		"email":      "anita.baker@example.com",
	}
	query := `INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)`
	// err = db.Select(&row, query, row)
	err = db.ExecWithReturn(query, &row)
	fmt.Printf("Inserted person: %+v\n", row)
}
