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

type User struct {
	UserID    int
	Username  string
	FirstName string
	LastName  string
	Email     string
}

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=oark_manager sslmode=disable", "test")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	var users []User
	err = db.Select(&users, "SELECT * FROM users")
	if err != nil {
		panic(err)
	}
	fmt.Println(users)
}
