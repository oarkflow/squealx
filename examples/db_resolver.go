package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	mysqlCheck()
	// postgresCheck()
}

func mysqlCheck() {
	masterDSN := "root:T#sT1234@tcp(localhost:3306)/tests"
	db := mysql.MustOpen(masterDSN)
	var users map[string]any
	err := db.Get(&users, "SELECT * FROM users LIMIT 1")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}

func postgresCheck() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
	db := postgres.MustOpen(masterDSN)
	var users []map[string]any
	err := db.Select(&users, "SELECT * FROM person LIMIT 1")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(users)
}
