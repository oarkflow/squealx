package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx/datatypes"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	mysqlCheck()
	// postgresCheck()
}

type MIndices struct {
	Name    string                `json:"name" gorm:"column:name"`
	Unique  bool                  `json:"unique" gorm:"column:uniq"`
	Columns datatypes.StringArray `json:"columns" gorm:"type:text[] column:columns"`
}

func mysqlCheck() {
	masterDSN := "root:T#sT1234@tcp(localhost:3306)/datav"
	db := mysql.MustOpen(masterDSN)
	var fields []map[string]any
	err := db.Select(&fields, `SELECT * FROM datasource WHERE team_id = :team_id`, map[string]any{
		"team_id": 1,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(fields)
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
