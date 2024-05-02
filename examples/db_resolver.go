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
	masterDSN := "root:root@tcp(localhost:3306)/eamitest"
	db := mysql.MustOpen(masterDSN)
	var fields []MIndices
	err := db.Select(&fields, `SELECT INDEX_NAME AS name, NON_UNIQUE as uniq, CONCAT('[', GROUP_CONCAT(CONCAT('"',COLUMN_NAME,'"') ORDER BY SEQ_IN_INDEX) ,']') AS columns FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name GROUP BY INDEX_NAME, NON_UNIQUE;`, map[string]any{
		"schema":     "eamitest",
		"table_name": "attendingPhysicianADTIn",
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
