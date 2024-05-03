package mysql

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/oarkflow/squealx"
)

// Open
/*
root:T#sT1234@tcp(localhost:3306)/datav
*/
func Open(dsn string) (*squealx.DB, error) {
	return squealx.Connect("mysql", dsn)
}

func MustOpen(dsn string) *squealx.DB {
	db, err := Open(dsn)
	if err != nil {
		panic(err)
	}
	return db
}
