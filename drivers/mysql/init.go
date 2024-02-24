package mysql

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/oarkflow/squealx"
)

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
