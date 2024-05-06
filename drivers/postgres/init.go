package postgres

import (
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/squealx"
)

// Open - "host=localhost user=postgres password=postgres dbname=sujit sslmode=disable"
func Open(dsn string, id string) (*squealx.DB, error) {
	return squealx.Connect("pgx", dsn, id)
}

func MustOpen(dsn string, id string) *squealx.DB {
	db, err := Open(dsn, id)
	if err != nil {
		panic(err)
	}
	return db
}
