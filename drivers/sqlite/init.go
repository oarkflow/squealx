package sqlite

import (
	_ "modernc.org/sqlite"

	"github.com/oarkflow/squealx"
)

// Open - sqlite.db
func Open(dsn string, id string) (*squealx.DB, error) {
	return squealx.Connect("sqlite", dsn, id)
}

func MustOpen(dsn string, id string) *squealx.DB {
	db, err := Open(dsn, id)
	if err != nil {
		panic(err)
	}
	return db
}
