package sqlite

import (
	_ "modernc.org/sqlite"

	"github.com/oarkflow/squealx"
)

// Open - sqlite.db
func Open(dsn string) (*squealx.DB, error) {
	return squealx.Connect("sqlite", dsn)
}

func MustOpen(dsn string) *squealx.DB {
	db, err := Open(dsn)
	if err != nil {
		panic(err)
	}
	return db
}
