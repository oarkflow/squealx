package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/oarkflow/squealx/drivers/cqlite"
)

func main() {
	if err := run(":memory:", "quickstart-memory"); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(os.TempDir(), "squealx-cqlite-quickstart.db")
	_ = os.Remove(path)
	defer os.Remove(path)
	if err := run(path, "quickstart-file"); err != nil {
		log.Fatal(err)
	}
}

func run(dsn, id string) error {
	db, err := cqlite.Open(dsn, id)
	if err != nil {
		return err
	}
	defer db.Close()
	if dsn == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
		return err
	}
	if _, err = db.NamedExecContext(ctx, `INSERT INTO notes(body) VALUES (:body)`, map[string]any{"body": "hello cqlite"}); err != nil {
		return err
	}
	var body string
	if err = db.GetContext(ctx, &body, `SELECT body FROM notes WHERE id = ?`, 1); err != nil {
		return err
	}
	fmt.Printf("%s (%s): %s\n", id, dsn, body)
	return nil
}
