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
	if err := run(":memory:", "features-memory"); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(os.TempDir(), "squealx-cqlite-features.db")
	_ = os.Remove(path)
	defer os.Remove(path)
	if err := run(path, "features-file"); err != nil {
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
	statements := []string{
		`CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, metadata TEXT)`,
		`INSERT INTO articles VALUES (1, 'Squealx and cqlite', '{"kind":"guide","published":true}')`,
	}
	for _, statement := range statements {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	var kind string
	if err = db.GetContext(ctx, &kind, `SELECT json_extract(metadata, '$.kind') FROM articles WHERE json_extract(metadata, '$.published') = ?`, true); err != nil {
		return err
	}
	fmt.Printf("%s (%s), JSON kind: %s\n", id, dsn, kind)
	return nil
}
