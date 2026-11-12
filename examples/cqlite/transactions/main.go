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
	if err := run(":memory:", "transactions-memory"); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(os.TempDir(), "squealx-cqlite-transactions.db")
	_ = os.Remove(path)
	defer os.Remove(path)
	if err := run(path, "transactions-file"); err != nil {
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
	if _, err = db.ExecContext(ctx, `CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)`); err != nil {
		return err
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO ledger(amount) VALUES (?)`, 25); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	var total int
	if err = db.GetContext(ctx, &total, `SELECT SUM(amount) FROM ledger`); err != nil {
		return err
	}
	fmt.Printf("%s (%s), committed total: %d\n", id, dsn, total)
	return nil
}
