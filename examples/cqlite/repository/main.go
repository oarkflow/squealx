package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/cqlite"
)

type Task struct {
	ID    int64  `db:"id"`
	Title string `db:"title"`
	Done  bool   `db:"done"`
}

func (Task) TableName() string  { return "tasks" }
func (Task) PrimaryKey() string { return "id" }

func main() {
	if err := run(":memory:", "repository-memory"); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(os.TempDir(), "squealx-cqlite-repository.db")
	_ = os.Remove(path)
	defer os.Remove(path)
	if err := run(path, "repository-file"); err != nil {
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
	if _, err = db.Exec(`CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN NOT NULL)`); err != nil {
		return err
	}
	repo := squealx.New[Task](db, "tasks", "id")
	ctx := context.Background()
	// Insert with SQL so this example does not need a separate metadata provider;
	// the repository performs typed reads on the same database.
	if _, err = db.ExecContext(ctx, `INSERT INTO tasks(title, done) VALUES (?, ?)`, "Ship cqlite adapter", false); err != nil {
		return err
	}
	tasks, err := repo.All(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%s (%s): %+v\n", id, dsn, tasks)
	return nil
}
