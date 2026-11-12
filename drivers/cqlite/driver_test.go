package cqlite

import (
	"context"
	"database/sql"
	"github.com/oarkflow/squealx"
	"path/filepath"
	"testing"
)

func TestSQLDriverQueriesTransactionsAndSharedFileEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter.db")
	first, err := Open(path, "first")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path, "second")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := first.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := first.NamedExec(`INSERT INTO items(value) VALUES (:value)`, map[string]any{"value": "named"}); err != nil {
		t.Fatal(err)
	}
	tx, err := second.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO items(value) VALUES (?)`, "transaction"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := first.Get(&count, `SELECT COUNT(*) FROM items`); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

func TestSQLOpenRegistersDriverAndBindsQuestionParameters(t *testing.T) {
	db, err := sql.Open("cqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got int
	if err := db.QueryRow(`SELECT ?`, 42).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	if got := squealx.BindType("cqlite"); got != squealx.QUESTION {
		t.Fatalf("bind type = %d, want QUESTION", got)
	}
}
