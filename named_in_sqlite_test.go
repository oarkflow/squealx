package squealx

import (
	"testing"

	_ "modernc.org/sqlite"
)

type namedInSQLiteUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func setupNamedInSQLiteDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open("sqlite", ":memory:", "named-in-sqlite-test")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close sqlite db: %v", err)
		}
	})

	if _, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO users (id, name)
		VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')
	`); err != nil {
		t.Fatalf("insert users: %v", err)
	}

	return db
}

func TestNamedSelectInPlaceholderWithoutParenthesesSQLite(t *testing.T) {
	db := setupNamedInSQLiteDB(t)

	var users []namedInSQLiteUser
	err := db.NamedSelect(&users, `
		SELECT id, name
		FROM users
		WHERE id IN :ids
		ORDER BY id
	`, map[string]any{
		"ids": []int{1, 3},
	})
	if err != nil {
		t.Fatalf("NamedSelect with IN :ids failed: %v", err)
	}

	if len(users) != 2 || users[0].ID != 1 || users[1].ID != 3 {
		t.Fatalf("unexpected users for IN :ids: %#v", users)
	}
}

func TestNamedSelectInPlaceholderWithParenthesesSQLite(t *testing.T) {
	db := setupNamedInSQLiteDB(t)

	var users []namedInSQLiteUser
	err := db.NamedSelect(&users, `
		SELECT id, name
		FROM users
		WHERE id IN (:ids)
		ORDER BY id
	`, map[string]any{
		"ids": []int{1, 3},
	})
	if err != nil {
		t.Fatalf("NamedSelect with IN (:ids) failed: %v", err)
	}

	if len(users) != 2 || users[0].ID != 1 || users[1].ID != 3 {
		t.Fatalf("unexpected users for IN (:ids): %#v", users)
	}
}
