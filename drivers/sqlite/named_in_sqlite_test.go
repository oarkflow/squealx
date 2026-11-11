package sqlite

import (
	"fmt"
	"sync"
	"testing"

	"github.com/oarkflow/squealx"
)

type namedInSQLiteUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func setupNamedInSQLiteDB(t *testing.T) *squealx.DB {
	t.Helper()

	db, err := Open(":memory:", "named-in-sqlite-test")
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

func TestNamedSelectInDoesNotMutateArgumentMapOrClobberNamesSQLite(t *testing.T) {
	db := setupNamedInSQLiteDB(t)
	args := map[string]any{"ids": []int{1, 3}, "__squealx_in_0": 2}
	var users []namedInSQLiteUser
	err := db.NamedSelect(&users, `
		SELECT id, name
		FROM users
		WHERE id = :__squealx_in_0 OR id IN (:ids)
		ORDER BY id
	`, args)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 3 || users[0].ID != 1 || users[1].ID != 2 || users[2].ID != 3 {
		t.Fatalf("unexpected rows: %#v", users)
	}
	if len(args) != 2 || args["__squealx_in_0"] != 2 {
		t.Fatalf("argument map was changed: %#v", args)
	}
	if ids, ok := args["ids"].([]int); !ok || len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("slice argument changed: %#v", args["ids"])
	}
}

func TestNamedSelectEmptyInSliceSQLite(t *testing.T) {
	db := setupNamedInSQLiteDB(t)
	var users []namedInSQLiteUser
	if err := db.NamedSelect(&users, "SELECT id,name FROM users WHERE id IN (:ids)", map[string]any{"ids": []int{}}); err != nil {
		t.Fatal(err)
	}
	if len(users) != 0 {
		t.Fatalf("empty IN slice returned rows: %#v", users)
	}
}

func TestNamedSelectConcurrentSharedSliceArgsSQLite(t *testing.T) {
	db := setupNamedInSQLiteDB(t)
	db.SetMaxOpenConns(1)
	args := map[string]any{"ids": []int{1, 3}}
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for worker := 0; worker < cap(errCh); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				var users []namedInSQLiteUser
				if err := db.NamedSelect(&users, "SELECT id,name FROM users WHERE id IN (:ids) ORDER BY id", args); err != nil {
					errCh <- err
					return
				}
				if len(users) != 2 || users[0].ID != 1 || users[1].ID != 3 {
					errCh <- fmt.Errorf("unexpected rows: %#v", users)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if len(args) != 1 {
		t.Fatalf("shared arguments were mutated: %#v", args)
	}
}
