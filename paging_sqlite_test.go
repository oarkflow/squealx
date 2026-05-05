package squealx

import (
	"testing"

	_ "modernc.org/sqlite"
)

type pagingSQLiteUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func TestPaginateTypedSQLiteUsesLimitThenOffset(t *testing.T) {
	db, err := Open("sqlite", ":memory:", "paging-sqlite-test")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);

		INSERT INTO users (id, name)
		VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
	`)

	response := PaginateTyped[pagingSQLiteUser](
		db,
		"SELECT * FROM users",
		Paging{Page: 1, Limit: 2, OrderBy: []string{"id asc"}},
	)
	if response.Error != nil {
		t.Fatalf("paginate: %v", response.Error)
	}
	if len(response.Items) != 2 {
		t.Fatalf("expected 2 items, got %#v", response.Items)
	}
	if response.Items[0].ID != 1 || response.Items[1].ID != 2 {
		t.Fatalf("unexpected page items: %#v", response.Items)
	}
	if response.Pagination.TotalRecords != 3 || response.Pagination.TotalPage != 2 {
		t.Fatalf("unexpected pagination metadata: %#v", response.Pagination)
	}
}
