package sqlite

import (
	"context"
	"testing"

	"github.com/oarkflow/squealx"
)

type ptrRow struct {
	ID   int64   `db:"id"`
	Name *string `db:"name"`
	Age  *int64  `db:"age"`
}

func setupPtrDB(t *testing.T) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", "alias-test")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
		INSERT INTO p VALUES (1,'a',10),(2,NULL,NULL),(3,'c',30);
	`); err != nil {
		t.Fatal(err)
	}
	return db
}

func checkPtrRows(t *testing.T, rows []ptrRow) {
	t.Helper()
	if len(rows) != 3 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Name == nil || *rows[0].Name != "a" || rows[0].Age == nil || *rows[0].Age != 10 {
		t.Fatalf("row0 corrupted by later rows: %+v", rows[0])
	}
	if rows[1].Name != nil || rows[1].Age != nil {
		t.Fatalf("row1 must keep NULL as nil (no stale pointers): %+v", rows[1])
	}
	if rows[2].Name == nil || *rows[2].Name != "c" || rows[2].Age == nil || *rows[2].Age != 30 {
		t.Fatalf("row2: %+v", rows[2])
	}
}

// Retained value rows must never alias later rows or leak stale pointers.
func TestSelectEachRetainedRowsDoNotAlias(t *testing.T) {
	db := setupPtrDB(t)
	var rows []ptrRow
	err := squealx.SelectEach(db, func(r ptrRow) error { rows = append(rows, r); return nil },
		"SELECT id,name,age FROM p ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	checkPtrRows(t, rows)
}

func TestCursorRetainedValueRowsDoNotAlias(t *testing.T) {
	db := setupPtrDB(t)
	c, err := squealx.QueryCursor[ptrRow](context.Background(), db, "SELECT id,name,age FROM p ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var rows []ptrRow
	for c.Next() {
		rows = append(rows, c.Value())
	}
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
	checkPtrRows(t, rows)
}

func TestSelectSliceLenientPointers(t *testing.T) {
	db := setupPtrDB(t)
	var rows []ptrRow
	if err := db.Select(&rows, "SELECT id,name,age FROM p ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	checkPtrRows(t, rows)
}
