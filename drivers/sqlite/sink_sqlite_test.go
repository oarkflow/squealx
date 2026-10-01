package sqlite

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/oarkflow/squealx"
)

func setupSinkDB(t *testing.T) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", "sink-test")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE s (id INTEGER PRIMARY KEY, name TEXT, data BLOB, score REAL);
		INSERT INTO s VALUES (1,'Alice',x'6869',1.5),(2,NULL,x'fffe',NULL),(3,'a,"b"',NULL,2);
	`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWriteJSONLines(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id,name,data,score FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := squealx.WriteJSONLines(context.Background(), &buf, rows)
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	want := []string{
		`{"id":1,"name":"Alice","data":"hi","score":1.5}`,
		`{"id":2,"name":null,"data":"//4=","score":null}`,
		`{"id":3,"name":"a,\"b\"","data":null,"score":2}`,
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("line %d: %s want %s", i, lines[i], want[i])
		}
	}
}

func TestWriteCSV(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id,name FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := squealx.WriteCSV(context.Background(), &buf, rows, true)
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	want := "id,name\n1,Alice\n2,\n3,\"a,\"\"b\"\"\"\n"
	if buf.String() != want {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteCancelled(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id FROM s")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	if _, err := squealx.WriteJSONLines(ctx, &buf, rows); err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestWriteMapCursorJSONLines(t *testing.T) {
	db := setupSinkDB(t)
	c, err := squealx.QueryCursor[map[string]any](context.Background(), db, "SELECT id,name FROM s WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := squealx.WriteMapCursorJSONLines(context.Background(), &buf, c)
	if err != nil || n != 1 || strings.TrimSpace(buf.String()) != `{"id":1,"name":"Alice"}` {
		t.Fatalf("n=%d err=%v out=%q", n, err, buf.String())
	}
}

func TestRawColumn(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id,data FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	rows.Next()
	r, ok, err := squealx.RawColumn(rows, 1)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	var b bytes.Buffer
	b.ReadFrom(r)
	if b.String() != "hi" {
		t.Fatalf("got %q", b.String())
	}
}
