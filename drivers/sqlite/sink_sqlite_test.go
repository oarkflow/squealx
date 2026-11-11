package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/oarkflow/squealx"
)

func setupSinkDB(t testing.TB) *squealx.DB {
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

func TestStreamingWritersAcceptNilContext(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var jsonOutput bytes.Buffer
	if n, err := squealx.WriteJSONLines(nil, &jsonOutput, rows); err != nil || n != 3 {
		t.Fatalf("JSON lines n=%d err=%v", n, err)
	}

	rows, err = db.Queryx("SELECT id FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var csvOutput bytes.Buffer
	if n, err := squealx.WriteCSV(nil, &csvOutput, rows, false); err != nil || n != 3 {
		t.Fatalf("CSV n=%d err=%v", n, err)
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
	if buf.Len() != 0 {
		t.Fatalf("cancelled writer consumed a row: %q", buf.String())
	}

	rows, err = db.Queryx("SELECT id,name FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if _, err := squealx.WriteCSV(ctx, &buf, rows, false); err == nil {
		t.Fatal("expected CSV cancellation error")
	}
	if buf.Len() != 0 {
		t.Fatalf("cancelled CSV writer consumed a row: %q", buf.String())
	}

	cursor, err := squealx.QueryCursor[map[string]any](context.Background(), db, "SELECT id,name FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if _, err := squealx.WriteMapCursorJSONLines(ctx, &buf, cursor); err == nil {
		t.Fatal("expected cursor cancellation error")
	}
	if buf.Len() != 0 {
		t.Fatalf("cancelled cursor writer consumed a row: %q", buf.String())
	}
}

func TestWriteMapCursorJSONLines(t *testing.T) {
	db := setupSinkDB(t)
	c, err := squealx.QueryCursor[map[string]any](context.Background(), db, "SELECT id,name,data,score FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := squealx.WriteMapCursorJSONLines(context.Background(), &buf, c)
	want := "{\"data\":\"hi\",\"id\":1,\"name\":\"Alice\",\"score\":1.5}\n" +
		"{\"data\":\"��\",\"id\":2,\"name\":null,\"score\":null}\n" +
		"{\"data\":null,\"id\":3,\"name\":\"a,\\\"b\\\"\",\"score\":2}\n"
	if err != nil || n != 3 || buf.String() != want {
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

func TestRawColumnScannerReusesTargets(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT id,data FROM s ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	scanner, err := squealx.NewRawColumnScanner(rows, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{[]byte("hi"), {0xff, 0xfe}, nil}
	for i := range want {
		if !rows.Next() {
			t.Fatalf("row %d missing: %v", i, rows.Err())
		}
		got, ok, err := scanner.Scan(rows)
		if err != nil {
			t.Fatal(err)
		}
		if ok != (want[i] != nil) || !bytes.Equal(got, want[i]) {
			t.Fatalf("row %d: bytes=%v ok=%v want=%v", i, got, ok, want[i])
		}
	}
	if rows.Next() {
		t.Fatal("unexpected extra row")
	}
}

func TestRawColumnScannerCloseReleasesLifecycle(t *testing.T) {
	db := setupSinkDB(t)
	rows, err := db.Queryx("SELECT data FROM s WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("row missing: %v", rows.Err())
	}
	scanner, err := squealx.NewRawColumnScanner(rows, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, ok, err := scanner.Scan(rows)
	if err != nil || !ok || !bytes.Equal(data, []byte("hi")) {
		t.Fatalf("data=%q ok=%t err=%v", data, ok, err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, _, err := scanner.Scan(rows); !errors.Is(err, squealx.ErrRawColumnScannerClosed) {
		t.Fatalf("Scan after Close error = %v", err)
	}
}

const rawColumnManyRowsQuery = `WITH RECURSIVE seq(id) AS (
	SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 1000
) SELECT CAST('payload' AS BLOB) AS data FROM seq`

func BenchmarkRawColumnOneOff1000(b *testing.B) {
	db := setupSinkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Queryx(rawColumnManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			r, ok, err := squealx.RawColumn(rows, 0)
			if err != nil || !ok {
				_ = rows.Close()
				b.Fatalf("raw column ok=%v err=%v", ok, err)
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawColumnScanner1000(b *testing.B) {
	db := setupSinkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Queryx(rawColumnManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		scanner, err := squealx.NewRawColumnScanner(rows, 0)
		if err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		for rows.Next() {
			data, ok, err := scanner.Scan(rows)
			if err != nil || !ok {
				_ = rows.Close()
				b.Fatalf("raw column ok=%v err=%v", ok, err)
			}
			_ = len(data)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawColumnScannerWide1000(b *testing.B) {
	db := setupSinkDB(b)
	for _, width := range []int{1, 16, 64, 128} {
		b.Run(strconv.Itoa(width), func(b *testing.B) {
			var query strings.Builder
			query.WriteString(`WITH RECURSIVE seq(id) AS (
				SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 1000
			) SELECT `)
			for col := 0; col < width; col++ {
				if col > 0 {
					query.WriteByte(',')
				}
				if col == width/2 {
					query.WriteString(`CAST('payload' AS BLOB)`)
				} else {
					query.WriteString(`id`)
				}
			}
			query.WriteString(` FROM seq`)
			b.Run("ReusableDiscard", func(b *testing.B) {
				benchmarkRawColumnWidth(b, db, query.String(), width, true)
			})
			b.Run("PerColumnClear", func(b *testing.B) {
				benchmarkRawColumnWidth(b, db, query.String(), width, false)
			})
		})
	}
}

func benchmarkRawColumnWidth(b *testing.B, db *squealx.DB, query string, width int, reuseDiscard bool) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Queryx(query)
		if err != nil {
			b.Fatal(err)
		}
		var scanner *squealx.RawColumnScanner
		var raw []sql.RawBytes
		var targets []any
		if reuseDiscard {
			scanner, err = squealx.NewRawColumnScanner(rows, width/2)
		} else {
			raw = make([]sql.RawBytes, width)
			targets = make([]any, width)
			for col := range targets {
				targets[col] = &raw[col]
			}
		}
		if err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		for rows.Next() {
			var data []byte
			if reuseDiscard {
				var ok bool
				data, ok, err = scanner.Scan(rows)
				if err == nil && !ok {
					err = errors.New("raw column is NULL")
				}
			} else {
				err = rows.Scan(targets...)
				data = raw[width/2]
				for col := range raw {
					if col != width/2 {
						raw[col] = nil
					}
				}
			}
			if err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
			_ = len(data)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if scanner != nil {
			_ = scanner.Close()
		}
		clear(raw)
		clear(targets)
	}
}

func BenchmarkWriteJSONLines1000(b *testing.B) {
	db := setupSinkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Queryx(cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := squealx.WriteJSONLines(context.Background(), io.Discard, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteMapCursorJSONLines1000(b *testing.B) {
	db := setupSinkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[map[string]any](context.Background(), db, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := squealx.WriteMapCursorJSONLines(context.Background(), io.Discard, cursor); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteMapCursorJSONEncoder1000(b *testing.B) {
	db := setupSinkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[map[string]any](context.Background(), db, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		encoder := json.NewEncoder(io.Discard)
		for cursor.Next() {
			if err := encoder.Encode(cursor.Value()); err != nil {
				_ = cursor.Close()
				b.Fatal(err)
			}
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
		if err := cursor.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
