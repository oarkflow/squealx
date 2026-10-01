package sqlite

import (
	"testing"

	"github.com/oarkflow/squealx"
)

type benchRow struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
	Age  int64  `db:"age"`
}

func benchDB(b *testing.B, n int) *squealx.DB {
	b.Helper()
	db, err := Open(":memory:", "ops-bench")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`); err != nil {
		b.Fatal(err)
	}
	tx, _ := db.Begin()
	for i := 1; i <= n; i++ {
		if _, err := tx.Exec(`INSERT INTO t VALUES (?,?,?)`, i, "name", i%90); err != nil {
			b.Fatal(err)
		}
	}
	_ = tx.Commit()
	return db
}

func BenchmarkOpRawQueryRow(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		var r benchRow
		if err := db.DB().QueryRow(`SELECT id,name,age FROM t WHERE id=?`, i%1000+1).Scan(&r.ID, &r.Name, &r.Age); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpGet(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		var r benchRow
		if err := db.Get(&r, `SELECT id,name,age FROM t WHERE id=?`, i%1000+1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpSelect100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		var rows []benchRow
		if err := db.Select(&rows, `SELECT id,name,age FROM t WHERE id<=100`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpRawSelect100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		rs, err := db.DB().Query(`SELECT id,name,age FROM t WHERE id<=100`)
		if err != nil {
			b.Fatal(err)
		}
		rows := make([]benchRow, 0, 8)
		for rs.Next() {
			var r benchRow
			_ = rs.Scan(&r.ID, &r.Name, &r.Age)
			rows = append(rows, r)
		}
		rs.Close()
	}
}

func BenchmarkOpNamedSelect100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		var rows []benchRow
		if err := db.NamedSelect(&rows, `SELECT id,name,age FROM t WHERE id<=:max`, map[string]any{"max": 100}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpSelectIN(b *testing.B) {
	db := benchDB(b, 1000)
	ids := []int{1, 5, 9, 50, 99}
	b.ReportAllocs()
	for b.Loop() {
		var rows []benchRow
		if err := db.Select(&rows, `SELECT id,name,age FROM t WHERE id IN (?)`, ids); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpExec(b *testing.B) {
	db := benchDB(b, 10)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, err := db.Exec(`UPDATE t SET age=? WHERE id=?`, i%90, i%10+1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpRawExec(b *testing.B) {
	db := benchDB(b, 10)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, err := db.DB().Exec(`UPDATE t SET age=? WHERE id=?`, i%90, i%10+1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpNamedExec(b *testing.B) {
	db := benchDB(b, 10)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, err := db.NamedExec(`UPDATE t SET age=:age WHERE id=:id`, map[string]any{"age": i % 90, "id": i%10 + 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpSelectMaps100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		var rows []map[string]any
		if err := db.Select(&rows, `SELECT id,name,age FROM t WHERE id<=100`); err != nil {
			b.Fatal(err)
		}
	}
}
