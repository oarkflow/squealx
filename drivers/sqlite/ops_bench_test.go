package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
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

func BenchmarkOpGetMap(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		var row map[string]any
		if err := db.Get(&row, `SELECT id,name,age FROM t WHERE id=?`, i%1000+1); err != nil {
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

func BenchmarkRepositoryRead1000(b *testing.B) {
	db := benchDB(b, 1000)
	repo := squealx.New[benchRow](db, "t", "id")
	b.Run("Find", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := repo.Find(context.Background(), nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(rows) != 1000 {
				b.Fatalf("loaded %d rows", len(rows))
			}
		}
	})
	b.Run("FindCursor", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cursor, err := squealx.FindCursor(context.Background(), repo, nil)
			if err != nil {
				b.Fatal(err)
			}
			var n int
			for cursor.Next() {
				n++
				_ = cursor.Value().ID
			}
			if err := cursor.Err(); err != nil {
				b.Fatal(err)
			}
			if n != 1000 {
				b.Fatalf("loaded %d rows", n)
			}
		}
	})
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

func BenchmarkScanEachRawSQLRows100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.SQLDB.DB().Query(`SELECT id,name,age FROM t WHERE id<=100`)
		if err != nil {
			b.Fatal(err)
		}
		err = squealx.ScanEach[benchRow](rows, false, func(benchRow) error { return nil })
		closeErr := rows.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
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

func BenchmarkSQLiteFilePoolConcurrentRead(b *testing.B) {
	for _, maxOpen := range []int{1, 2, 4, 8} {
		b.Run(strconv.Itoa(maxOpen), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "pool.sqlite"), fmt.Sprintf("file-pool-%d", maxOpen))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(maxOpen)
			db.SetMaxIdleConns(maxOpen)
			if _, err := db.Exec(`CREATE TABLE pool_rows (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
				b.Fatal(err)
			}
			tx, err := db.Beginx()
			if err != nil {
				b.Fatal(err)
			}
			for i := 1; i <= 1000; i++ {
				if _, err := tx.Exec(`INSERT INTO pool_rows VALUES (?, ?)`, i, "row"); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}

			var sequence atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var row struct {
					ID   int    `db:"id"`
					Name string `db:"name"`
				}
				for pb.Next() {
					id := sequence.Add(1)%1000 + 1
					if err := db.Get(&row, `SELECT id,name FROM pool_rows WHERE id=?`, id); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkSQLiteFilePoolMixedReadWrite(b *testing.B) {
	for _, maxOpen := range []int{1, 2, 4, 8} {
		b.Run(strconv.Itoa(maxOpen), func(b *testing.B) {
			dsn := filepath.Join(b.TempDir(), "pool.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
			db, err := Open(dsn, fmt.Sprintf("file-mixed-pool-%d", maxOpen))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(maxOpen)
			db.SetMaxIdleConns(maxOpen)
			if _, err := db.Exec(`CREATE TABLE pool_rows (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
				b.Fatal(err)
			}
			tx, err := db.Beginx()
			if err != nil {
				b.Fatal(err)
			}
			for i := 1; i <= 1000; i++ {
				if _, err := tx.Exec(`INSERT INTO pool_rows VALUES (?, ?)`, i, "row"); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}

			var sequence atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var row struct {
					ID   int    `db:"id"`
					Name string `db:"name"`
				}
				var operation uint64
				for pb.Next() {
					id := sequence.Add(1)%1000 + 1
					operation++
					if operation%10 == 0 {
						if _, err := db.Exec(`UPDATE pool_rows SET name=name WHERE id=?`, id); err != nil {
							b.Error(err)
							return
						}
					} else if err := db.Get(&row, `SELECT id,name FROM pool_rows WHERE id=?`, id); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
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

func BenchmarkOpSelectTypedMaps100(b *testing.B) {
	db := benchDB(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		var rows []map[string]string
		if err := db.Select(&rows, `SELECT id,name,age FROM t WHERE id<=100`); err != nil {
			b.Fatal(err)
		}
	}
}
