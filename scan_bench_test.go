package squealx

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

// benchScanRow is the destination struct used by the Select/Get/StructScan
// benchmarks below.
type benchScanRow struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

// benchScanConn is a minimal database/sql/driver.Conn that answers every
// query with a fixed number of canned rows, so Select/Get/StructScan can be
// benchmarked end-to-end without a real database.
type benchScanConn struct{ rowCount int }

func (c benchScanConn) Prepare(query string) (driver.Stmt, error) {
	return benchScanStmt{rowCount: c.rowCount}, nil
}
func (benchScanConn) Close() error { return nil }
func (benchScanConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("squealx: benchScanConn does not support transactions")
}

type benchScanStmt struct{ rowCount int }

func (benchScanStmt) Close() error  { return nil }
func (benchScanStmt) NumInput() int { return -1 }
func (benchScanStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s benchScanStmt) Query([]driver.Value) (driver.Rows, error) {
	return &benchScanRows{rowCount: s.rowCount}, nil
}

type benchScanRows struct {
	rowCount int
	next     int
}

func (*benchScanRows) Columns() []string { return []string{"id", "name", "email"} }
func (*benchScanRows) Close() error      { return nil }
func (r *benchScanRows) Next(dest []driver.Value) error {
	if r.next >= r.rowCount {
		return io.EOF
	}
	dest[0] = int64(r.next + 1)
	dest[1] = fmt.Sprintf("user-%d", r.next+1)
	dest[2] = fmt.Sprintf("user-%d@example.com", r.next+1)
	r.next++
	return nil
}

type benchScanDriver struct{ rowCount int }

func (d benchScanDriver) Open(string) (driver.Conn, error) {
	return benchScanConn{rowCount: d.rowCount}, nil
}

var benchScanDriverSeq atomic.Uint64

// newBenchScanDB registers a fresh fake driver returning rowCount canned rows
// per query and returns a *DB connected to it.
func newBenchScanDB(b *testing.B, rowCount int) *DB {
	b.Helper()
	name := fmt.Sprintf("squealx-scan-bench-%d", benchScanDriverSeq.Add(1))
	sql.Register(name, benchScanDriver{rowCount: rowCount})
	db, err := Open(name, "", "scan-bench")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkSelectStructScanSmall(b *testing.B) {
	db := newBenchScanDB(b, 10)
	b.ReportAllocs()
	for b.Loop() {
		var rows []benchScanRow
		if err := db.Select(&rows, "SELECT id, name, email FROM users"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectStructScanLarge(b *testing.B) {
	db := newBenchScanDB(b, 500)
	b.ReportAllocs()
	for b.Loop() {
		var rows []benchScanRow
		if err := db.Select(&rows, "SELECT id, name, email FROM users"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetStruct(b *testing.B) {
	db := newBenchScanDB(b, 1)
	b.ReportAllocs()
	for b.Loop() {
		var row benchScanRow
		if err := db.Get(&row, "SELECT id, name, email FROM users WHERE id = 1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryxStructScan(b *testing.B) {
	db := newBenchScanDB(b, 100)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Queryx("SELECT id, name, email FROM users")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var row benchScanRow
			if err := rows.StructScan(&row); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
