package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

type stmtCacheTestDriver struct {
	prepares atomic.Int32
	closes   atomic.Int32
}

func (d *stmtCacheTestDriver) Open(string) (driver.Conn, error) {
	return stmtCacheTestConn{driver: d}, nil
}

type stmtCacheTestConn struct{ driver *stmtCacheTestDriver }

func (c stmtCacheTestConn) Prepare(query string) (driver.Stmt, error) {
	c.driver.prepares.Add(1)
	return stmtCacheTestStmt{driver: c.driver}, nil
}
func (stmtCacheTestConn) Close() error { return nil }
func (stmtCacheTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("stmt cache test driver does not support transactions")
}

type stmtCacheTestStmt struct{ driver *stmtCacheTestDriver }

func (s stmtCacheTestStmt) Close() error  { s.driver.closes.Add(1); return nil }
func (stmtCacheTestStmt) NumInput() int   { return -1 }
func (stmtCacheTestStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (stmtCacheTestStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("stmt cache test driver does not support queries")
}

var stmtCacheTestDriverSeq atomic.Uint64

func registerStmtCacheTestDriver(t *testing.T) (*stmtCacheTestDriver, string) {
	t.Helper()
	d := &stmtCacheTestDriver{}
	name := fmt.Sprintf("squealx-stmt-cache-test-%d", stmtCacheTestDriverSeq.Add(1))
	sql.Register(name, d)
	return d, name
}

func TestStatementCacheDisabledByDefaultPreparesEveryCall(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "stmt-cache-disabled")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		if _, err := db.Exec("UPDATE t SET x = ?", i); err != nil {
			t.Fatal(err)
		}
	}
	if got := drv.prepares.Load(); got != 5 {
		t.Fatalf("prepares = %d, want 5 (cache disabled should prepare every call)", got)
	}
}

func TestStatementCacheReusesPreparedStatement(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "stmt-cache-enabled")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.EnableStatementCache(0)

	for i := 0; i < 5; i++ {
		if _, err := db.Exec("UPDATE t SET x = ?", i); err != nil {
			t.Fatal(err)
		}
	}
	if got := drv.prepares.Load(); got != 1 {
		t.Fatalf("prepares = %d, want 1 (cache should prepare once and reuse)", got)
	}

	if _, err := db.ExecContext(context.Background(), "UPDATE t SET y = ?", 1); err != nil {
		t.Fatal(err)
	}
	if got := drv.prepares.Load(); got != 2 {
		t.Fatalf("prepares = %d, want 2 (new query text should prepare once)", got)
	}

	if err := db.DisableStatementCache(); err != nil {
		t.Fatal(err)
	}
	if got := drv.closes.Load(); got != 2 {
		t.Fatalf("closes = %d, want 2 after disabling (both cached statements closed)", got)
	}

	if _, err := db.Exec("UPDATE t SET x = ?", 99); err != nil {
		t.Fatal(err)
	}
	if got := drv.prepares.Load(); got != 3 {
		t.Fatalf("prepares = %d, want 3 (cache disabled, back to preparing every call)", got)
	}
}

func TestStatementCacheEvictsOldestAndClosesIt(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "stmt-cache-evict")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.EnableStatementCache(2)

	for i := 0; i < 3; i++ {
		query := fmt.Sprintf("UPDATE t SET col%d = ?", i)
		if _, err := db.Exec(query, i); err != nil {
			t.Fatal(err)
		}
	}
	if got := drv.prepares.Load(); got != 3 {
		t.Fatalf("prepares = %d, want 3 (3 distinct queries, max 2 cached)", got)
	}
	if got := drv.closes.Load(); got != 1 {
		t.Fatalf("closes = %d, want 1 (oldest entry evicted and closed)", got)
	}
}

func TestStatementCacheSkipsOverlongQueries(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "stmt-cache-overlong")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.EnableStatementCache(0)

	longQuery := "UPDATE t SET x = ? -- " + string(make([]byte, stmtCacheMaxQueryLen+1))
	for i := 0; i < 3; i++ {
		if _, err := db.Exec(longQuery, i); err != nil {
			t.Fatal(err)
		}
	}
	if got := drv.prepares.Load(); got != 3 {
		t.Fatalf("prepares = %d, want 3 (overlong query must never be cached)", got)
	}
}
