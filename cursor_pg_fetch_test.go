package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// pgFetchTestDriver is a minimal fake PostgreSQL-shaped driver: it answers
// "DECLARE ... CURSOR FOR ..." and "CLOSE ..." as no-ops, and
// "FETCH FORWARD n FROM ..." by handing out the next n of totalRows integer
// rows, so postgresFetchRows's batching/exhaustion logic can be exercised
// without a real PostgreSQL server.
type pgFetchTestDriver struct {
	totalRows int
	fetches   []int // rows actually returned by each FETCH FORWARD call
}

func (d *pgFetchTestDriver) Open(string) (driver.Conn, error) {
	return &pgFetchTestConn{driver: d}, nil
}

type pgFetchTestConn struct {
	driver  *pgFetchTestDriver
	fetched int
}

func (c *pgFetchTestConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("pg fetch test driver does not support Prepare")
}
func (c *pgFetchTestConn) Close() error              { return nil }
func (c *pgFetchTestConn) Begin() (driver.Tx, error) { return pgFetchTestTx{}, nil }

func (c *pgFetchTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return driver.ResultNoRows, nil
}

func (c *pgFetchTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	n := parsePgFetchN(query)
	remaining := c.driver.totalRows - c.fetched
	count := max(min(n, remaining), 0)
	c.driver.fetches = append(c.driver.fetches, count)
	rows := &pgFetchTestRows{start: c.fetched, count: count}
	c.fetched += count
	return rows, nil
}

func parsePgFetchN(query string) int {
	fields := strings.Fields(query)
	for i, f := range fields {
		if strings.EqualFold(f, "FORWARD") && i+1 < len(fields) {
			n, err := strconv.Atoi(fields[i+1])
			if err == nil {
				return n
			}
		}
	}
	return 0
}

type pgFetchTestTx struct{}

func (pgFetchTestTx) Commit() error   { return nil }
func (pgFetchTestTx) Rollback() error { return nil }

type pgFetchTestRows struct {
	start, count, idx int
}

func (r *pgFetchTestRows) Columns() []string { return []string{"id"} }
func (r *pgFetchTestRows) Close() error      { return nil }
func (r *pgFetchTestRows) Next(dest []driver.Value) error {
	if r.idx >= r.count {
		return io.EOF
	}
	dest[0] = int64(r.start + r.idx + 1)
	r.idx++
	return nil
}

var registerPgFetchDriverOnce sync.Once

// registerPgFetchDriver registers the fake driver under the literal name
// "postgres" (required so isPostgresDriver recognizes it), exactly once per
// test binary since sql.Register panics on a duplicate name; each test gets
// its own *pgFetchTestDriver instance via a package-level pointer swapped
// before use, since the registered driver.Driver itself can't be replaced.
var activePgFetchDriver *pgFetchTestDriver
var activePgFetchDriverMu sync.Mutex

type pgFetchDriverProxy struct{}

func (pgFetchDriverProxy) Open(dsn string) (driver.Conn, error) {
	activePgFetchDriverMu.Lock()
	d := activePgFetchDriver
	activePgFetchDriverMu.Unlock()
	return d.Open(dsn)
}

func registerPgFetchDriver(t *testing.T, totalRows int) *pgFetchTestDriver {
	t.Helper()
	registerPgFetchDriverOnce.Do(func() {
		sql.Register("postgres", pgFetchDriverProxy{})
	})
	d := &pgFetchTestDriver{totalRows: totalRows}
	activePgFetchDriverMu.Lock()
	activePgFetchDriver = d
	activePgFetchDriverMu.Unlock()
	return d
}

func TestPostgresFetchCursorPagesInBatches(t *testing.T) {
	drv := registerPgFetchDriver(t, 10)
	db, err := Open("postgres", "", "pg-fetch")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cursor, err := QueryCursorConfig[int](context.Background(), db, CursorConfig{FetchSize: 4}, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for cursor.Next() {
		got = append(got, cursor.Value())
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	// 10 rows at 4/batch: fetches of 4, 4, 2 (the short batch ends it,
	// no extra round trip to confirm exhaustion).
	if !slices.Equal(drv.fetches, []int{4, 4, 2}) {
		t.Fatalf("fetches = %v, want [4 4 2]", drv.fetches)
	}
}

func TestPostgresFetchCursorExactMultipleStopsCleanly(t *testing.T) {
	drv := registerPgFetchDriver(t, 8)
	db, err := Open("postgres", "", "pg-fetch-exact")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cursor, err := QueryCursorConfig[int](context.Background(), db, CursorConfig{FetchSize: 4}, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for cursor.Next() {
		got = append(got, cursor.Value())
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 8 {
		t.Fatalf("got %d rows, want 8", len(got))
	}
	// Exactly divisible: the driver must fetch once more to see 0 rows and
	// know the cursor is exhausted, unlike the short-final-batch case.
	if !slices.Equal(drv.fetches, []int{4, 4, 0}) {
		t.Fatalf("fetches = %v, want [4 4 0]", drv.fetches)
	}
}

func TestNonPostgresDriverIgnoresFetchSize(t *testing.T) {
	drv, name := registerStmtCacheTestDriver(t)
	db, err := Open(name, "", "not-postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// FetchSize must have no effect for a non-Postgres driver name: it must
	// take the plain Queryx path (which this fake driver fails with a
	// distinct "does not support queries" error), not the Postgres
	// DECLARE/FETCH path (which would instead fail on BeginTx with a
	// different "does not support transactions" error).
	_, err = QueryCursorConfig[int](context.Background(), db, CursorConfig{FetchSize: 4}, "SELECT id FROM t")
	if err == nil || !strings.Contains(err.Error(), "does not support queries") {
		t.Fatalf("error = %v, want the plain-Queryx path's error", err)
	}
	_ = drv
}
