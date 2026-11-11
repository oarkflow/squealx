package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/oarkflow/squealx"
)

func openLoaderTestDB(t *testing.T) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", "file-loader-test")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.MustExec(`CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT, org_id INTEGER)`)
	db.MustExec(`INSERT INTO users (user_id, username, org_id) VALUES (1, 'ada', 7), (2, 'lin', 7), (3, 'max', 8)`)
	return db
}

func writeLoaderSQL(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestFileLoaderStrictMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: bound\n-- connection: primary\nSELECT 1\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStrict())
	if err != nil {
		t.Fatal(err)
	}
	db := openLoaderTestDB(t)

	err = loader.Select(db, new([]map[string]any), "missing-name")
	if !errors.Is(err, squealx.ErrQueryNotFound) {
		t.Fatalf("err = %v, want ErrQueryNotFound", err)
	}
	var rows []map[string]any
	if err := loader.Select(db, &rows, "SELECT 1 AS n"); err != nil {
		t.Fatalf("raw SQL passthrough: %v", err)
	}
	db.ID = "replica"
	if err := loader.Select(db, &rows, "bound"); !errors.Is(err, squealx.ErrConnectionMismatch) {
		t.Fatalf("err = %v, want ErrConnectionMismatch", err)
	}
}

func TestFileLoaderNamedQueries(t *testing.T) {
	dir := t.TempDir()
	writeLoaderSQL(t, filepath.Join(dir, "queries.sql"), `
-- sql-name: list-users
-- doc: List users in an organization
SELECT user_id, username
FROM users
WHERE org_id = :org_id
ORDER BY user_id
-- sql-end

-- sql-name: one-user
SELECT username
FROM users
WHERE user_id = @user_id
-- sql-end

-- sql-name: org-headcount
SELECT COUNT(*) AS n
FROM users
WHERE org_id = @org_id
-- sql-end
`)
	loader, err := squealx.LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	db := openLoaderTestDB(t)

	var users []map[string]any
	if err := loader.Select(db, &users, "list-users", map[string]any{"org_id": 7}); err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("users = %v", users)
	}

	var one map[string]any
	if err := loader.Get(db, &one, "one-user", map[string]any{"user_id": 1}); err != nil {
		t.Fatalf("get: %v", err)
	}
	if one["username"] != "ada" {
		t.Fatalf("row = %v", one)
	}

	var headcount []map[string]any
	if err := loader.Select(db, &headcount, "org-headcount", map[string]any{"org_id": 8}); err != nil {
		t.Fatalf("headcount: %v", err)
	}
	if len(headcount) != 1 {
		t.Fatalf("headcount = %v", headcount)
	}
}

func TestFileLoaderStatementCacheInvalidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStmtCache())
	if err != nil {
		t.Fatal(err)
	}
	db := openLoaderTestDB(t)

	first, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	second, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("cached prepare returned a different handle")
	}

	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 2 AS n\n-- sql-end\n")
	if _, err := loader.Reload(); err != nil {
		t.Fatal(err)
	}
	third, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("stale handle survived query reload")
	}
	if err := loader.CloseStmts(); err != nil {
		t.Fatal(err)
	}
}

func TestFileLoaderStatementCacheSeparatesNamedAndContextPrepares(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n-- sql-name: canceled\nSELECT 2 AS n\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStmtCache())
	if err != nil {
		t.Fatal(err)
	}
	db := openLoaderTestDB(t)
	defer loader.CloseStmts()

	stmt, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	named, err := loader.PrepareNamed(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil || named == nil {
		t.Fatalf("prepared statement handles: stmt=%v named=%v", stmt, named)
	}
	var got int
	if err := named.Get(&got, map[string]any{}); err != nil || got != 1 {
		t.Fatalf("named statement result = %d, err=%v", got, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loader.PreparexContext(db, ctx, "canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("PreparexContext error = %v, want context.Canceled", err)
	}
}

func TestFileLoaderCachedHandlesCanBeClosedWithoutClosingCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: positional\nSELECT 7 AS n\n-- sql-end\n-- sql-name: named\nSELECT :n AS n\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStmtCache())
	if err != nil {
		t.Fatal(err)
	}
	defer loader.CloseStmts()
	db := openLoaderTestDB(t)

	stmt, err := loader.Preparex(db, "positional")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	stmtAgain, err := loader.Preparex(db, "positional")
	if err != nil {
		t.Fatal(err)
	}
	if stmtAgain != stmt {
		t.Fatal("borrowed cached statement handle changed")
	}
	var positional int
	if err := stmtAgain.Get(&positional); err != nil || positional != 7 {
		t.Fatalf("cached statement after caller Close = %d, err=%v", positional, err)
	}

	named, err := loader.PrepareNamed(db, "named")
	if err != nil {
		t.Fatal(err)
	}
	if err := named.Close(); err != nil {
		t.Fatal(err)
	}
	namedAgain, err := loader.PrepareNamed(db, "named")
	if err != nil {
		t.Fatal(err)
	}
	if namedAgain != named {
		t.Fatal("borrowed cached named statement handle changed")
	}
	var namedValue int
	if err := namedAgain.Get(&namedValue, map[string]any{"n": 9}); err != nil || namedValue != 9 {
		t.Fatalf("cached named statement after caller Close = %d, err=%v", namedValue, err)
	}
}

func BenchmarkFileLoaderPreparedStatementCache(b *testing.B) {
	for _, cached := range []bool{false, true} {
		name := "uncached"
		if cached {
			name = "cached"
		}
		b.Run(name, func(b *testing.B) {
			queryPath := filepath.Join(b.TempDir(), "queries.sql")
			if err := os.WriteFile(queryPath, []byte("-- sql-name: lookup\nSELECT user_id, username FROM users WHERE user_id = :user_id\n-- sql-end\n"), 0o600); err != nil {
				b.Fatal(err)
			}
			options := []squealx.LoaderOption{}
			if cached {
				options = append(options, squealx.WithStmtCache())
			}
			loader, err := squealx.LoadFromFile(queryPath, options...)
			if err != nil {
				b.Fatal(err)
			}
			db, err := Open(":memory:", "file-loader-cache-benchmark")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			defer loader.CloseStmts()
			db.MustExec(`CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT NOT NULL); INSERT INTO users VALUES (1, 'ada')`)
			arg := map[string]any{"user_id": 1}
			type userRow struct {
				ID   int    `db:"user_id"`
				Name string `db:"username"`
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stmt, err := loader.PrepareNamed(db, "lookup")
				if err != nil {
					b.Fatal(err)
				}
				var got userRow
				if err := stmt.Get(&got, arg); err != nil {
					b.Fatal(err)
				}
				if !cached {
					if err := stmt.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestFileLoaderStatementCacheConcurrentPrepare(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStmtCache())
	if err != nil {
		t.Fatal(err)
	}
	defer loader.CloseStmts()
	db := openLoaderTestDB(t)

	const workers = 32
	statements := make([]*squealx.Stmt, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			statements[i], errs[i] = loader.Preparex(db, "q")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
		if statements[i] != statements[0] {
			t.Fatalf("prepare %d returned a different cached statement", i)
		}
	}
}

func TestFileLoaderStatementCacheReloadWhilePreparing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n")
	loader, err := squealx.LoadFromFile(path, squealx.WithStmtCache())
	if err != nil {
		t.Fatal(err)
	}
	defer loader.CloseStmts()
	db := openLoaderTestDB(t)

	const workers = 8
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var prepareErr error
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 20 {
				if _, err := loader.Preparex(db, "q"); err != nil {
					errMu.Lock()
					prepareErr = errors.Join(prepareErr, err)
					errMu.Unlock()
					return
				}
			}
		}()
	}
	writeLoaderSQL(t, path, "-- sql-name: q\nSELECT 2 AS n\n-- sql-end\n")
	if _, err := loader.Reload(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	errMu.Lock()
	defer errMu.Unlock()
	if prepareErr != nil {
		t.Fatal(prepareErr)
	}
	stmt, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatal(err)
	}
	var got int
	if err := stmt.Get(&got); err != nil || got != 2 {
		t.Fatalf("post-reload statement result = %d, err=%v", got, err)
	}
}
