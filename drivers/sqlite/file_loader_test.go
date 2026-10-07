package sqlite

import (
	"errors"
	"os"
	"path/filepath"
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
