package squealx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func writeSQL(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func testDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open("sqlite", ":memory:", "test-sqlite")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.MustExec(`CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT, org_id INTEGER)`)
	db.MustExec(`INSERT INTO users (user_id, username, org_id) VALUES (1, 'ada', 7), (2, 'lin', 7), (3, 'max', 8)`)
	return db
}

func TestParseQueriesMetadataAndBody(t *testing.T) {
	content := "\ufeff-- sql-name: list-users\r\n" +
		"-- doc: List users in an organization\r\n" +
		"-- connection: primary\r\n" +
		"\r\n" +
		"SELECT user_id, username\r\n" +
		"FROM users\r\n" +
		"-- doc: this comment stays in the body\r\n" +
		"WHERE org_id = :org_id;\r\n" +
		"-- sql-end\r\n"

	queries, err := parseQueries("queries.sql", content)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, ok := queries["list-users"]
	if !ok {
		t.Fatal("expected list-users query")
	}
	if q.Doc != "List users in an organization" {
		t.Errorf("doc = %q", q.Doc)
	}
	if q.Connection != "primary" {
		t.Errorf("connection = %q", q.Connection)
	}
	if !strings.Contains(q.Query, "-- doc: this comment stays in the body") {
		t.Errorf("body lost trailing doc comment: %q", q.Query)
	}
	if strings.Contains(q.Query, "List users in an organization") {
		t.Errorf("header doc leaked into body: %q", q.Query)
	}
	if q.Line != 1 {
		t.Errorf("line = %d, want 1", q.Line)
	}
	if q.Source != "queries.sql" {
		t.Errorf("source = %q", q.Source)
	}
	if q.Hash == "" {
		t.Error("missing content hash")
	}
}

func TestParseQueriesErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "unterminated block",
			content: "-- sql-name: a\nSELECT 1\n",
			want:    "missing -- sql-end",
		},
		{
			name:    "stray sql-end",
			content: "-- sql-end\n",
			want:    "without a matching -- sql-name",
		},
		{
			name:    "empty body",
			content: "-- sql-name: a\n-- doc: only metadata\n-- sql-end\n",
			want:    "empty query body",
		},
		{
			name:    "missing name",
			content: "-- sql-name:\nSELECT 1\n-- sql-end\n",
			want:    "missing query name",
		},
		{
			name:    "duplicate in file",
			content: "-- sql-name: a\nSELECT 1\n-- sql-end\n-- sql-name: a\nSELECT 2\n-- sql-end\n",
			want:    "duplicate query name",
		},
		{
			name:    "nested name without end",
			content: "-- sql-name: a\nSELECT 1\n-- sql-name: b\nSELECT 2\n-- sql-end\n",
			want:    "missing -- sql-end before next -- sql-name",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseQueries("queries.sql", tc.content)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
			var perr *ParseError
			if !errors.As(err, &perr) {
				t.Fatalf("error %v is not a *ParseError", err)
			}
			if perr.Source != "queries.sql" {
				t.Errorf("source = %q", perr.Source)
			}
		})
	}
}

func TestLoadFromDirMergeAndExtensions(t *testing.T) {
	dir := t.TempDir()
	writeSQL(t, filepath.Join(dir, "a.sql"), "-- sql-name: a\nSELECT 1\n-- sql-end\n")
	writeSQL(t, filepath.Join(dir, "b.SQL"), "-- sql-name: b\nSELECT 2\n-- sql-end\n")
	writeSQL(t, filepath.Join(dir, "nested", "c.sql"), "-- sql-name: c\nSELECT 3\n-- sql-end\n")
	writeSQL(t, filepath.Join(dir, "ignored.txt"), "-- sql-name: nope\nSELECT 0\n-- sql-end\n")

	loader, err := LoadFromDir(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loader.Len() != 2 || !loader.Has("a") || !loader.Has("b") {
		t.Fatalf("names = %v", loader.Names())
	}

	loader, err = LoadFromDir(dir, WithRecursive())
	if err != nil {
		t.Fatalf("recursive load: %v", err)
	}
	if loader.Len() != 3 || !loader.Has("c") {
		t.Fatalf("names = %v", loader.Names())
	}

	loader, err = LoadFromDir(dir, WithExtensions(".txt"))
	if err != nil {
		t.Fatalf("load with .txt filter: %v", err)
	}
	if !loader.Has("nope") || loader.Has("a") {
		t.Fatalf("names = %v", loader.Names())
	}
}

func TestDuplicateAcrossFilesIsRejected(t *testing.T) {
	dir := t.TempDir()
	writeSQL(t, filepath.Join(dir, "a.sql"), "-- sql-name: dup\nSELECT 1\n-- sql-end\n")
	writeSQL(t, filepath.Join(dir, "b.sql"), "-- sql-name: dup\nSELECT 2\n-- sql-end\n")

	_, err := LoadFromDir(dir)
	if err == nil || !errors.Is(err, ErrDuplicateQuery) {
		t.Fatalf("err = %v, want ErrDuplicateQuery", err)
	}
}

func TestReloadChangeSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: keep\nSELECT 1\n-- sql-end\n-- sql-name: edit\nSELECT 2\n-- sql-end\n")

	loader, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loader.Resolve("edit") != "SELECT 2" {
		t.Fatalf("resolve = %q", loader.Resolve("edit"))
	}

	writeSQL(t, path, "-- sql-name: keep\nSELECT 1\n-- sql-end\n-- sql-name: edit\nSELECT 22\n-- sql-end\n-- sql-name: fresh\nSELECT 3\n-- sql-end\n")

	cs, err := loader.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(cs.Added) != 1 || cs.Added[0] != "fresh" {
		t.Errorf("added = %v", cs.Added)
	}
	if len(cs.Updated) != 1 || cs.Updated[0] != "edit" {
		t.Errorf("updated = %v", cs.Updated)
	}
	if len(cs.Removed) != 0 {
		t.Errorf("removed = %v", cs.Removed)
	}
	if loader.Resolve("edit") != "SELECT 22" {
		t.Fatalf("resolve after reload = %q", loader.Resolve("edit"))
	}

	writeSQL(t, path, "-- sql-name: fresh\nSELECT 3\n-- sql-end\n")
	cs, err = loader.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(cs.Removed) != 2 || cs.Removed[0] != "edit" || cs.Removed[1] != "keep" {
		t.Errorf("removed = %v", cs.Removed)
	}
	if len(cs.Added) != 0 || len(cs.Updated) != 0 {
		t.Errorf("unexpected change set %s", cs)
	}

	cs, err = loader.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !cs.Empty() {
		t.Errorf("no-op reload reported %s", cs)
	}
}

func TestReloadOnlyChangedFileReparsed(t *testing.T) {
	dir := t.TempDir()
	stable := filepath.Join(dir, "stable.sql")
	volatile := filepath.Join(dir, "volatile.sql")
	writeSQL(t, stable, "-- sql-name: stable\nSELECT 1\n-- sql-end\n")
	writeSQL(t, volatile, "-- sql-name: volatile\nSELECT 2\n-- sql-end\n")

	loader, err := LoadFromDir(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	before := loader.GetQuery("stable")

	writeSQL(t, volatile, "-- sql-name: volatile\nSELECT 22\n-- sql-end\n")
	cs, err := loader.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(cs.Updated) != 1 || cs.Updated[0] != "volatile" {
		t.Fatalf("change set = %s", cs)
	}
	if got := loader.GetQuery("stable"); got != before {
		t.Error("unchanged file was reparsed instead of reused")
	}
}

func TestReloadKeepsRegistryOnParseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: good\nSELECT 1\n-- sql-end\n")
	loader, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	writeSQL(t, path, "-- sql-name: broken\nSELECT 1\n")
	if _, err := loader.Reload(); err == nil {
		t.Fatal("expected reload error")
	}
	if !loader.Has("good") {
		t.Fatal("registry was replaced by a broken reload")
	}

	writeSQL(t, path, "-- sql-name: broken\nSELECT 1\n-- sql-end\n")
	cs, err := loader.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if loader.Has("good") || !loader.Has("broken") {
		t.Fatalf("names = %v", loader.Names())
	}
	if len(cs.Removed) != 1 || cs.Removed[0] != "good" {
		t.Errorf("change set = %s", cs)
	}
}

func TestWatchReloadsOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: q\nSELECT 1\n-- sql-end\n")
	loader, err := LoadFromFile(path, WithWatchInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	changes := make(chan *ChangeSet, 4)
	loader.OnReload(func(cs *ChangeSet) { changes <- cs })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan error, 1)
	go func() { watchDone <- loader.Watch(ctx) }()

	writeSQL(t, path, "-- sql-name: q\nSELECT 22\n-- sql-end\n")

	select {
	case cs := <-changes:
		if len(cs.Updated) != 1 || cs.Updated[0] != "q" {
			t.Fatalf("change set = %s", cs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reload")
	}

	cancel()
	if err := <-watchDone; err != nil {
		t.Fatalf("watch returned %v", err)
	}
	if loader.Resolve("q") != "SELECT 22" {
		t.Fatalf("resolve = %q", loader.Resolve("q"))
	}
}

func TestStrictModeUnknownNamesAndConnections(t *testing.T) {
	dir := t.TempDir()
	writeSQL(t, filepath.Join(dir, "queries.sql"), `
-- sql-name: bound
-- connection: primary
SELECT 1
-- sql-end
`)
	loader, err := LoadFromDir(dir, WithStrict())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	db := testDB(t)

	err = loader.Select(db, new([]map[string]any), "missing-name")
	if !errors.Is(err, ErrQueryNotFound) {
		t.Fatalf("err = %v, want ErrQueryNotFound", err)
	}

	var rows []map[string]any
	if err := loader.Select(db, &rows, "SELECT 1 AS n"); err != nil {
		t.Fatalf("raw SQL passthrough failed: %v", err)
	}

	other := testDB(t)
	other.ID = "replica"
	err = loader.Select(other, &rows, "bound")
	if !errors.Is(err, ErrConnectionMismatch) {
		t.Fatalf("err = %v, want ErrConnectionMismatch", err)
	}
}

func TestNonStrictFallsBackToRawSQL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n")
	loader, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	db := testDB(t)

	var rows []map[string]any
	if err := loader.Select(db, &rows, "missing-name"); err == nil {
		t.Fatal("expected unknown name to fail as SQL")
	}
	if err := loader.Select(db, &rows, "SELECT 1 AS n"); err != nil {
		t.Fatalf("raw SQL: %v", err)
	}
	if loader.Resolve("q") != "SELECT 1 AS n" || loader.Resolve("SELECT 1") != "SELECT 1" {
		t.Fatal("resolve mismatch")
	}
}

func TestLoaderExecutesNamedQueries(t *testing.T) {
	dir := t.TempDir()
	writeSQL(t, filepath.Join(dir, "queries.sql"), `
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
	loader, err := LoadFromDir(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	db := testDB(t)

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

func TestQueryRegistryIntrospection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, `
-- sql-name: zeta
-- doc: last
SELECT 1
-- sql-end
-- sql-name: alpha
SELECT 2
-- sql-end
`)
	loader, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	names := loader.Names()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "zeta" {
		t.Fatalf("names = %v", names)
	}
	if loader.MustGetQuery("zeta").Doc != "last" {
		t.Fatal("MustGetQuery lost metadata")
	}
	if loader.Len() != 2 {
		t.Fatalf("len = %d", loader.Len())
	}
	if len(loader.Sources()) != 1 {
		t.Fatalf("sources = %v", loader.Sources())
	}
	snapshot := loader.Queries()
	delete(snapshot, "alpha")
	if !loader.Has("alpha") {
		t.Fatal("Queries() exposed the live registry")
	}
	if loader.GetQuery("missing") != nil {
		t.Fatal("GetQuery should return nil for unknown names")
	}
}

func TestStmtCacheInvalidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: q\nSELECT 1 AS n\n-- sql-end\n")
	loader, err := LoadFromFile(path, WithStmtCache())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	db := testDB(t)

	first, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	second, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if first != second {
		t.Fatal("cached prepare returned a new handle")
	}

	writeSQL(t, path, "-- sql-name: q\nSELECT 2 AS n\n-- sql-end\n")
	if _, err := loader.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	third, err := loader.Preparex(db, "q")
	if err != nil {
		t.Fatalf("prepare after reload: %v", err)
	}
	if third == first {
		t.Fatal("stale statement handle survived the reload")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close stale handle: %v", err)
	}
	if err := loader.CloseStmts(); err != nil {
		t.Fatalf("CloseStmts: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("close cached handle: %v", err)
	}
}

func TestMustGetQueryPanicsOnUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "-- sql-name: q\nSELECT 1\n-- sql-end\n")
	loader, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	loader.MustGetQuery("typo")
}

func TestLoadRejectsFilesWithoutQueries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.sql")
	writeSQL(t, path, "SELECT 1\n")
	if _, err := LoadFromFile(path); !errors.Is(err, ErrNoQueries) {
		t.Fatalf("err = %v, want ErrNoQueries", err)
	}
}
