package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/oarkflow/squealx"
)

type cursorUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func setupCursorDB(t testing.TB) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", "cursor-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO users(id,name) VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
	`); err != nil {
		_ = db.Close()
		t.Fatalf("setup: %v", err)
	}
	if t, ok := t.(*testing.T); ok {
		t.Cleanup(func() { _ = db.Close() })
	}
	return db
}

func TestTypedCursorStruct(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	var ids []int
	for cursor.Next() {
		ids = append(ids, cursor.Value().ID)
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	if stats := cursor.Stats(); stats.Rows != 3 || !stats.Closed {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestTypedCursorPointerReuseAndCopy(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursor[*cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	firstReusable := cursor.Value()
	firstCopy := cursor.Copy()
	if !cursor.Next() {
		t.Fatal(cursor.Err())
	}
	if firstReusable != cursor.Value() || firstReusable.ID != 2 {
		t.Fatalf("pointer storage was not reused: first=%p current=%p value=%+v", firstReusable, cursor.Value(), firstReusable)
	}
	if firstCopy.ID != 1 || firstCopy == firstReusable {
		t.Fatalf("copy was not independent: copy=%+v", firstCopy)
	}
}

func TestTypedCursorMapScalarAndNamed(t *testing.T) {
	db := setupCursorDB(t)
	maps, err := squealx.QueryCursor[map[string]any](context.Background(), db, "SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Next() || maps.Value()["name"] != "Alice" {
		t.Fatalf("map=%v err=%v", maps.Value(), maps.Err())
	}
	if err := maps.Close(); err != nil {
		t.Fatal(err)
	}

	scalar, err := squealx.QueryCursor[int](context.Background(), db, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !scalar.Next() || scalar.Value() != 3 {
		t.Fatalf("count=%d err=%v", scalar.Value(), scalar.Err())
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	named, err := squealx.NamedQueryCursor[cursorUser](context.Background(), db,
		"SELECT id,name FROM users WHERE id IN :ids ORDER BY id", map[string]any{"ids": []int{1, 3}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := named.Collect(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != 1 || rows[1].ID != 3 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestCursorLimitAndStrictColumns(t *testing.T) {
	db := setupCursorDB(t)
	cursor, err := squealx.QueryCursorConfig[cursorUser](context.Background(), db,
		squealx.CursorConfig{MaxRows: 1}, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Next() || !errors.Is(cursor.Err(), squealx.ErrCursorLimit) {
		t.Fatalf("limit err=%v", cursor.Err())
	}

	type onlyID struct {
		ID int `db:"id"`
	}
	if _, err := squealx.QueryCursor[onlyID](context.Background(), db, "SELECT id,name FROM users"); err == nil {
		t.Fatal("expected strict missing-column error")
	}
	unsafeCursor, err := squealx.QueryCursor[onlyID](context.Background(), db.Unsafe(), "SELECT id,name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	_ = unsafeCursor.Close()
}

func TestRowErrDoesNotConsumeRowAndUnsafePreservesHooks(t *testing.T) {
	db := setupCursorDB(t)
	row := db.QueryRowx("SELECT id,name FROM users WHERE id=1")
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	var user cursorUser
	if err := row.StructScan(&user); err != nil {
		t.Fatalf("row was consumed by Err: %v", err)
	}

	called := 0
	db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		called++
		return ctx, query, args, nil
	})
	var count int
	if err := db.Unsafe().Get(&count, "SELECT COUNT(*) FROM users"); err != nil {
		t.Fatal(err)
	}
	if called == 0 {
		t.Fatal("Unsafe dropped hooks")
	}
	if name, err := db.GetDBName(); err != nil || name != "main" {
		t.Fatalf("db name=%q err=%v", name, err)
	}
}

func TestHookErrorAndPanicBecomeRowErrors(t *testing.T) {
	db := setupCursorDB(t)
	blocked := errors.New("blocked")
	db.UseBefore(func(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
		return ctx, query, args, blocked
	})
	row := db.QueryRowx("SELECT 1")
	if !errors.Is(row.Err(), blocked) {
		t.Fatalf("err=%v", row.Err())
	}
	if err := row.Scan(new(int)); !errors.Is(err, blocked) {
		t.Fatalf("scan err=%v", err)
	}

	db2 := setupCursorDB(t)
	db2.UseBefore(func(context.Context, string, ...any) (context.Context, string, []any, error) {
		panic("hook boom")
	})
	if _, err := db2.Exec("SELECT 1"); err == nil {
		t.Fatal("expected hook panic error")
	} else {
		var panicErr *squealx.HookPanicError
		if !errors.As(err, &panicErr) {
			t.Fatalf("err=%T %v", err, err)
		}
	}
}

func TestSelectTypedPointer(t *testing.T) {
	db := setupCursorDB(t)
	user, err := squealx.SelectTyped[*cursorUser](db, "SELECT id,name FROM users WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if user == nil || user.ID != 1 {
		t.Fatalf("user=%+v", user)
	}
}

func BenchmarkCursorStruct(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id")
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			_ = cursor.Value().ID
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestExecWithReturnContextSQLite(t *testing.T) {
	db := setupCursorDB(t)
	user := &cursorUser{Name: "Dora"}
	if err := db.ExecWithReturnContext(context.Background(),
		"INSERT INTO users(name) VALUES (:name)", user); err != nil {
		t.Fatal(err)
	}
	if user.ID == 0 || user.Name != "Dora" {
		t.Fatalf("returned user=%+v", user)
	}

	user.Name = "Dora Updated"
	if err := db.ExecWithReturnContext(context.Background(),
		"UPDATE users SET name=:name WHERE id=:id", user); err != nil {
		t.Fatal(err)
	}
	if user.Name != "Dora Updated" {
		t.Fatalf("updated user=%+v", user)
	}
}

func TestSmartSelectContextNamedAndIN(t *testing.T) {
	db := setupCursorDB(t)
	var one cursorUser
	if err := db.SmartSelectContext(context.Background(), &one,
		"SELECT id,name FROM users WHERE id=:id", map[string]any{"id": 2}); err != nil {
		t.Fatal(err)
	}
	if one.ID != 2 {
		t.Fatalf("one=%+v", one)
	}
	var many []cursorUser
	if err := db.SmartSelectContext(context.Background(), &many,
		"SELECT id,name FROM users WHERE id IN (?) ORDER BY id", []int{1, 3}); err != nil {
		t.Fatal(err)
	}
	if len(many) != 2 || many[1].ID != 3 {
		t.Fatalf("many=%+v", many)
	}
}

const cursorManyRowsQuery = `WITH RECURSIVE seq(id) AS (
	SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 1000
) SELECT id, 'user-' || id AS name FROM seq`

func BenchmarkCursorStruct1000(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := squealx.QueryCursor[cursorUser](context.Background(), db, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			_ = cursor.Value().ID
		}
		if err := cursor.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectEachStruct1000(b *testing.B) {
	db := setupCursorDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := squealx.SelectEach(db, func(user cursorUser) error {
			_ = user.ID
			return nil
		}, cursorManyRowsQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestQueryIterStruct(t *testing.T) {
	db := setupCursorDB(t)
	var ids []int
	for u, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT id,name FROM users ORDER BY id") {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, u.ID)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	// early break closes the cursor; connection (MaxOpenConns=1) must be reusable
	for _, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT id,name FROM users") {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	var n int
	if err := db.Get(&n, "SELECT COUNT(*) FROM users"); err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, err := range squealx.QueryIter[cursorUser](context.Background(), db, "SELECT nope FROM missing") {
		if err == nil {
			t.Fatal("expected error")
		}
	}
}

func TestTxAndStmtCursor(t *testing.T) {
	db := setupCursorDB(t)
	tx, err := db.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	cur, err := squealx.TxQueryCursor[cursorUser](context.Background(), tx, "SELECT id,name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for u, err := range cur.All() {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, u.Name)
	}
	if fmt.Sprint(names) != "[Alice Bob Carol]" {
		t.Fatalf("names=%v", names)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Preparex("SELECT id,name FROM users WHERE id > ? ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	scur, err := squealx.StmtQueryCursor[cursorUser](context.Background(), stmt, 1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := scur.Collect(0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}

func TestSelectEachVariants(t *testing.T) {
	db := setupCursorDB(t)
	q := "SELECT id,name FROM users ORDER BY id"
	var vals []cursorUser
	if err := squealx.SelectEach(db, func(u cursorUser) error { vals = append(vals, u); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(vals) != "[{1 Alice} {2 Bob} {3 Carol}]" {
		t.Fatalf("vals=%v", vals)
	}
	var ptrs []*cursorUser
	if err := squealx.SelectEach(db, func(u *cursorUser) error { ptrs = append(ptrs, u); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if ptrs[0] == ptrs[1] || ptrs[0].Name != "Alice" || ptrs[2].Name != "Carol" {
		t.Fatalf("ptrs not independent")
	}
	var ids []int
	if err := squealx.SelectEach(db, func(i int) error { ids = append(ids, i); return nil }, "SELECT id FROM users ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("ids=%v", ids)
	}
	var maps []map[string]any
	if err := squealx.SelectEach(db, func(m map[string]any) error { maps = append(maps, m); return nil }, q); err != nil {
		t.Fatal(err)
	}
	if len(maps) != 3 || maps[0]["name"] == maps[1]["name"] {
		t.Fatalf("maps=%v", maps)
	}
	type bad struct {
		Other int `db:"other"`
	}
	err := squealx.SelectEach(db, func(bad) error { return nil }, q)
	if err == nil {
		t.Fatal("expected missing destination error")
	}
	if err := squealx.SelectEach(db, func(any) error { return nil }, q); err == nil {
		t.Fatal("expected ambiguity error")
	}
	stop := errors.New("stop")
	if err := squealx.SelectEach(db, func(cursorUser) error { return stop }, q); !errors.Is(err, stop) {
		t.Fatalf("err=%v", err)
	}
}
