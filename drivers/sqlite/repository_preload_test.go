package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/oarkflow/squealx"
)

type preloadBenchOrder struct {
	ID     int `db:"id" json:"id"`
	UserID int `db:"user_id" json:"user_id"`
}

type preloadBenchUser struct {
	ID     int                 `db:"id" json:"id"`
	Orders []preloadBenchOrder `json:"orders"`
}

func TestRepositoryPreloadBatchesLargeKeySets(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "preload-batch-test.sqlite"), "preload-batch-test")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 501; id++ {
		if _, err := tx.Exec("INSERT INTO users(id) VALUES (?)", id); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec("INSERT INTO orders(id,user_id) VALUES (?,?)", 1, 501); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	repo := squealx.New[map[string]any](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	})
	users, err := repo.Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 501 {
		t.Fatalf("loaded %d users, want 501", len(users))
	}
	foundParent := false
	for _, user := range users {
		if fmt.Sprint(user["id"]) != "501" {
			continue
		}
		orders, ok := user["orders"].([]any)
		if !ok || len(orders) != 1 {
			t.Fatalf("preloaded orders for user 501 in row %#v", user)
		}
		order, ok := orders[0].(map[string]any)
		if !ok || fmt.Sprint(order["user_id"]) != "501" {
			t.Fatalf("unexpected preloaded order: %#v", orders[0])
		}
		foundParent = true
		break
	}
	if !foundParent {
		t.Fatal("user 501 missing from query result")
	}

	cursor, err := squealx.FindPreloadedCursor(context.Background(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	count := 0
	foundStreamParent := false
	var retainedFirst map[string]any
	for cursor.Next() {
		user := cursor.Value()
		count++
		if count == 1 {
			retainedFirst = user
		}
		if fmt.Sprint(user["id"]) != "501" {
			continue
		}
		orders, ok := user["orders"].([]any)
		if !ok || len(orders) != 1 {
			t.Fatalf("streamed orders for user 501 in row %#v", user)
		}
		order, ok := orders[0].(map[string]any)
		if !ok || fmt.Sprint(order["user_id"]) != "501" {
			t.Fatalf("unexpected streamed order: %#v", orders[0])
		}
		foundStreamParent = true
	}
	if err := cursor.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 501 || !foundStreamParent {
		t.Fatalf("streamed %d parents, user 501 found=%t", count, foundStreamParent)
	}
	if fmt.Sprint(retainedFirst["id"]) != "1" {
		t.Fatalf("retained streamed value changed after advancing: %#v", retainedFirst)
	}

	limited, err := squealx.FindPreloadedCursorConfig(context.Background(), repo, nil, squealx.CursorConfig{MaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if !limited.Next() || !limited.Next() || limited.Next() {
		t.Fatalf("preloaded cursor did not enforce MaxRows; err=%v", limited.Err())
	}
	if !errors.Is(limited.Err(), squealx.ErrCursorLimit) {
		t.Fatalf("preloaded cursor limit error = %v", limited.Err())
	}

	smallBatches, err := squealx.FindPreloadedCursorWithConfig(context.Background(), repo, nil,
		squealx.PreloadedCursorConfig{BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for smallBatches.Next() {
		count++
	}
	if err := smallBatches.Err(); err != nil || count != 501 {
		t.Fatalf("custom batch cursor rows=%d err=%v", count, err)
	}
	if _, err := squealx.FindPreloadedCursorWithConfig(context.Background(), repo, nil,
		squealx.PreloadedCursorConfig{BatchSize: -1}); !errors.Is(err, squealx.ErrInvalidCursorConfig) {
		t.Fatalf("negative preload batch size error = %v", err)
	}

	callbackErr := errors.New("stop consuming")
	viaEach, err := squealx.FindPreloadedCursor(context.Background(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := viaEach.Each(func(map[string]any) error { return callbackErr }); !errors.Is(err, callbackErr) {
		t.Fatalf("preloaded Each error = %v, want %v", err, callbackErr)
	}
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("preloaded Each left %d connections in use", stats.InUse)
	}

	viaAll, err := squealx.FindPreloadedCursor(context.Background(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	for row, err := range viaAll.All() {
		if err != nil {
			t.Fatal(err)
		}
		if row["id"] == nil {
			t.Fatalf("preloaded iterator yielded empty row: %#v", row)
		}
		visited++
		break
	}
	if visited != 1 {
		t.Fatalf("preloaded iterator visited %d rows before break", visited)
	}
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("early iterator break left %d connections in use", stats.InUse)
	}
}

func TestRepositoryPreloadMapPreservesDriverValues(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "preload-map-values.sqlite"), "preload-map-values")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);
		INSERT INTO users VALUES (1, 'Ada');
		INSERT INTO orders VALUES (7, 1);
	`); err != nil {
		t.Fatal(err)
	}
	users, err := squealx.New[map[string]any](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	}).Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 {
		t.Fatalf("loaded %d users, want 1", len(users))
	}
	if _, ok := users[0]["id"].(int64); !ok {
		t.Fatalf("driver-native id type = %T, want int64", users[0]["id"])
	}
	orders, ok := users[0]["orders"].([]any)
	if !ok || len(orders) != 1 {
		t.Fatalf("preloaded orders = %#v, want one-element []any", users[0]["orders"])
	}
	order, ok := orders[0].(map[string]any)
	if !ok || order["id"] != int64(7) {
		t.Fatalf("preloaded order = %#v, want map with int64 id", orders[0])
	}
}

func TestRepositoryPreloadEmptyResultsRemainNil(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "preload-empty.sqlite"), "preload-empty")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);
	`); err != nil {
		t.Fatal(err)
	}
	repo := squealx.New[preloadBenchUser](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	})
	found, err := repo.Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("Find with no rows returned %#v, want nil", found)
	}
	all, err := repo.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if all != nil {
		t.Fatalf("All with no rows returned %#v, want nil", all)
	}
}

func BenchmarkRepositoryPreloadRead1000(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "repository-preload-benchmark.sqlite"), "preload-benchmark")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);`); err != nil {
		b.Fatal(err)
	}
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	for id := 1; id <= 1000; id++ {
		if _, err := tx.Exec("INSERT INTO users(id) VALUES (?)", id); err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO orders(id,user_id) VALUES (?,?)", id, id); err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	repo := squealx.New[map[string]any](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	})
	structRepo := squealx.New[preloadBenchUser](db, "users", "id").Preload(squealx.Relation{
		With: "orders", LocalField: "id", RelatedField: "user_id",
	})
	b.Run("Find", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := repo.Find(context.Background(), nil)
			if err != nil || len(rows) != 1000 {
				b.Fatalf("rows=%d err=%v", len(rows), err)
			}
		}
	})
	b.Run("PreloadedCursor", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cursor, err := squealx.FindPreloadedCursor(context.Background(), repo, nil)
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for cursor.Next() {
				count++
				_ = cursor.Value()["id"]
			}
			if err := cursor.Err(); err != nil || count != 1000 {
				b.Fatalf("rows=%d err=%v", count, err)
			}
		}
	})
	b.Run("Struct/Find", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := structRepo.Find(context.Background(), nil)
			if err != nil || len(rows) != 1000 {
				b.Fatalf("rows=%d err=%v", len(rows), err)
			}
		}
	})
	b.Run("Struct/PreloadedCursor", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cursor, err := squealx.FindPreloadedCursor(context.Background(), structRepo, nil)
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for cursor.Next() {
				count++
				_ = cursor.Value().ID
			}
			if err := cursor.Err(); err != nil || count != 1000 {
				b.Fatalf("rows=%d err=%v", count, err)
			}
		}
	})
}
