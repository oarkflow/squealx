package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/oarkflow/squealx"
)

type keysetRow struct {
	ID    int    `db:"id"`
	Grp   int    `db:"grp"`
	Label string `db:"label"`
}

func newKeysetDB(t *testing.T, name string) *squealx.DB {
	t.Helper()
	db, err := Open(":memory:", name)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.MustExec(`CREATE TABLE items (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, label TEXT NOT NULL)`)
	for i := 1; i <= 23; i++ {
		db.MustExec(`INSERT INTO items (id, grp, label) VALUES (?, ?, ?)`, i, i%3, "item")
	}
	return db
}

func traverse(t *testing.T, db *squealx.DB, query string, keys []squealx.KeyColumn, limit int, params map[string]any) []keysetRow {
	t.Helper()
	var all []keysetRow
	after := ""
	for i := 0; i < 100; i++ {
		page, err := squealx.KeysetPaginate[keysetRow](context.Background(), db, query, keys, limit, after, params)
		if err != nil {
			t.Fatalf("keyset paginate: %v", err)
		}
		if len(page.Items) > limit {
			t.Fatalf("page too large: %d", len(page.Items))
		}
		all = append(all, page.Items...)
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("cursor set on last page")
			}
			return all
		}
		after = page.NextCursor
	}
	t.Fatal("did not terminate")
	return nil
}

func checkExactlyOnce(t *testing.T, rows []keysetRow, want int) {
	t.Helper()
	seen := map[int]bool{}
	for _, r := range rows {
		if seen[r.ID] {
			t.Fatalf("duplicate id %d", r.ID)
		}
		seen[r.ID] = true
	}
	if len(seen) != want {
		t.Fatalf("expected %d rows, got %d", want, len(seen))
	}
}

func TestKeysetPaginateSingleKey(t *testing.T) {
	db := newKeysetDB(t, "keyset-single")
	rows := traverse(t, db, "SELECT * FROM items", []squealx.KeyColumn{{Name: "id"}}, 5, nil)
	checkExactlyOnce(t, rows, 23)
	for i, r := range rows {
		if r.ID != i+1 {
			t.Fatalf("order broken at %d: %d", i, r.ID)
		}
	}
}

func TestKeysetPaginateDescAndParams(t *testing.T) {
	db := newKeysetDB(t, "keyset-desc")
	rows := traverse(t, db, "SELECT * FROM items WHERE id > :min", []squealx.KeyColumn{{Name: "id", Desc: true}}, 4, map[string]any{"min": 3})
	checkExactlyOnce(t, rows, 20)
	if rows[0].ID != 23 || rows[len(rows)-1].ID != 4 {
		t.Fatalf("unexpected order: first=%d last=%d", rows[0].ID, rows[len(rows)-1].ID)
	}
}

func TestKeysetPaginateDuplicateFirstKey(t *testing.T) {
	db := newKeysetDB(t, "keyset-dup")
	for _, keys := range [][]squealx.KeyColumn{
		{{Name: "grp"}, {Name: "id"}},
		{{Name: "grp", Desc: true}, {Name: "id", Desc: true}},
		{{Name: "grp"}, {Name: "id", Desc: true}},
	} {
		rows := traverse(t, db, "SELECT * FROM items", keys, 4, nil)
		checkExactlyOnce(t, rows, 23)
		for i := 1; i < len(rows); i++ {
			a, b := rows[i-1], rows[i]
			if keys[0].Desc && a.Grp < b.Grp || !keys[0].Desc && a.Grp > b.Grp {
				t.Fatalf("grp order broken: %v then %v", a, b)
			}
			if a.Grp == b.Grp && (keys[1].Desc && a.ID < b.ID || !keys[1].Desc && a.ID > b.ID) {
				t.Fatalf("id order broken: %v then %v", a, b)
			}
		}
	}
}

func TestKeysetPaginateMapRows(t *testing.T) {
	db := newKeysetDB(t, "keyset-map")
	var n int
	after := ""
	for i := 0; i < 10; i++ {
		page, err := squealx.KeysetPaginate[map[string]any](context.Background(), db, "SELECT * FROM items", []squealx.KeyColumn{{Name: "grp"}, {Name: "id"}}, 10, after, nil)
		if err != nil {
			t.Fatal(err)
		}
		n += len(page.Items)
		if !page.HasMore {
			break
		}
		after = page.NextCursor
	}
	if n != 23 {
		t.Fatalf("expected 23, got %d", n)
	}
}

func TestKeysetPaginateErrors(t *testing.T) {
	db := newKeysetDB(t, "keyset-err")
	ctx := context.Background()
	keys := []squealx.KeyColumn{{Name: "id"}}
	if _, err := squealx.KeysetPaginate[keysetRow](ctx, db, "SELECT * FROM items", keys, 5, "###", nil); err == nil {
		t.Fatal("expected malformed cursor error")
	}
	if _, err := squealx.KeysetPaginate[keysetRow](ctx, db, "SELECT * FROM items", []squealx.KeyColumn{{Name: "id; x"}}, 5, "", nil); err == nil {
		t.Fatal("expected identifier error")
	}
	two, _ := squealx.EncodeKeysetCursor([]any{1, 2})
	if _, err := squealx.KeysetPaginate[keysetRow](ctx, db, "SELECT * FROM items", keys, 5, two, nil); err == nil {
		t.Fatal("expected cursor arity error")
	}
}

func TestKeysetPaginateRejectsNullBoundaryKey(t *testing.T) {
	db := newKeysetDB(t, "keyset-null-boundary")
	_, err := squealx.KeysetPaginate[map[string]any](
		context.Background(), db,
		"SELECT id, NULL AS nullable_key FROM items",
		[]squealx.KeyColumn{{Name: "nullable_key"}, {Name: "id"}},
		1, "", nil,
	)
	if !errors.Is(err, squealx.ErrInvalidKeysetRow) {
		t.Fatalf("keyset page error = %v, want ErrInvalidKeysetRow", err)
	}
}

func BenchmarkKeysetVsOffsetDeepPage(b *testing.B) {
	db, err := Open(":memory:", "keyset-deep-benchmark")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, label TEXT NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO items (id, grp, label) VALUES (?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	for id := 1; id <= 50000; id++ {
		if _, err := stmt.Exec(id, id%10, "item"); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			b.Fatal(err)
		}
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	after, err := squealx.EncodeKeysetCursor([]any{int64(49000)})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Offset", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var rows []keysetRow
			if err := db.Select(&rows, `SELECT * FROM items ORDER BY id LIMIT ? OFFSET ?`, 101, 49000); err != nil {
				b.Fatal(err)
			}
			if len(rows) != 101 {
				b.Fatalf("loaded %d offset rows", len(rows))
			}
		}
	})
	b.Run("Keyset", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			page, err := squealx.KeysetPaginate[keysetRow](context.Background(), db,
				"SELECT * FROM items", []squealx.KeyColumn{{Name: "id"}}, 100, after, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(page.Items) != 100 || !page.HasMore {
				b.Fatalf("keyset page rows=%d hasMore=%t", len(page.Items), page.HasMore)
			}
		}
	})
}
