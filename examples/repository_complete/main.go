package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/squealx"
	_ "modernc.org/sqlite"
)

type Author struct {
	AuthorID  int        `db:"id" json:"id"`
	Name      string     `db:"name" json:"name"`
	Active    bool       `db:"active" json:"active"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at"`
}

func (Author) TableName() string  { return "authors" }
func (Author) PrimaryKey() string { return "id" }
func (a Author) ID() string       { return fmt.Sprintf("%d", a.AuthorID) }

func (a *Author) BeforeCreate(*squealx.DB) error {
	now := time.Now().UTC()
	a.CreatedAt = now
	a.UpdatedAt = now
	return nil
}

func (a *Author) BeforeUpdate(*squealx.DB) error {
	a.UpdatedAt = time.Now().UTC()
	return nil
}

func main() {
	ctx := context.Background()
	db, err := squealx.Open("sqlite", ":memory:", "repository-complete")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mustExec(db, `
		CREATE TABLE authors (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			deleted_at TIMESTAMP
		);
		CREATE TABLE books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL
		);
		CREATE TABLE author_books (
			author_id INTEGER NOT NULL,
			book_id INTEGER NOT NULL
		);
		CREATE TABLE comments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			book_id INTEGER NOT NULL,
			body TEXT NOT NULL
		);
	`)

	authors := squealx.New[Author](db, "authors", "id")
	author := &Author{Name: "Ada Lovelace", Active: true}
	if err := authors.Create(ctx, author); err != nil {
		log.Fatal(err)
	}
	if err := authors.Create(ctx, &Author{Name: "Grace Hopper", Active: true}); err != nil {
		log.Fatal(err)
	}

	mustExec(db, `
		INSERT INTO books (id, title) VALUES (1, 'Analytical Engines'), (2, 'Compilers');
		INSERT INTO author_books (author_id, book_id) VALUES (1, 1), (2, 2);
		INSERT INTO comments (book_id, body) VALUES (1, 'Visionary'), (2, 'Practical');
	`)

	selectedCtx := squealx.WithQueryParams(ctx, squealx.QueryParams{
		Fields: []string{"id", "name"},
		Sort:   squealx.Sort{Field: "name", Dir: "asc"},
		Limit:  10,
		AllowedFields: map[string]string{
			"id":   "id",
			"name": "name",
		},
	})
	list, err := authors.Find(selectedCtx, map[string]any{"active": true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("selected authors: %d\n", len(list))

	if err := authors.Update(ctx, map[string]any{
		"name":       "Augusta Ada King",
		"updated_at": squealx.Expr("CURRENT_TIMESTAMP"),
	}, map[string]any{"id": 1}); err != nil {
		log.Fatal(err)
	}

	joinedCtx := squealx.WithQueryParams(ctx, squealx.QueryParams{
		Fields:  []string{"id", "name", "book_count"},
		Join:    []string{"books"},
		GroupBy: []string{"id", "name"},
		Having:  "has_books",
		Sort:    squealx.Sort{Field: "name", Dir: "asc"},
		AllowedFields: map[string]string{
			"id":         "authors.id",
			"name":       "authors.name",
			"book_count": "COUNT(books.id) AS book_count",
		},
		AllowedJoins: map[string]string{
			"books": "LEFT JOIN author_books ab ON ab.author_id = authors.id LEFT JOIN books ON books.id = ab.book_id",
		},
		AllowedHaving: map[string]string{
			"has_books": "COUNT(books.id) > 0",
		},
	})
	rows, err := squealx.New[map[string]any](db, "authors", "id").Find(joinedCtx, map[string]any{"authors.active": true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("joined rows: %d\n", len(rows))

	books := squealx.Relation{
		With:                 "books",
		LocalField:           "id",
		RelatedField:         "id",
		JoinTable:            "author_books",
		JoinWithLocalField:   "author_id",
		JoinWithRelatedField: "book_id",
	}
	comments := squealx.Relation{
		With:         "books.comments",
		LocalField:   "id",
		RelatedField: "book_id",
	}
	preloaded, err := squealx.New[map[string]any](db, "authors", "id").
		Preload(books).
		Preload(comments).
		Find(ctx, map[string]any{"id": []int{1, 2}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("preloaded authors: %d\n", len(preloaded))

	rawCtx := squealx.WithQueryParams(ctx, squealx.QueryParams{
		AllowedRaw: map[string]string{
			"active_author_names": "SELECT id, name, active, created_at, updated_at, deleted_at FROM authors WHERE active = :active ORDER BY name",
		},
	})
	rawAuthors, err := authors.Raw(rawCtx, "active_author_names", map[string]any{"active": true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("raw allowlisted authors: %d\n", len(rawAuthors))

	page := authors.Paginate(ctx, squealx.Paging{Page: 1, Limit: 1, OrderBy: []string{"id asc"}}, map[string]any{"active": true})
	if page.Error != nil {
		log.Fatal(page.Error)
	}
	fmt.Printf("page total: %d\n", page.Pagination.TotalRecords)

	if err := authors.SoftDelete(ctx, map[string]any{"id": 2}); err != nil {
		log.Fatal(err)
	}
	count, err := authors.Count(ctx, map[string]any{"deleted_at": nil})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("not deleted: %d\n", count)
}

func mustExec(db *squealx.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}
