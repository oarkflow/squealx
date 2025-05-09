package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

// Updated Author struct to include related books.
type Author struct {
	AID      int    `json:"aID"`
	AuthID   int    `json:"auth_id"`
	Name     string `json:"name"`
	Lastname string `json:"lastname"`
	Books    []Book `json:"books"` // new field for preloaded books
}

type Comment struct {
	BookID      int    `json:"book_id"`
	CID         int    `json:"cid"`
	CommentText string `json:"comment_text"`
}

// New Book struct for related books.
type Book struct {
	BID      int       `json:"bID"`
	AuthID   int       `json:"auth_id"`
	BookName string    `json:"book_name"`
	Comments []Comment `json:"comments"`
}

// Optionally implement TableName for Book.
func (b Book) TableName() string {
	return "books"
}

// Implement TableName so the repository can determine the table.
func (a Author) TableName() string {
	return "authors"
}

func connectDB() (*squealx.DB, error) {
	connStr := "user=postgres password=postgres dbname=sujit port=5432 host=localhost sslmode=disable"
	db, err := postgres.Open(connStr, "postgres")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func main() {
	db, err := connectDB()
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()

	/* // Create sample tables if they don't exist.
	createAuthors := `CREATE TABLE IF NOT EXISTS authors (
		aID SERIAL PRIMARY KEY,
		auth_id INT NOT NULL,
		name VARCHAR(255),
		lastname VARCHAR(255)
	);`
	createBooks := `CREATE TABLE IF NOT EXISTS books (
		bID SERIAL PRIMARY KEY,
		auth_id INT NOT NULL,
		book_name VARCHAR(255)
	);`
	createAuthorBooks := `CREATE TABLE IF NOT EXISTS author_books (
		auth_id INT NOT NULL,
		book_id INT NOT NULL
	);`
	// New comments table
	createComments := `CREATE TABLE IF NOT EXISTS comments (
		cID SERIAL PRIMARY KEY,
		book_id INT NOT NULL,
		comment_text VARCHAR(255)
	);`
	_, err = db.Exec(createAuthors)
	if err != nil {
		log.Fatalf("Error creating authors table: %v", err)
	}
	_, err = db.Exec(createBooks)
	if err != nil {
		log.Fatalf("Error creating books table: %v", err)
	}
	_, err = db.Exec(createAuthorBooks)
	if err != nil {
		log.Fatalf("Error creating author_books table: %v", err)
	}
	_, err = db.Exec(createComments)
	if err != nil {
		log.Fatalf("Error creating comments table: %v", err)
	}

	// Insert sample data into authors table.
	insertAuthor := `INSERT INTO authors (auth_id, name, lastname)
		VALUES (1, 'John', 'Doe')
		ON CONFLICT DO NOTHING;`
	_, err = db.Exec(insertAuthor)
	if err != nil {
		log.Fatalf("Error inserting into authors: %v", err)
	}

	// Insert sample data into books table.
	insertBooks := `INSERT INTO books (auth_id, book_name)
		VALUES (1, 'Go Programming'),
		       (1, 'Advanced SQL')
		ON CONFLICT DO NOTHING;`
	_, err = db.Exec(insertBooks)
	if err != nil {
		log.Fatalf("Error inserting into books: %v", err)
	}

	// Insert sample data into author_books join table.
	// Assuming books IDs are 1 and 2.
	insertAuthorBooks := `INSERT INTO author_books (auth_id, book_id)
		VALUES (1, 1),
		       (1, 2)
		ON CONFLICT DO NOTHING;`
	_, err = db.Exec(insertAuthorBooks)
	if err != nil {
		log.Fatalf("Error inserting into author_books: %v", err)
	}

	// Insert sample data into comments table.
	insertComments := `INSERT INTO comments (book_id, comment_text)
		VALUES (1, 'Great book!'),
		       (2, 'Very informative')
		ON CONFLICT DO NOTHING;`
	_, err = db.Exec(insertComments)
	if err != nil {
		log.Fatalf("Error inserting into comments: %v", err)
	} */

	// Create repository for authors (records of type map[string]any)
	repo := squealx.New[map[string]any](db, "authors", "aID")

	// Define a relation via join table to preload related books.
	relation := squealx.Relation{
		With:                 "books",
		LocalField:           "aID", // current table field
		RelatedField:         "bID", // related table field
		JoinTable:            "author_books",
		JoinWithLocalField:   "auth_id", // field in join table corresponding to authors
		JoinWithRelatedField: "book_id", // field in join table corresponding to books
	}

	// Use Preload to set up relation(s) before querying.
	preloadedRepo := repo.Preload(relation)
	data, err := preloadedRepo.Find(context.Background(), map[string]any{
		"aID": []int{1},
	})
	fmt.Println("Map Example:", data, err)

	// --- New example with struct type ---

	// Create repository for authors with struct type.
	authorRepo := squealx.New[Author](db, "authors", "aID")
	preloadedAuthorRepo := authorRepo.Preload(relation)
	authors, err := preloadedAuthorRepo.Find(context.Background(), map[string]any{
		"aID": []int{1},
	})
	fmt.Println("Struct Example:", authors, err)

	// --- New multi-relation example with nested preload ---
	// Preload books and then nested preload comments on books using dot notation.
	relationComments := squealx.Relation{
		With: "books.comments", // nested relation: load comments for each book
		// For nested, LocalField is on the parent record (a book) and RelatedField is in comments.
		LocalField:   "bID",
		RelatedField: "book_id",
		JoinTable:    "", // direct foreign key relation
	}
	multiPreloadRepo := authorRepo.Preload(relation, relationComments)
	authors, err = multiPreloadRepo.Find(context.Background(), map[string]any{
		"aID": []int{1},
		// Filtering using related data via dot notation is supported in conditions.
		// For example, to filter authors whose books have a specific bID, one could use:
		// "books.bID": 1
	})
	fmt.Println("Multi Preload Example:", authors, err)
}
