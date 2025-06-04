// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oarkflow/squealx"
	_ "modernc.org/sqlite"
)

// ────────────────────────────────────────────────────────────────────────────────
// Example entity:
type User struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {
	ctx := context.Background()

	// ------ PostgreSQL example ------
	pgDSN := "postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable"
	pgDB, err := squealx.Open("pgx", pgDSN, "test")
	if err != nil {
		log.Fatalf("Postgres connect error: %v", err)
	}
	defer pgDB.Close()

	// Create table if not exists (including created_at):
	pgCreate := `
	CREATE TABLE IF NOT EXISTS users (
	  id         SERIAL PRIMARY KEY,
	  name       TEXT NOT NULL,
	  email      TEXT NOT NULL UNIQUE,
	  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`
	if _, err := pgDB.ExecContext(ctx, pgCreate); err != nil {
		log.Fatalf("Postgres create table: %v", err)
	}

	// INSERT with RETURNING (native):
	u1 := &User{Name: "Alice", Email: "alice@example.com"}
	if err := pgDB.ExecWithReturn("INSERT INTO users (name, email) VALUES (:name, :email)", u1); err != nil {
		log.Fatalf("Postgres INSERT ExecWithReturn: %v", err)
	}
	fmt.Printf("Postgres Inserted → %+v\n", u1)

	// UPDATE with RETURNING:
	u1.Email = "alice+pg@pg.com"
	if err := pgDB.ExecWithReturn("UPDATE users SET email = :email WHERE id = :id", u1); err != nil {
		log.Fatalf("Postgres UPDATE ExecWithReturn: %v", err)
	}
	fmt.Printf("Postgres Updated  → %+v\n", u1)

	// DELETE with RETURNING:
	uDel := &User{ID: u1.ID}
	if err := pgDB.ExecWithReturn("DELETE FROM users WHERE id = :id", uDel); err != nil {
		log.Fatalf("Postgres DELETE ExecWithReturn: %v", err)
	}
	fmt.Printf("Postgres Deleted  → %+v\n\n", uDel)

	// ------ MySQL example ------
	myDSN := "root:root@tcp(127.0.0.1:3306)/testdb?parseTime=true"
	myDB, err := squealx.Open("mysql", myDSN, "test")
	if err != nil {
		log.Fatalf("MySQL connect error: %v", err)
	}
	defer myDB.Close()

	myCreate := `
	CREATE TABLE IF NOT EXISTS users (
	  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
	  name       VARCHAR(255) NOT NULL,
	  email      VARCHAR(255) NOT NULL UNIQUE,
	  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB;
	`
	if _, err := myDB.ExecContext(ctx, myCreate); err != nil {
		log.Fatalf("MySQL create table: %v", err)
	}

	// INSERT fallback: Exec + LastInsertId + SELECT:
	u2 := &User{Name: "Bob", Email: "bob@example.com"}
	if err := myDB.ExecWithReturn("INSERT INTO users (name, email) VALUES (:name, :email)", u2); err != nil {
		log.Fatalf("MySQL INSERT ExecWithReturn: %v", err)
	}
	fmt.Printf("MySQL Inserted → %+v\n", u2)

	// UPDATE fallback: Exec + SELECT:
	u2.Email = "bob+mysql@mysql.com"
	if err := myDB.ExecWithReturn("UPDATE users SET email = :email WHERE id = :id", u2); err != nil {
		log.Fatalf("MySQL UPDATE ExecWithReturn: %v", err)
	}
	fmt.Printf("MySQL Updated  → %+v\n", u2)

	// DELETE fallback: fetch before delete + NamedExec:
	uDel2 := &User{ID: u2.ID}
	if err := myDB.ExecWithReturn("DELETE FROM users WHERE id = :id", uDel2); err != nil {
		log.Fatalf("MySQL DELETE ExecWithReturn: %v", err)
	}
	fmt.Printf("MySQL Deleted  → %+v\n\n", uDel2)

	// ------ SQLite example ------
	sqliteDSN := "file:demo.db?cache=shared&mode=rwc"
	sqliteDB, err := squealx.Open("sqlite", sqliteDSN, "test")
	if err != nil {
		log.Fatalf("SQLite connect error: %v", err)
	}
	defer sqliteDB.Close()

	sqliteCreate := `
	CREATE TABLE IF NOT EXISTS users (
	  id         INTEGER PRIMARY KEY AUTOINCREMENT,
	  name       TEXT    NOT NULL,
	  email      TEXT    NOT NULL UNIQUE,
	  created_at DATETIME NOT NULL DEFAULT (datetime('now'))
	);
	`
	if _, err := sqliteDB.ExecContext(ctx, sqliteCreate); err != nil {
		log.Fatalf("SQLite create table: %v", err)
	}

	// INSERT fallback: Exec + LastInsertId + SELECT:
	u3 := &User{Name: "Carol", Email: "carol@example.com"}
	if err := sqliteDB.ExecWithReturn("INSERT INTO users (name, email) VALUES (:name, :email)", u3); err != nil {
		log.Fatalf("SQLite INSERT ExecWithReturn: %v", err)
	}
	fmt.Printf("SQLite Inserted → %+v\n", u3)

	// UPDATE fallback: Exec + SELECT:
	u3.Email = "carol+sqlite@sqlite.com"
	if err := sqliteDB.ExecWithReturn("UPDATE users SET email = :email WHERE id = :id", u3); err != nil {
		log.Fatalf("SQLite UPDATE ExecWithReturn: %v", err)
	}
	fmt.Printf("SQLite Updated  → %+v\n", u3)

	// DELETE fallback: fetch before delete + NamedExec:
	uDel3 := &User{ID: u3.ID}
	if err := sqliteDB.ExecWithReturn("DELETE FROM users WHERE id = :id", uDel3); err != nil {
		log.Fatalf("SQLite DELETE ExecWithReturn: %v", err)
	}
	fmt.Printf("SQLite Deleted  → %+v\n", uDel3)
}
