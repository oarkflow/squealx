package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/squealx/drivers/sqlite"
)

type NamedInUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func main() {
	db, err := sqlite.Open(":memory:", "named-in-example")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);

		INSERT INTO users (id, name)
		VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave');
	`)

	params := map[string]any{
		"ids": []int{1, 3, 4},
	}

	var withoutParentheses []NamedInUser
	if err := db.NamedSelect(&withoutParentheses, `
		SELECT id, name
		FROM users
		WHERE id IN :ids
		ORDER BY id
	`, params); err != nil {
		log.Fatalln("IN :ids:", err)
	}
	fmt.Println("IN :ids", withoutParentheses)

	var withParentheses []NamedInUser
	if err := db.NamedSelect(&withParentheses, `
		SELECT id, name
		FROM users
		WHERE id IN (:ids)
		ORDER BY id
	`, params); err != nil {
		log.Fatalln("IN (:ids):", err)
	}
	fmt.Println("IN (:ids)", withParentheses)
}
