package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/sqlite"
	"github.com/oarkflow/squealx/hooks"
)

var schema = `
DROP TABLE IF EXISTS pipelines;
DROP TABLE IF EXISTS entries;

CREATE TABLE pipelines (
	pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id     INTEGER NOT NULL,
	name        TEXT NOT NULL
);

CREATE TABLE entries (
	entry_id     INTEGER PRIMARY KEY AUTOINCREMENT,
	pipeline_id  INTEGER NOT NULL,
	content      TEXT NOT NULL,
	FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
);
`

type Pipeline struct {
	PipelineID int    `db:"pipeline_id"`
	UserID     int    `db:"user_id"`
	Name       string `db:"name"`
}

type EntryView struct {
	EntryID     int    `db:"entry_id"`
	PipelineID  int    `db:"pipeline_id"`
	Pipeline    string `db:"pipeline"`
	Content     string `db:"content"`
}

type scopeKey string

const userIDKey scopeKey = "user_id"

func main() {
	_ = os.Remove("scoped_example.db")

	db, err := sqlite.Open("scoped_example.db", "example")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	db.MustExec(schema)
	seed(db)

	scope := hooks.NewResourceScopeHook(
		hooks.ArgsFromContextValue(userIDKey),
		hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
		hooks.ScopeRule{
			Table:     "entries",
			Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.user_id = {{param}})",
		},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetAllowTrustedBypass(true).
		SetRequireBypassToken(true).
		SetAuditSink(func(_ context.Context, d hooks.ScopeDecision) {
			if d.Action == hooks.ScopeDecisionPassthrough {
				return
			}
			fmt.Printf("audit: action=%s reason=%s tables=%v rules=%v\n", d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules)
		})
	db.Use(scope)

	ctxUser1 := context.WithValue(context.Background(), userIDKey, 1)
	ctxUser2 := context.WithValue(context.Background(), userIDKey, 2)

	fmt.Println("--- Query without explicit WHERE (auto-scoped) ---")
	var user1Pipelines []Pipeline
	if err := db.SelectContext(ctxUser1, &user1Pipelines, "SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=1 pipelines:", user1Pipelines)

	var user2Pipelines []Pipeline
	if err := db.SelectContext(ctxUser2, &user2Pipelines, "SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=2 pipelines:", user2Pipelines)

	fmt.Println("--- Parameterized query with existing filter (AND scope injected) ---")
	var filtered []Pipeline
	if err := db.SelectContext(ctxUser1, &filtered, "SELECT * FROM pipelines WHERE name LIKE ? ORDER BY pipeline_id", "%Build%"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=1 filtered pipelines:", filtered)

	fmt.Println("--- Join query also scoped via entries rule ---")
	var entries []EntryView
	q := `
SELECT e.entry_id, e.pipeline_id, p.name AS pipeline, e.content
FROM entries e
JOIN pipelines p ON p.pipeline_id = e.pipeline_id
ORDER BY e.entry_id`
	if err := db.SelectContext(ctxUser1, &entries, q); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=1 entries:", entries)

	fmt.Println("--- Transaction query inherits hooks ---")
	tx, err := db.BeginTxx(ctxUser1, nil)
	if err != nil {
		log.Fatalln(err)
	}
	var txRows []Pipeline
	if err := tx.SelectContext(ctxUser1, &txRows, "SELECT * FROM pipelines ORDER BY pipeline_id"); err != nil {
		_ = tx.Rollback()
		log.Fatalln(err)
	}
	_ = tx.Rollback()
	fmt.Println("user=1 tx pipelines:", txRows)

	fmt.Println("--- Strict mode blocks missing scope in context ---")
	var shouldFail []Pipeline
	err = db.SelectContext(context.Background(), &shouldFail, "SELECT * FROM pipelines")
	fmt.Println("missing user context error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}

	fmt.Println("--- Nested subquery is scoped ---")
	var nested []Pipeline
	nestedQuery := `
SELECT *
FROM pipelines p
WHERE p.pipeline_id IN (
	SELECT e.pipeline_id
	FROM entries e
	WHERE e.content LIKE ?
)
ORDER BY p.pipeline_id`
	if err := db.SelectContext(ctxUser1, &nested, nestedQuery, "%started%"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=1 nested query pipelines:", nested)

	fmt.Println("--- CTE main statement is scoped ---")
	var cte []Pipeline
	cteQuery := `
WITH candidate AS (
	SELECT pipeline_id FROM entries WHERE content LIKE ?
)
SELECT p.*
FROM pipelines p
JOIN candidate c ON c.pipeline_id = p.pipeline_id
ORDER BY p.pipeline_id`
	if err := db.SelectContext(ctxUser1, &cte, cteQuery, "%build%"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=1 CTE pipelines:", cte)

	fmt.Println("--- UPDATE and DELETE are scoped ---")
	if _, err := db.ExecContext(ctxUser1, "UPDATE pipelines SET name = ? WHERE pipeline_id = ?", "Build Main (u1)", 1); err != nil {
		log.Fatalln(err)
	}
	if _, err := db.ExecContext(ctxUser1, "UPDATE pipelines SET name = ? WHERE pipeline_id = ?", "Build Side (blocked)", 3); err != nil {
		log.Fatalln(err)
	}
	var check []Pipeline
	if err := db.SelectContext(ctxUser2, &check, "SELECT * FROM pipelines WHERE pipeline_id = 3"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=2 pipeline after user=1 update attempt:", check)

	if _, err := db.ExecContext(ctxUser1, "DELETE FROM entries WHERE pipeline_id = ?", 3); err != nil {
		log.Fatalln(err)
	}
	var verifyDelete []EntryView
	if err := db.SelectContext(ctxUser2, &verifyDelete, q); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("user=2 entries after user=1 delete attempt:", verifyDelete)

	fmt.Println("--- Trusted bypass (token + context) ---")
	bypassCtx := hooks.WithTrustedScopeBypass(context.Background(), "internal-maintenance")
	var bypassRows []Pipeline
	err = db.SelectContext(bypassCtx, &bypassRows, "/* scope:bypass */ SELECT * FROM pipelines ORDER BY pipeline_id")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("bypassed rows:", bypassRows)

	fmt.Println("--- Bypass token without trusted context (denied) ---")
	var bypassDenied []Pipeline
	err = db.SelectContext(context.Background(), &bypassDenied, "/* scope:bypass */ SELECT * FROM pipelines")
	fmt.Println("error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}
}

func seed(db *squealx.DB) {
	db.MustExec("INSERT INTO pipelines (user_id, name) VALUES (?, ?)", 1, "Build Main")
	db.MustExec("INSERT INTO pipelines (user_id, name) VALUES (?, ?)", 1, "Deploy Main")
	db.MustExec("INSERT INTO pipelines (user_id, name) VALUES (?, ?)", 2, "Build Side")

	db.MustExec("INSERT INTO entries (pipeline_id, content) VALUES (?, ?)", 1, "build started")
	db.MustExec("INSERT INTO entries (pipeline_id, content) VALUES (?, ?)", 1, "build finished")
	db.MustExec("INSERT INTO entries (pipeline_id, content) VALUES (?, ?)", 3, "side build started")
}
