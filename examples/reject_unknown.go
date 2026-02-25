package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/squealx/drivers/sqlite"
	"github.com/oarkflow/squealx/hooks"
)

type scopeKey string

const userIDKey scopeKey = "user_id"

var schemaReject = `
DROP TABLE IF EXISTS pipelines;
DROP TABLE IF EXISTS audits;
CREATE TABLE pipelines (
	pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
	user_id     INTEGER NOT NULL,
	name        TEXT NOT NULL
);

CREATE TABLE audits (
	audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
	message  TEXT NOT NULL
);
`

type Pipeline struct {
	PipelineID int    `db:"pipeline_id"`
	UserID     int    `db:"user_id"`
	Name       string `db:"name"`
}

func main() {
	_ = os.Remove("reject_unknown.db")
	db, err := sqlite.Open("reject_unknown.db", "reject")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	db.MustExec(schemaReject)
	db.MustExec("INSERT INTO pipelines (user_id, name) VALUES (?, ?)", 1, "Build Main")
	db.MustExec("INSERT INTO pipelines (user_id, name) VALUES (?, ?)", 2, "Build Side")
	db.MustExec("INSERT INTO audits (message) VALUES (?)", "seed")

	scope := hooks.NewResourceScopeHook(
		hooks.ArgsFromContextValue(userIDKey),
		hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetAllowTrustedBypass(true).
		SetRequireBypassToken(true).
		SetAuditSink(func(_ context.Context, d hooks.ScopeDecision) {
			fmt.Printf("audit: action=%s reason_code=%s tables=%v rules=%v\n", d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules)
		})
	db.Use(scope)

	ctx := context.WithValue(context.Background(), userIDKey, 1)

	fmt.Println("--- Positive (allowed) ---")
	var okRows []Pipeline
	if err := db.SelectContext(ctx, &okRows, "SELECT * FROM pipelines"); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("scoped rows:", okRows)

	fmt.Println("--- Negative (missing context) ---")
	var noCtxRows []Pipeline
	err = db.SelectContext(context.Background(), &noCtxRows, "SELECT * FROM pipelines")
	fmt.Println("error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}

	fmt.Println("--- Negative (bypass token without trusted context) ---")
	var bypassRows []Pipeline
	err = db.SelectContext(context.Background(), &bypassRows, "/* scope:bypass */ SELECT * FROM pipelines")
	fmt.Println("error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}

	fmt.Println("--- Positive (trusted bypass + token) ---")
	bypassCtx := hooks.WithTrustedScopeBypass(context.Background(), "internal-debug")
	var bypassOK []Pipeline
	err = db.SelectContext(bypassCtx, &bypassOK, "/* scope:bypass */ SELECT * FROM pipelines ORDER BY pipeline_id")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("bypassed rows (unscoped):", bypassOK)

	fmt.Println("--- Negative (strict all tables missing rule) ---")
	scope2 := hooks.NewResourceScopeHook(
		hooks.ArgsFromContextValue(userIDKey),
		hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true).SetStrictAllTables(true)

	db2, err := sqlite.Open("reject_unknown.db", "reject-2")
	if err != nil {
		log.Fatalln(err)
	}
	defer db2.Close()
	db2.Use(scope2)

	var strictRows []Pipeline
	err = db2.SelectContext(ctx, &strictRows, "SELECT p.* FROM pipelines p JOIN audits a ON 1=1")
	fmt.Println("error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}
}
