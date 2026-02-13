package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/hooks"
)

var mysqlSchema = `
DROP TABLE IF EXISTS entries;
DROP TABLE IF EXISTS pipelines;
DROP TABLE IF EXISTS documents;

CREATE TABLE pipelines (
	pipeline_id BIGINT PRIMARY KEY AUTO_INCREMENT,
	org_id BIGINT NOT NULL,
	owner_user_id BIGINT NOT NULL,
	visibility VARCHAR(16) NOT NULL DEFAULT 'private',
	name VARCHAR(128) NOT NULL
);

CREATE TABLE entries (
	entry_id BIGINT PRIMARY KEY AUTO_INCREMENT,
	pipeline_id BIGINT NOT NULL,
	org_id BIGINT NOT NULL,
	content VARCHAR(255) NOT NULL,
	FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
);

CREATE TABLE documents (
	doc_id BIGINT PRIMARY KEY AUTO_INCREMENT,
	org_id BIGINT NOT NULL,
	classification INT NOT NULL,
	title VARCHAR(128) NOT NULL
);
`

type TenantScope struct {
	OrgID             int64
	UserID            int64
	MaxClassification int
}

type scopeKey string

const tenantKey scopeKey = "tenant_scope"

type Pipeline struct {
	PipelineID   int64  `db:"pipeline_id"`
	OrgID        int64  `db:"org_id"`
	OwnerUserID  int64  `db:"owner_user_id"`
	Visibility   string `db:"visibility"`
	Name         string `db:"name"`
}

type EntryView struct {
	EntryID   int64  `db:"entry_id"`
	Pipeline  string `db:"pipeline"`
	Content   string `db:"content"`
}

type Document struct {
	DocID          int64  `db:"doc_id"`
	OrgID          int64  `db:"org_id"`
	Classification int    `db:"classification"`
	Title          string `db:"title"`
}

func main() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		fmt.Println("Set MYSQL_DSN first, e.g. MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/cleardb?parseTime=true'")
		return
	}

	db, err := mysql.Open(dsn, "mysql-scope-example")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	db.MustExec(mysqlSchema)
	seed(db)

	defaultResolver := func(ctx context.Context) ([]any, error) {
		tenant, ok := tenantFromContext(ctx)
		if !ok {
			return nil, &hooks.ScopeError{Code: hooks.ScopeDenyMissingContext, Message: "tenant scope missing in context"}
		}
		return []any{tenant.OrgID}, nil
	}

	scope := hooks.NewResourceScopeHook(
		defaultResolver,
		hooks.ScopeRule{
			Table: "pipelines",
			Predicate: "({{alias}}.org_id = {{param}} AND ({{alias}}.owner_user_id = {{param}} OR {{alias}}.visibility = 'org'))",
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				tenant, ok := tenantFromContext(ctx)
				if !ok {
					return nil, &hooks.ScopeError{Code: hooks.ScopeDenyMissingContext, Message: "tenant scope missing in context"}
				}
				return []any{tenant.OrgID, tenant.UserID}, nil
			},
		},
		hooks.ScopeRule{
			Table: "entries",
			Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.org_id = {{param}} AND (p.owner_user_id = {{param}} OR p.visibility = 'org'))",
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				tenant, ok := tenantFromContext(ctx)
				if !ok {
					return nil, &hooks.ScopeError{Code: hooks.ScopeDenyMissingContext, Message: "tenant scope missing in context"}
				}
				return []any{tenant.OrgID, tenant.UserID}, nil
			},
		},
		hooks.ScopeRule{
			Table: "documents",
			Predicate: "({{alias}}.org_id = {{param}} AND {{alias}}.classification <= {{param}})",
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				tenant, ok := tenantFromContext(ctx)
				if !ok {
					return nil, &hooks.ScopeError{Code: hooks.ScopeDenyMissingContext, Message: "tenant scope missing in context"}
				}
				return []any{tenant.OrgID, tenant.MaxClassification}, nil
			},
		},
	).
		SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetStrictAllTables(false).
		SetAllowTrustedBypass(true).
		SetRequireBypassToken(true).
		SetBypassToken("/* scope:bypass */").
		SetAuditSink(func(_ context.Context, d hooks.ScopeDecision) {
			if d.Action == hooks.ScopeDecisionPassthrough {
				return
			}
			fmt.Printf("audit: action=%s code=%s tables=%v rules=%v\n", d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules)
		})

	db.Use(scope)

	ctxOrg10User101 := withTenant(context.Background(), TenantScope{OrgID: 10, UserID: 101, MaxClassification: 2})
	ctxOrg20User201 := withTenant(context.Background(), TenantScope{OrgID: 20, UserID: 201, MaxClassification: 5})

	fmt.Println("--- 1) SELECT auto-scope ---")
	showPipelines(db, ctxOrg10User101, "org10/user101")
	showPipelines(db, ctxOrg20User201, "org20/user201")

	fmt.Println("--- 2) JOIN + parent-child scope ---")
	var entries []EntryView
	joinQuery := `
SELECT e.entry_id, p.name AS pipeline, e.content
FROM entries e
JOIN pipelines p ON p.pipeline_id = e.pipeline_id
ORDER BY e.entry_id`
	must(db.SelectContext(ctxOrg10User101, &entries, joinQuery))
	fmt.Println("entries for org10/user101:", entries)

	fmt.Println("--- 3) Nested subquery scope ---")
	var nested []Pipeline
	nestedQuery := `
SELECT * FROM pipelines p
WHERE p.pipeline_id IN (
	SELECT e.pipeline_id FROM entries e WHERE e.content LIKE ?
)
ORDER BY p.pipeline_id`
	must(db.SelectContext(ctxOrg10User101, &nested, nestedQuery, "%started%"))
	fmt.Println("nested pipelines for org10/user101:", nested)

	fmt.Println("--- 4) CTE scope ---")
	var cteRows []Pipeline
	cteQuery := `
WITH active_entries AS (
	SELECT pipeline_id FROM entries WHERE content LIKE ?
)
SELECT p.*
FROM pipelines p
JOIN active_entries ae ON ae.pipeline_id = p.pipeline_id
ORDER BY p.pipeline_id`
	must(db.SelectContext(ctxOrg10User101, &cteRows, cteQuery, "%build%"))
	fmt.Println("cte pipelines for org10/user101:", cteRows)

	fmt.Println("--- 5) UPDATE / DELETE write-path protection ---")
	_, err = db.ExecContext(ctxOrg10User101, "UPDATE pipelines SET name=? WHERE pipeline_id=?", "cross-org-should-not-change", 4)
	must(err)
	showPipelines(db, ctxOrg20User201, "org20/user201 after cross-org update attempt")

	_, err = db.ExecContext(ctxOrg10User101, "DELETE FROM entries WHERE pipeline_id=?", 4)
	must(err)
	var verifyEntries []EntryView
	must(db.SelectContext(ctxOrg20User201, &verifyEntries, joinQuery))
	fmt.Println("org20/user201 entries after cross-org delete attempt:", verifyEntries)

	fmt.Println("--- 6) Classification (org + role-like max class) ---")
	var docs []Document
	must(db.SelectContext(ctxOrg10User101, &docs, "SELECT * FROM documents ORDER BY doc_id"))
	fmt.Println("documents for org10/user101:", docs)

	fmt.Println("--- 7) Missing context deny + deterministic code ---")
	var denied []Pipeline
	err = db.SelectContext(context.Background(), &denied, "SELECT * FROM pipelines")
	printDeny(err)

	fmt.Println("--- 8) Bypass denied (token without trusted context) ---")
	err = db.SelectContext(context.Background(), &denied, "/* scope:bypass */ SELECT * FROM pipelines")
	printDeny(err)

	fmt.Println("--- 9) Trusted bypass allowed (token + trusted context) ---")
	trustedBypassCtx := hooks.WithTrustedScopeBypass(context.Background(), "internal-maintenance")
	var bypassRows []Pipeline
	must(db.SelectContext(trustedBypassCtx, &bypassRows, "/* scope:bypass */ SELECT * FROM pipelines ORDER BY pipeline_id"))
	fmt.Println("bypassed rows:", bypassRows)

	fmt.Println("done")
}

func seed(db *squealx.DB) {
	db.MustExec("INSERT INTO pipelines (org_id, owner_user_id, visibility, name) VALUES (10, 101, 'private', 'Org10 Build A')")
	db.MustExec("INSERT INTO pipelines (org_id, owner_user_id, visibility, name) VALUES (10, 102, 'org', 'Org10 Shared Build')")
	db.MustExec("INSERT INTO pipelines (org_id, owner_user_id, visibility, name) VALUES (10, 102, 'private', 'Org10 Private B')")
	db.MustExec("INSERT INTO pipelines (org_id, owner_user_id, visibility, name) VALUES (20, 201, 'private', 'Org20 Build X')")

	db.MustExec("INSERT INTO entries (pipeline_id, org_id, content) VALUES (1, 10, 'build started')")
	db.MustExec("INSERT INTO entries (pipeline_id, org_id, content) VALUES (2, 10, 'build finished')")
	db.MustExec("INSERT INTO entries (pipeline_id, org_id, content) VALUES (4, 20, 'build started')")

	db.MustExec("INSERT INTO documents (org_id, classification, title) VALUES (10, 1, 'Org10 Public Doc')")
	db.MustExec("INSERT INTO documents (org_id, classification, title) VALUES (10, 3, 'Org10 Secret Doc')")
	db.MustExec("INSERT INTO documents (org_id, classification, title) VALUES (20, 1, 'Org20 Public Doc')")
}

func showPipelines(db interface {
	SelectContext(context.Context, any, string, ...any) error
}, ctx context.Context, label string) {
	var rows []Pipeline
	must(db.SelectContext(ctx, &rows, "SELECT * FROM pipelines ORDER BY pipeline_id"))
	fmt.Println(label+":", rows)
}

func withTenant(ctx context.Context, tenant TenantScope) context.Context {
	return context.WithValue(ctx, tenantKey, tenant)
}

func tenantFromContext(ctx context.Context) (TenantScope, bool) {
	tenant, ok := ctx.Value(tenantKey).(TenantScope)
	return tenant, ok
}

func printDeny(err error) {
	fmt.Println("error:", err)
	if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
		fmt.Println("deny code:", code)
	}
}

func must(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
