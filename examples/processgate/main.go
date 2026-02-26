// Package main demonstrates the ProcessGate scope hook.
//
// Callers write plain SQL. The hook rewrites every query in-flight to inject
// the correct predicates. The only required values are TenantID and UserID;
// everything else is optional.
//
// Run with:
//
//	PG_DSN='postgres://user:pass@localhost/processgate' go run .
package main

import (
	"context"
	"examples/processgate/scope"
	"fmt"
	"log"

	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/hooks"
)

// ── Domain models ─────────────────────────────────────────────────────────────

type Pipeline struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	Name     string `db:"name"`
	Slug     string `db:"slug"`
}

type Entry struct {
	ID           string  `db:"id"`
	TenantID     string  `db:"tenant_id"`
	PipelineID   string  `db:"pipeline_id"`
	GlobalStatus string  `db:"global_status"`
	WorkspaceID  *string `db:"workspace_id"`
}

type Page struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	Name     string `db:"name"`
}

type AuditRow struct {
	ID           string `db:"id"`
	ResourceType string `db:"resource_type"`
	Action       string `db:"action"`
	ActorID      string `db:"actor_id"`
}

type EntryView struct {
	EntryID      string `db:"entry_id"`
	PipelineName string `db:"pipeline_name"`
	StageName    string `db:"stage_name"`
	Status       string `db:"status"`
}

type Notification struct {
	ID      string `db:"id"`
	Type    string `db:"type"`
	Subject string `db:"subject"`
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	dsn := "postgres://postgres:postgres@localhost/processgate"
	// dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		fmt.Println("Set PG_DSN, e.g. PG_DSN='postgres://user:pass@localhost/processgate'")
		return
	}

	db, err := postgres.Open(dsn, "processgate-scope-example")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	tenantAID := "8eaec119-be10-427f-87b2-f2c99e46fcac"
	// Register the hook once at startup. Every query is auto-rewritten.
	db.Use(scope.NewProcessGateScopeHook())

	// Alice – minimal scope: member-only pipeline visibility, own audit rows only.
	aliceCtx := scope.WithRequestScope(context.Background(),
		scope.NewRequestScope(tenantAID, "user-alice-uuid"),
	)

	var alicePipelines []map[string]any
	must(db.SelectContext(aliceCtx, &alicePipelines, `SELECT * FROM forms`))
	fmt.Printf("  Alice (no options): %d form(s) – only forms she is a member of\n", len(alicePipelines))
}

func update() {
	dsn := "postgres://postgres:postgres@localhost/processgate"
	// dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		fmt.Println("Set PG_DSN, e.g. PG_DSN='postgres://user:pass@localhost/processgate'")
		return
	}

	db, err := postgres.Open(dsn, "processgate-scope-example")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	// Register the hook once at startup. Every query is auto-rewritten.
	db.Use(scope.NewProcessGateScopeHook())

	// Alice – minimal scope: member-only pipeline visibility, own audit rows only.
	aliceCtx := scope.WithRequestScope(context.Background(),
		scope.NewRequestScope("tenant-a-uuid", "user-alice-uuid"),
	)

	// Bob – pipeline bypass (confirmed by caller's auth layer) + full audit access.
	bobCtx := scope.WithRequestScope(context.Background(),
		scope.NewRequestScope("tenant-a-uuid", "user-bob-uuid",
			scope.WithPipelineBypass(),
			scope.WithAuditReadAll(),
		),
	)

	// Carol – different tenant entirely.
	carolCtx := scope.WithRequestScope(context.Background(),
		scope.NewRequestScope("tenant-b-uuid", "user-carol-uuid"),
	)

	// Dave – workspace-restricted session: only sees entries in one workspace.
	daveCtx := scope.WithRequestScope(context.Background(),
		scope.NewRequestScope("tenant-a-uuid", "user-dave-uuid",
			scope.WithWorkspaceID("workspace-1-uuid"),
		),
	)

	sep := func(n int, label string) {
		fmt.Printf("\n%s\n%d. %s\n%s\n", bar(), n, label, bar())
	}

	// ─────────────────────────────────────────────────────────────────────────
	sep(1, "MINIMAL SCOPE – only TenantID + UserID required")
	//
	// Alice passes no options at all. She gets member-only pipeline visibility.
	// No role name or permission was specified – the predicate handles it in SQL.
	fmt.Println("  SQL:  SELECT * FROM pipelines ORDER BY name")
	fmt.Println("  Gets: WHERE (tenant_id=$1 AND deleted_at IS NULL AND (bypass OR membership OR no-members))")

	var alicePipelines []Pipeline
	must(db.SelectContext(aliceCtx, &alicePipelines, `SELECT * FROM pipelines ORDER BY name`))
	fmt.Printf("  Alice (no options): %d pipeline(s) – only pipelines she is a member of\n", len(alicePipelines))

	var bobPipelines []Pipeline
	must(db.SelectContext(bobCtx, &bobPipelines, `SELECT * FROM pipelines ORDER BY name`))
	fmt.Printf("  Bob (WithPipelineBypass): %d pipeline(s) – all tenant pipelines\n", len(bobPipelines))

	// ─────────────────────────────────────────────────────────────────────────
	sep(2, "CROSS-TENANT ISOLATION – Carol (tenant-b) sees zero of tenant-a rows")

	var carolPipelines []Pipeline
	must(db.SelectContext(carolCtx, &carolPipelines, `SELECT * FROM pipelines ORDER BY name`))

	setA := make(map[string]bool, len(bobPipelines))
	for _, r := range bobPipelines {
		setA[r.ID] = true
	}
	overlap := 0
	for _, r := range carolPipelines {
		if setA[r.ID] {
			overlap++
		}
	}
	fmt.Printf("  Tenant-A: %d  Tenant-B: %d  Overlap: %d (must be 0)\n",
		len(bobPipelines), len(carolPipelines), overlap)

	// ─────────────────────────────────────────────────────────────────────────
	sep(3, "CALLER ADDS OWN WHERE – scope AND-s its predicate on top")
	fmt.Println("  SQL:  SELECT * FROM pipelines WHERE is_active = true")

	var activePipelines []Pipeline
	must(db.SelectContext(aliceCtx, &activePipelines,
		`SELECT * FROM pipelines WHERE is_active = true ORDER BY name`))
	fmt.Printf("  Active pipelines for Alice: %d\n", len(activePipelines))

	// ─────────────────────────────────────────────────────────────────────────
	sep(4, "JOIN QUERY – each aliased table gets its own predicate")

	var views []EntryView
	must(db.SelectContext(aliceCtx, &views, `
SELECT
    e.id        AS entry_id,
    p.name      AS pipeline_name,
    s.name      AS stage_name,
    es.status   AS status
FROM entries e
JOIN pipelines    p  ON p.id        = e.pipeline_id
JOIN entry_stages es ON es.entry_id = e.id
JOIN stages       s  ON s.id        = es.stage_id
ORDER BY e.created_at DESC
LIMIT 20`))
	fmt.Printf("  Rows: %d\n", len(views))

	// ─────────────────────────────────────────────────────────────────────────
	sep(5, "CTE QUERY – CTE alias skipped; body tables scoped")

	var ctePipelines []Pipeline
	must(db.SelectContext(aliceCtx, &ctePipelines, `
WITH active_entries AS (
    SELECT pipeline_id FROM entries WHERE global_status LIKE $1
)
SELECT p.*
FROM pipelines p
JOIN active_entries ae ON ae.pipeline_id = p.id
ORDER BY p.name`, "active%"))
	fmt.Printf("  CTE result: %d pipeline(s)\n", len(ctePipelines))

	// ─────────────────────────────────────────────────────────────────────────
	sep(6, "SUBQUERY IN WHERE – both outer and inner table scoped")

	var subEntries []Entry
	must(db.SelectContext(aliceCtx, &subEntries, `
SELECT * FROM entries e
WHERE e.pipeline_id IN (
    SELECT id FROM pipelines p WHERE p.is_active = true
)
ORDER BY e.created_at DESC`))
	fmt.Printf("  Entries via subquery: %d\n", len(subEntries))

	// ─────────────────────────────────────────────────────────────────────────
	sep(7, "WORKSPACE SCOPE – WithWorkspaceID filters entries to one workspace")
	// Dave has WithWorkspaceID("workspace-1-uuid") set. The entries predicate
	// adds: AND (entries.workspace_id = $N::uuid)
	// Alice has no workspace set so she sees entries across all her workspaces.

	var daveEntries, aliceEntries []Entry
	must(db.SelectContext(daveCtx, &daveEntries, `SELECT * FROM entries ORDER BY created_at DESC`))
	must(db.SelectContext(aliceCtx, &aliceEntries, `SELECT * FROM entries ORDER BY created_at DESC`))
	fmt.Printf("  Dave (workspace-1 only): %d entry/entries\n", len(daveEntries))
	fmt.Printf("  Alice (all workspaces):  %d entry/entries\n", len(aliceEntries))

	// ─────────────────────────────────────────────────────────────────────────
	sep(8, "PAGE VISIBILITY – roles[] UUID check done in DB, no values in scope")
	// The predicate joins user_role_assignments live using UserID + TenantID.
	// No role UUIDs or names need to be pre-loaded into the RequestScope.

	var alicePages, bobPages []Page
	must(db.SelectContext(aliceCtx, &alicePages,
		`SELECT * FROM page_builder_pages ORDER BY name`))
	must(db.SelectContext(bobCtx, &bobPages,
		`SELECT * FROM page_builder_pages ORDER BY name`))
	fmt.Printf("  Alice sees %d page(s);  Bob sees %d page(s)\n",
		len(alicePages), len(bobPages))

	// ─────────────────────────────────────────────────────────────────────────
	sep(9, "AUDIT LOG – Alice sees own rows; Bob (WithAuditReadAll) sees all")

	var aliceAudit, bobAudit []AuditRow
	must(db.SelectContext(aliceCtx, &aliceAudit,
		`SELECT id, resource_type, action, actor_id FROM audit_logs ORDER BY created_at DESC LIMIT 10`))
	must(db.SelectContext(bobCtx, &bobAudit,
		`SELECT id, resource_type, action, actor_id FROM audit_logs ORDER BY created_at DESC LIMIT 10`))
	fmt.Printf("  Alice: %d row(s) (actor_id = Alice only)\n", len(aliceAudit))
	fmt.Printf("  Bob:   %d row(s) (entire tenant)\n", len(bobAudit))

	// ─────────────────────────────────────────────────────────────────────────
	sep(10, "UPDATE WRITE PROTECTION – tenant + membership predicate injected")
	fmt.Println("  SQL:  UPDATE pipelines SET updated_at = NOW() WHERE is_active = true")
	fmt.Println("  DB:   WHERE is_active = true AND (tenant_id=$1 AND ... membership ...)")

	res, err := db.ExecContext(aliceCtx,
		`UPDATE pipelines SET updated_at = NOW() WHERE is_active = true`)
	must(err)
	n, _ := res.RowsAffected()
	fmt.Printf("  Affected: %d row(s) – only Alice's tenant + her memberships\n", n)

	// ─────────────────────────────────────────────────────────────────────────
	sep(11, "DELETE WRITE PROTECTION")

	res2, err2 := db.ExecContext(aliceCtx,
		`DELETE FROM entries WHERE global_status = 'cancelled:expired'`)
	must(err2)
	n2, _ := res2.RowsAffected()
	fmt.Printf("  Deleted: %d row(s) – only Alice's tenant rows\n", n2)

	// ─────────────────────────────────────────────────────────────────────────
	sep(12, "MISSING CONTEXT – ScopeDenyMissingContext returned")
	// No scope in context at all → the hook rejects the query immediately.

	var denied []Pipeline
	err3 := db.SelectContext(context.Background(), &denied, `SELECT * FROM pipelines`)
	if code, ok := hooks.ScopeDenyCodeFromError(err3); ok {
		fmt.Printf("  Denied (expected): code=%s\n", code)
	} else {
		fmt.Printf("  Unexpected outcome: err=%v rows=%d\n", err3, len(denied))
	}

	// ─────────────────────────────────────────────────────────────────────────
	sep(13, "BYPASS TOKEN WITHOUT TRUSTED CTX – denied")

	var denied2 []Pipeline
	err4 := db.SelectContext(context.Background(), &denied2,
		`/* scope:bypass */ SELECT * FROM pipelines`)
	if code, ok := hooks.ScopeDenyCodeFromError(err4); ok {
		fmt.Printf("  Denied (expected): code=%s\n", code)
	}

	// ─────────────────────────────────────────────────────────────────────────
	sep(14, "TRUSTED BYPASS – internal job; unrestricted across all tenants")

	bypassCtx := hooks.WithTrustedScopeBypass(context.Background(), "nightly-reindex-job")
	var allPipelines []Pipeline
	must(db.SelectContext(bypassCtx, &allPipelines,
		`/* scope:bypass */ SELECT * FROM pipelines ORDER BY tenant_id, name`))
	fmt.Printf("  All pipelines (bypass): %d across all tenants\n", len(allPipelines))

	// ─────────────────────────────────────────────────────────────────────────
	sep(15, "FORMS + LOOKUP TABLES – tenant-scoped join, no options needed")

	type FormLookup struct {
		FormName   string `db:"form_name"`
		LookupSlug string `db:"lookup_slug"`
	}
	var fl []FormLookup
	must(db.SelectContext(aliceCtx, &fl, `
SELECT f.name AS form_name, lt.slug AS lookup_slug
FROM forms f
JOIN workspace_forms   wf ON wf.form_id      = f.id
JOIN workspaces        ws ON ws.id           = wf.workspace_id
JOIN workspace_lookups wl ON wl.workspace_id = ws.id
JOIN lookup_tables     lt ON lt.id           = wl.lookup_id
ORDER BY f.name, lt.slug`))
	fmt.Printf("  Form+lookup pairs: %d\n", len(fl))

	// ─────────────────────────────────────────────────────────────────────────
	sep(16, "ROLES + STAGE BINDINGS – tenant-scoped, no role names in query")

	type RoleStage struct {
		RoleName  string `db:"role_name"`
		StageName string `db:"stage_name"`
	}
	var roleStages []RoleStage
	must(db.SelectContext(aliceCtx, &roleStages, `
SELECT r.name AS role_name, s.name AS stage_name
FROM roles r
JOIN stage_role_bindings srb ON srb.role_id = r.id
JOIN stages              s   ON s.id        = srb.stage_id
ORDER BY r.name, s.name`))
	fmt.Printf("  Role+stage bindings: %d\n", len(roleStages))

	// ─────────────────────────────────────────────────────────────────────────
	sep(17, "NOTIFICATIONS – user-scoped automatically, no options needed")

	var notifs []Notification
	must(db.SelectContext(aliceCtx, &notifs,
		`SELECT id, type, subject FROM notifications ORDER BY created_at DESC LIMIT 5`))
	fmt.Printf("  Alice notifications: %d (user_id = Alice only)\n", len(notifs))

	fmt.Println("\ndone")
}

func must(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func bar() string { return "────────────────────────────────────────────────────" }
