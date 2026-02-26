package hooks

import (
	"context"
	"strings"
	"testing"
)

func staticResolver(_ context.Context) ([]any, error) {
	return []any{42}, nil
}

func TestResourceScopeHook_CTEShadowingScopesOnlyBaseTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "orders", Column: "tenant_id"},
		ScopeRule{Table: "customers", Column: "tenant_id"},
	)

	query := "WITH orders AS (SELECT * FROM base_orders) SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id"
	_, rewritten, args, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(rewritten, "c.tenant_id") {
		t.Fatalf("expected customers scope predicate, got: %s", rewritten)
	}
	if strings.Contains(rewritten, "o.tenant_id") {
		t.Fatalf("expected CTE alias to be excluded from rule rewrite, got: %s", rewritten)
	}
	if len(args) != 1 {
		t.Fatalf("expected one injected arg, got %d", len(args))
	}
}

func TestResourceScopeHook_UnknownShapeRejectedWithoutCompatibilityMode(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "SELECT * FROM (VALUES (1)) v"
	_, _, _, err := hook.Before(context.Background(), query)
	if err == nil {
		t.Fatal("expected unknown shape error")
	}
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		t.Fatalf("expected scope error, got: %v", err)
	}
	if code != ScopeDenyUnknownShape {
		t.Fatalf("expected deny code %q, got %q", ScopeDenyUnknownShape, code)
	}
}

func TestResourceScopeHook_CompatibilityModePassthroughOnLowConfidence(t *testing.T) {
	var decisions []ScopeDecision
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetCompatibilityMode(true).
		SetAuditSink(func(_ context.Context, d ScopeDecision) {
			decisions = append(decisions, d)
		})

	query := "SELECT * FROM (VALUES (1)) v"
	_, rewritten, _, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("expected passthrough in compatibility mode, got error: %v", err)
	}
	if rewritten != query {
		t.Fatalf("expected query passthrough, got: %s", rewritten)
	}
	if len(decisions) == 0 {
		t.Fatal("expected audit decisions")
	}
	last := decisions[len(decisions)-1]
	if last.Action != ScopeDecisionPassthrough {
		t.Fatalf("expected passthrough action, got %q", last.Action)
	}
	if last.Confidence != ScopeConfidenceLow {
		t.Fatalf("expected low confidence, got %q", last.Confidence)
	}
	if !containsString(last.Coverage, "derived_table") || !containsString(last.Coverage, "select") {
		t.Fatalf("expected coverage to include derived_table/select, got: %#v", last.Coverage)
	}
}

func TestResourceScopeHook_AuditLineageMarksCTEAndBaseOrigins(t *testing.T) {
	var decisions []ScopeDecision
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "orders", Column: "tenant_id"},
		ScopeRule{Table: "customers", Column: "tenant_id"},
	).SetAuditSink(func(_ context.Context, d ScopeDecision) {
		decisions = append(decisions, d)
	})

	query := "WITH orders AS (SELECT * FROM base_orders) SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id"
	_, _, _, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var scoped *ScopeDecision
	for i := range decisions {
		d := decisions[i]
		if d.Action == ScopeDecisionScoped && d.StatementType == "SELECT" {
			scoped = &decisions[i]
		}
	}
	if scoped == nil {
		t.Fatal("expected scoped SELECT decision in audit")
	}
	if !hasLineageOrigin(scoped.Lineage, "orders", ScopeTableOriginCTE) {
		t.Fatalf("expected orders lineage as cte, got: %#v", scoped.Lineage)
	}
	if !hasLineageOrigin(scoped.Lineage, "customers", ScopeTableOriginBase) {
		t.Fatalf("expected customers lineage as base table, got: %#v", scoped.Lineage)
	}
}

func TestScopeReasonTaxonomyFromError(t *testing.T) {
	err := scopeErr(ScopeDenyMissingRule, "missing")
	tax, ok := ScopeReasonTaxonomyFromError(err)
	if !ok {
		t.Fatal("expected taxonomy for scope error")
	}
	if tax.Category != ScopeReasonCategoryRule {
		t.Fatalf("expected rule category, got %q", tax.Category)
	}
	if tax.Severity != ScopeReasonSeverityCritical {
		t.Fatalf("expected critical severity, got %q", tax.Severity)
	}
}

func TestResourceScopeHook_CompatibilityModeBudgetGate(t *testing.T) {
	var decisions []ScopeDecision
	var budgets []ScopeBudgetSnapshot
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).
		SetRejectUnknownShapes(true).
		SetCompatibilityMode(true).
		SetPassthroughBudget(0.40, 2).
		SetAuditSink(func(_ context.Context, d ScopeDecision) {
			decisions = append(decisions, d)
		}).
		SetBudgetSink(func(_ context.Context, s ScopeBudgetSnapshot) {
			budgets = append(budgets, s)
		})

	query := "SELECT * FROM (VALUES (1)) v"

	// First low-confidence passthrough is allowed.
	_, rewritten, _, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("expected first passthrough allowed, got error: %v", err)
	}
	if rewritten != query {
		t.Fatalf("expected passthrough query unchanged, got: %s", rewritten)
	}

	// Second passthrough breaches budget ratio and is rejected.
	_, _, _, err = hook.Before(context.Background(), query)
	if err == nil {
		t.Fatal("expected passthrough budget rejection")
	}
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		t.Fatalf("expected scope error, got: %v", err)
	}
	if code != ScopeDenyPassthroughBudget {
		t.Fatalf("expected deny code %q, got %q", ScopeDenyPassthroughBudget, code)
	}
	last := decisions[len(decisions)-1]
	if last.Action != ScopeDecisionRejected {
		t.Fatalf("expected rejected action, got %q", last.Action)
	}
	if last.ReasonCode != ScopeDenyPassthroughBudget {
		t.Fatalf("expected passthrough budget reason code, got %q", last.ReasonCode)
	}
	if last.ReasonCategory != ScopeReasonCategoryBudget || last.ReasonSeverity != ScopeReasonSeverityCritical {
		t.Fatalf("expected budget taxonomy on rejection, got category=%q severity=%q", last.ReasonCategory, last.ReasonSeverity)
	}
	snapshot := hook.BudgetSnapshot()
	if !snapshot.Exceeded {
		t.Fatalf("expected budget snapshot exceeded, got: %#v", snapshot)
	}
	if len(budgets) == 0 {
		t.Fatal("expected budget sink snapshots")
	}
}

func TestResourceScopeHook_PlaceholderStylesAcrossDBMS(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	tests := []struct {
		name            string
		query           string
		args            []any
		wantPlaceholder string
	}{
		{
			name:            "mysql question style",
			query:           "SELECT * FROM pipelines WHERE name = ?",
			args:            []any{"build"},
			wantPlaceholder: "pipelines.tenant_id = ?",
		},
		{
			name:            "postgres dollar style",
			query:           "SELECT * FROM pipelines WHERE name = $1",
			args:            []any{"build"},
			wantPlaceholder: "pipelines.tenant_id = $2",
		},
		{
			name:            "sqlserver at-param style",
			query:           "SELECT * FROM pipelines WHERE name = @p1",
			args:            []any{"build"},
			wantPlaceholder: "pipelines.tenant_id = @p2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, rewritten, gotArgs, err := hook.Before(context.Background(), tc.query, tc.args...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(rewritten, tc.wantPlaceholder) {
				t.Fatalf("expected rewritten query to contain %q, got: %s", tc.wantPlaceholder, rewritten)
			}
			if len(gotArgs) != len(tc.args)+1 {
				t.Fatalf("expected args len %d, got %d", len(tc.args)+1, len(gotArgs))
			}
			if gotArgs[len(gotArgs)-1] != 42 {
				t.Fatalf("expected appended scope arg 42, got: %#v", gotArgs)
			}
		})
	}
}

func TestResourceScopeHook_NestedSubqueryScopedOnceWithoutDuplicatePredicates(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
		ScopeRule{Table: "entries", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "SELECT p.pipeline_id FROM pipelines p WHERE EXISTS (SELECT 1 FROM entries e WHERE e.pipeline_id = p.pipeline_id)"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.user_id = ?"); count != 1 {
		t.Fatalf("expected one outer predicate injection, got %d in query: %s", count, rewritten)
	}
	if count := strings.Count(rewritten, "e.user_id = ?"); count != 1 {
		t.Fatalf("expected one nested predicate injection, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("expected two injected args, got %d", len(gotArgs))
	}
}

func TestResourceScopeHook_DoesNotRewriteScalarExpressionSubquery(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
	)

	query := "SELECT COALESCE((SELECT MAX(p.pipeline_id) FROM pipelines p), 0) AS max_id"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rewritten != query {
		t.Fatalf("expected scalar-expression subquery to pass through unchanged, got: %s", rewritten)
	}
	if len(gotArgs) != 0 {
		t.Fatalf("expected no added args, got %d", len(gotArgs))
	}
}

func TestResourceScopeHook_PredicateInsertionKeepsSpacingBeforeOrderBy(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "SELECT * FROM pipelines ORDER BY pipeline_id"
	_, rewritten, _, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(rewritten, ") ORDER BY") {
		t.Fatalf("expected spacing before ORDER BY, got: %s", rewritten)
	}
	if strings.Contains(rewritten, ")ORDER BY") {
		t.Fatalf("expected no malformed ')ORDER BY', got: %s", rewritten)
	}
}

func TestResourceScopeHook_StrictModeAllowsCTEOnlyOuterSelectWhenNestedScoped(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "WITH scoped AS (SELECT * FROM pipelines p) SELECT * FROM scoped"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("expected strict mode query to pass due nested scoping, got: %v", err)
	}
	if !strings.Contains(rewritten, "p.user_id = ?") {
		t.Fatalf("expected nested CTE body to be scoped, got: %s", rewritten)
	}
	if strings.Contains(rewritten, "scoped.user_id = ?") {
		t.Fatalf("expected outer CTE reference not to be directly scoped, got: %s", rewritten)
	}
	if len(gotArgs) != 1 {
		t.Fatalf("expected one injected arg, got %d", len(gotArgs))
	}
}

func TestResourceScopeHook_RewritesUnionAllBranches(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "SELECT p.pipeline_id FROM pipelines p WHERE p.name = ? UNION ALL SELECT q.pipeline_id FROM pipelines q WHERE q.name = ? ORDER BY pipeline_id"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query, "alpha", "beta")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.user_id = ?"); count != 1 {
		t.Fatalf("expected one scoped predicate on first branch, got %d in query: %s", count, rewritten)
	}
	if count := strings.Count(rewritten, "q.user_id = ?"); count != 1 {
		t.Fatalf("expected one scoped predicate on second branch, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 4 {
		t.Fatalf("expected 4 args, got %d", len(gotArgs))
	}
	if gotArgs[0] != "alpha" || gotArgs[1] != 42 || gotArgs[2] != "beta" || gotArgs[3] != 42 {
		t.Fatalf("unexpected arg order for union rewrite: %#v", gotArgs)
	}
	if !strings.Contains(rewritten, ") ORDER BY") {
		t.Fatalf("expected spacing before ORDER BY on union tail, got: %s", rewritten)
	}
}

func TestResourceScopeHook_RewritesSetOperatorBranches(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "user_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	cases := []struct {
		name      string
		query     string
		leftPred  string
		rightPred string
	}{
		{
			name:      "intersect",
			query:     "SELECT p.pipeline_id FROM pipelines p INTERSECT SELECT q.pipeline_id FROM pipelines q",
			leftPred:  "p.user_id = ?",
			rightPred: "q.user_id = ?",
		},
		{
			name:      "except",
			query:     "SELECT p.pipeline_id FROM pipelines p EXCEPT SELECT q.pipeline_id FROM pipelines q",
			leftPred:  "p.user_id = ?",
			rightPred: "q.user_id = ?",
		},
		{
			name:      "intersection alias",
			query:     "SELECT p.pipeline_id FROM pipelines p INTERSECTION SELECT q.pipeline_id FROM pipelines q",
			leftPred:  "p.user_id = ?",
			rightPred: "q.user_id = ?",
		},
		{
			name:      "minus alias",
			query:     "SELECT p.pipeline_id FROM pipelines p MINUS SELECT q.pipeline_id FROM pipelines q",
			leftPred:  "p.user_id = ?",
			rightPred: "q.user_id = ?",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rewritten, gotArgs, err := hook.Before(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count := strings.Count(rewritten, tc.leftPred); count != 1 {
				t.Fatalf("expected one left branch predicate, got %d in query: %s", count, rewritten)
			}
			if count := strings.Count(rewritten, tc.rightPred); count != 1 {
				t.Fatalf("expected one right branch predicate, got %d in query: %s", count, rewritten)
			}
			if len(gotArgs) != 2 {
				t.Fatalf("expected two injected args, got %d", len(gotArgs))
			}
		})
	}
}

func TestResourceScopeHook_UpdateFromScopesTargetAndFromTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
		ScopeRule{Table: "users", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "UPDATE pipelines p SET name = ? FROM users u WHERE u.id = p.user_id"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query, "next-name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one target-table predicate, got %d in query: %s", count, rewritten)
	}
	if count := strings.Count(rewritten, "u.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one FROM-table predicate, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 3 {
		t.Fatalf("expected three args, got %d", len(gotArgs))
	}
	if gotArgs[0] != "next-name" || gotArgs[1] != 42 || gotArgs[2] != 42 {
		t.Fatalf("unexpected arg order for UPDATE ... FROM rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_DeleteUsingScopesAllTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
		ScopeRule{Table: "users", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "DELETE FROM pipelines p USING users u WHERE u.id = p.user_id"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one delete-target predicate, got %d in query: %s", count, rewritten)
	}
	if count := strings.Count(rewritten, "u.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one USING-table predicate, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("expected two injected args, got %d", len(gotArgs))
	}
}

func TestResourceScopeHook_DeleteJoinScopesAllTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
		ScopeRule{Table: "users", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "DELETE p FROM pipelines p JOIN users u ON u.id = p.user_id WHERE u.active = ?"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one delete-join target predicate, got %d in query: %s", count, rewritten)
	}
	if count := strings.Count(rewritten, "u.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one joined-table predicate, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 3 {
		t.Fatalf("expected three args, got %d", len(gotArgs))
	}
	if gotArgs[0] != true || gotArgs[1] != 42 || gotArgs[2] != 42 {
		t.Fatalf("unexpected arg order for DELETE ... JOIN rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_StrictAllTablesRejectsMissingRuleInUpdateFromAndDeleteJoin(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true).SetStrictAllTables(true)

	cases := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "update from missing joined-table rule",
			query: "UPDATE pipelines p SET name = ? FROM users u WHERE u.id = p.user_id",
			args:  []any{"renamed"},
		},
		{
			name:  "delete join missing joined-table rule",
			query: "DELETE p FROM pipelines p JOIN users u ON u.id = p.user_id WHERE p.pipeline_id = ?",
			args:  []any{7},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := hook.Before(context.Background(), tc.query, tc.args...)
			if err == nil {
				t.Fatal("expected missing rule rejection")
			}
			code, ok := ScopeDenyCodeFromError(err)
			if !ok {
				t.Fatalf("expected scope error, got: %v", err)
			}
			if code != ScopeDenyMissingRule {
				t.Fatalf("expected deny code %q, got %q", ScopeDenyMissingRule, code)
			}
		})
	}
}

func TestResourceScopeHook_InsertSelectScopesSourceTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "INSERT INTO audits (pipeline_id) SELECT p.pipeline_id FROM pipelines p WHERE p.name = ?"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query, "build-main")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count := strings.Count(rewritten, "p.tenant_id = ?"); count != 1 {
		t.Fatalf("expected one source-table predicate, got %d in query: %s", count, rewritten)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("expected two args, got %d", len(gotArgs))
	}
	if gotArgs[0] != "build-main" || gotArgs[1] != 42 {
		t.Fatalf("unexpected arg order for INSERT ... SELECT rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_InsertSelectKeepsOnConflictTail(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "INSERT INTO audits (pipeline_id) SELECT p.pipeline_id FROM pipelines p ON CONFLICT (pipeline_id) DO NOTHING"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(rewritten, "WHERE (p.tenant_id = ?) ON CONFLICT") {
		t.Fatalf("expected predicate to be inserted before ON CONFLICT, got: %s", rewritten)
	}
	if len(gotArgs) != 1 || gotArgs[0] != 42 {
		t.Fatalf("unexpected args for INSERT ... ON CONFLICT rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_InsertValuesRejectedWhenRejectUnknownEnabled(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "INSERT INTO pipelines (name) VALUES (?)"
	_, _, _, err := hook.Before(context.Background(), query, "build-main")
	if err == nil {
		t.Fatal("expected INSERT ... VALUES to be rejected when reject unknown shapes is enabled")
	}
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		t.Fatalf("expected scope error, got: %v", err)
	}
	if code != ScopeDenyUnknownShape {
		t.Fatalf("expected deny code %q, got %q", ScopeDenyUnknownShape, code)
	}
}

func TestResourceScopeHook_MergeScopesTargetAndUsingTables(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
		ScopeRule{Table: "users", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "MERGE INTO pipelines p USING users u ON p.user_id = u.id WHEN MATCHED THEN UPDATE SET p.name = ?"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query, "renamed")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(rewritten, "ON p.user_id = u.id") || !strings.Contains(rewritten, "AND (p.tenant_id = ? AND u.tenant_id = ?)") || !strings.Contains(rewritten, "WHEN MATCHED") {
		t.Fatalf("expected MERGE ON clause to include scoped predicates, got: %s", rewritten)
	}
	if len(gotArgs) != 3 {
		t.Fatalf("expected three args, got %d", len(gotArgs))
	}
	if gotArgs[0] != 42 || gotArgs[1] != 42 || gotArgs[2] != "renamed" {
		t.Fatalf("unexpected arg order for MERGE rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_MergeUsingSubqueryScopesNestedSelect(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "audits", Column: "tenant_id"},
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "MERGE INTO audits a USING (SELECT p.pipeline_id FROM pipelines p) s ON a.pipeline_id = s.pipeline_id WHEN NOT MATCHED THEN INSERT (pipeline_id) VALUES (s.pipeline_id)"
	_, rewritten, gotArgs, err := hook.Before(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(rewritten, "FROM pipelines p WHERE (p.tenant_id = ?)") {
		t.Fatalf("expected nested USING subquery to be scoped, got: %s", rewritten)
	}
	if !strings.Contains(rewritten, "ON a.pipeline_id = s.pipeline_id") || !strings.Contains(rewritten, "AND (a.tenant_id = ?)") || !strings.Contains(rewritten, "WHEN NOT MATCHED") {
		t.Fatalf("expected MERGE target to be scoped in ON clause, got: %s", rewritten)
	}
	if len(gotArgs) != 2 || gotArgs[0] != 42 || gotArgs[1] != 42 {
		t.Fatalf("unexpected args for MERGE USING subquery rewrite: %#v", gotArgs)
	}
}

func TestResourceScopeHook_StrictAllTablesRejectsMissingRuleInMerge(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true).SetStrictAllTables(true)

	query := "MERGE INTO pipelines p USING users u ON p.user_id = u.id WHEN MATCHED THEN UPDATE SET p.name = ?"
	_, _, _, err := hook.Before(context.Background(), query, "renamed")
	if err == nil {
		t.Fatal("expected missing-rule rejection for MERGE source table")
	}
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		t.Fatalf("expected scope error, got: %v", err)
	}
	if code != ScopeDenyMissingRule {
		t.Fatalf("expected deny code %q, got %q", ScopeDenyMissingRule, code)
	}
}

func TestResourceScopeHook_MergeWithoutOnRejectedWhenRejectUnknownEnabled(t *testing.T) {
	hook := NewResourceScopeHook(
		staticResolver,
		ScopeRule{Table: "pipelines", Column: "tenant_id"},
	).SetStrictMode(true).SetRejectUnknownShapes(true)

	query := "MERGE INTO pipelines p USING users u WHEN MATCHED THEN UPDATE SET p.name = ?"
	_, _, _, err := hook.Before(context.Background(), query, "renamed")
	if err == nil {
		t.Fatal("expected MERGE without ON to be rejected")
	}
	code, ok := ScopeDenyCodeFromError(err)
	if !ok {
		t.Fatalf("expected scope error, got: %v", err)
	}
	if code != ScopeDenyUnknownShape {
		t.Fatalf("expected deny code %q, got %q", ScopeDenyUnknownShape, code)
	}
}

func hasLineageOrigin(items []ScopeLineage, table string, origin ScopeTableOrigin) bool {
	for _, item := range items {
		if canonicalTableName(item.Table) == table && item.Origin == origin {
			return true
		}
	}
	return false
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
