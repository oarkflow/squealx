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
