// Package scope provides automatic row-level security for the ProcessGate
// data model. Callers write plain SQL; this hook rewrites every query in-flight
// to inject the correct tenant / user / workspace / permission predicates.
//
// # Design principles
//
//   - No role names, permission strings, or any business values are hardcoded.
//     All access decisions are expressed as database-side joins/subqueries
//     against the live roles, user_role_assignments, and pipeline_members tables.
//   - The only identity the hook needs is TenantID and UserID. Everything else
//     is optional: workspace restriction, pipeline bypass, and audit read-all are
//     opt-in flags that default to the most restrictive safe value.
//   - Callers never pass role names or permission strings into the scope. If they
//     want to elevate access (e.g. bypass pipeline membership checks), they set a
//     boolean flag after verifying the relevant permission in their own auth layer.
//
// # Minimal usage – just pass TenantID and UserID
//
//	ctx := scope.WithRequestScope(r.Context(),
//	    scope.NewRequestScope(tenantID, userID),
//	)
//
// # Optional elevation
//
//	ctx := scope.WithRequestScope(r.Context(), scope.NewRequestScope(
//	    tenantID, userID,
//	    scope.WithWorkspaceID(wsID),   // restrict to one workspace
//	    scope.WithPipelineBypass(),    // see all tenant pipelines
//	    scope.WithAuditReadAll(),      // see full audit trail
//	))
//
// # Bypass for internal jobs
//
//	ctx := hooks.WithTrustedScopeBypass(ctx, "reason")
//	db.QueryContext(ctx, "/* scope:bypass */ SELECT * FROM pipelines")
package scope

import (
	"context"
	"fmt"

	"github.com/oarkflow/squealx/hooks"
)

// ---------------------------------------------------------------------------
// RequestScope – the per-request identity. No role values stored here.
// ---------------------------------------------------------------------------

// RequestScope carries the identity needed to scope a query.
// Only TenantID and UserID are required. All other fields are optional and
// default to the most restrictive safe behaviour when not set.
type RequestScope struct {
	// TenantID is the UUID of the authenticated user's tenant. Required.
	TenantID string

	// UserID is the UUID of the authenticated user. Required.
	UserID string

	// WorkspaceID, when non-empty, further restricts workspace-owned resources
	// (entries, workspace_* tables) to a single workspace.
	// Optional: leave empty to allow all workspaces the user belongs to.
	WorkspaceID string

	// CanBypassPipelineMembership, when true, lets this user see all pipelines
	// in the tenant regardless of pipeline_members rows.
	// Optional: defaults to false (member-only visibility).
	// Set this only after verifying the user holds the relevant permission.
	CanBypassPipelineMembership bool

	// CanReadAllAuditLogs, when true, removes the actor_id filter on audit_logs
	// so the user sees the full tenant audit trail.
	// Optional: defaults to false (user sees only their own audit rows).
	// Set this only after verifying the user holds the relevant permission.
	CanReadAllAuditLogs bool
}

// RequestScopeOption mutates a RequestScope during construction.
type RequestScopeOption func(*RequestScope)

// WithWorkspaceID restricts workspace-owned resources to the given workspace.
// Optional: if not set, the user sees entries across all workspaces they belong to.
func WithWorkspaceID(id string) RequestScopeOption {
	return func(s *RequestScope) { s.WorkspaceID = id }
}

// WithPipelineBypass grants the user sight of all tenant pipelines regardless
// of pipeline_members rows. Optional: call this only after confirming the user
// holds a broad pipeline permission. Defaults to false (member-only visibility).
func WithPipelineBypass() RequestScopeOption {
	return func(s *RequestScope) { s.CanBypassPipelineMembership = true }
}

// WithAuditReadAll removes the per-actor filter on audit_logs so the user sees
// the full tenant audit trail. Optional: defaults to false (own rows only).
func WithAuditReadAll() RequestScopeOption {
	return func(s *RequestScope) { s.CanReadAllAuditLogs = true }
}

// NewRequestScope constructs a RequestScope.
// Only TenantID and UserID are required. All options are optional:
//
//	// Minimal – tenant isolation only, member-only pipeline visibility
//	scope.NewRequestScope(tenantID, userID)
//
//	// With workspace restriction
//	scope.NewRequestScope(tenantID, userID, scope.WithWorkspaceID(wsID))
//
//	// With elevated flags (set only after verifying the relevant permissions)
//	scope.NewRequestScope(tenantID, userID,
//	    scope.WithPipelineBypass(),
//	    scope.WithAuditReadAll(),
//	)
func NewRequestScope(tenantID, userID string, opts ...RequestScopeOption) *RequestScope {
	s := &RequestScope{TenantID: tenantID, UserID: userID}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

type scopeContextKey struct{}

// ScopeContextKey is the context key under which *RequestScope is stored.
var ScopeContextKey = scopeContextKey{}

// WithRequestScope stores rs in ctx for the duration of the request.
func WithRequestScope(ctx context.Context, rs *RequestScope) context.Context {
	return context.WithValue(ctx, ScopeContextKey, rs)
}

// RequestScopeFromContext retrieves the *RequestScope from ctx.
func RequestScopeFromContext(ctx context.Context) (*RequestScope, bool) {
	rs, ok := ctx.Value(ScopeContextKey).(*RequestScope)
	if !ok || rs == nil {
		return nil, false
	}
	return rs, true
}

// ---------------------------------------------------------------------------
// Internal resolver building blocks
// ---------------------------------------------------------------------------

func missingContext() error {
	return &hooks.ScopeError{
		Code:    hooks.ScopeDenyMissingContext,
		Message: "request scope missing in context – wrap your context with scope.WithRequestScope",
	}
}

// resolve extracts the *RequestScope and calls fn with it.
func resolve(ctx context.Context, fn func(*RequestScope) ([]any, error)) ([]any, error) {
	rs, ok := RequestScopeFromContext(ctx)
	if !ok {
		return nil, missingContext()
	}
	return fn(rs)
}

// tenantOnly returns [tenantID].
func tenantOnly(ctx context.Context) ([]any, error) {
	return resolve(ctx, func(rs *RequestScope) ([]any, error) {
		return []any{rs.TenantID}, nil
	})
}

// tenantUser returns [tenantID, userID].
func tenantUser(ctx context.Context) ([]any, error) {
	return resolve(ctx, func(rs *RequestScope) ([]any, error) {
		return []any{rs.TenantID, rs.UserID}, nil
	})
}

// userOnly returns [userID].
func userOnly(ctx context.Context) ([]any, error) {
	return resolve(ctx, func(rs *RequestScope) ([]any, error) {
		return []any{rs.UserID}, nil
	})
}

// ---------------------------------------------------------------------------
// Rule helpers
// ---------------------------------------------------------------------------

// tenantRule scopes a table with a single tenant_id = $1 predicate.
func tenantRule(table string) hooks.ScopeRule {
	return hooks.ScopeRule{
		Table:       table,
		Predicate:   "{{alias}}.tenant_id = {{param}}",
		ResolveArgs: tenantOnly,
	}
}

// tenantSoftDeleteRule adds deleted_at IS NULL on top of tenant isolation.
func tenantSoftDeleteRule(table string) hooks.ScopeRule {
	return hooks.ScopeRule{
		Table:       table,
		Predicate:   "({{alias}}.tenant_id = {{param}} AND {{alias}}.deleted_at IS NULL)",
		ResolveArgs: tenantOnly,
	}
}

// tenantUserRule scopes to tenant + a named user-id column.
func tenantUserRule(table, userCol string) hooks.ScopeRule {
	return hooks.ScopeRule{
		Table:     table,
		Predicate: fmt.Sprintf("({{alias}}.tenant_id = {{param}} AND {{alias}}.%s = {{param}})", userCol),
		ResolveArgs: tenantUser,
	}
}

// userColRule scopes using only a named user-id column (no tenant_id column on table).
func userColRule(table, userCol string) hooks.ScopeRule {
	return hooks.ScopeRule{
		Table:     table,
		Predicate: fmt.Sprintf("{{alias}}.%s = {{param}}", userCol),
		ResolveArgs: userOnly,
	}
}

// ---------------------------------------------------------------------------
// NewProcessGateScopeHook
// ---------------------------------------------------------------------------

// NewProcessGateScopeHook builds the fully wired hook.  Call this once and
// register it on the *squealx.DB:
//
//	db.Use(scope.NewProcessGateScopeHook())
func NewProcessGateScopeHook() *hooks.ResourceScopeHook {
	return hooks.NewResourceScopeHook(
		tenantOnly, // default resolver for any unmatched table
		allRules()...,
	).
		SetStrictMode(true).           // tables with a rule must satisfy it
		SetStrictAllTables(false).     // unknown tables (migrations, etc.) pass through
		SetRejectUnknownShapes(false). // complex shapes degrade to passthrough
		SetCompatibilityMode(true).    // graceful handling of edge-case SQL shapes
		SetAllowTrustedBypass(true).
		SetRequireBypassToken(true).
		SetBypassToken("/* scope:bypass */").
		SetPassthroughBudget(0.05, 50). // warn if >5 % of queries slip through unscoped
		SetAuditSink(defaultAuditSink).
		SetBudgetSink(defaultBudgetSink)
}

// allRules returns every ScopeRule for the ProcessGate schema.
// Each rule is a self-contained predicate; no role names or enum values appear.
func allRules() []hooks.ScopeRule {
	return []hooks.ScopeRule{

		// ── tenants ────────────────────────────────────────────────────────────
		// A user may only read their own tenant row.
		{
			Table:     "tenants",
			Predicate: "{{alias}}.id = {{param}}",
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					return []any{rs.TenantID}, nil
				})
			},
		},

		// ── users ──────────────────────────────────────────────────────────────
		// All non-deleted users within the same tenant are visible to each other.
		// (Callers who need a narrower view can add their own WHERE predicates.)
		tenantSoftDeleteRule("users"),

		// ── roles ──────────────────────────────────────────────────────────────
		// Roles are tenant-owned; soft-deleted roles are excluded.
		tenantSoftDeleteRule("roles"),

		// ── user_role_assignments ──────────────────────────────────────────────
		tenantRule("user_role_assignments"),

		// ── pipelines ─────────────────────────────────────────────────────────
		// Two-tier check:
		//   1. The pipeline belongs to this tenant and is not deleted.
		//   2. EITHER the user holds a pipeline_members row with status='active'
		//      OR the user's CanBypassPipelineMembership flag is set (which means
		//      they were verified at login to hold a broad pipeline permission).
		{
			Table: "pipelines",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND {{alias}}.deleted_at IS NULL
				AND (
					{{param}} = 'true'
					OR EXISTS (
						SELECT 1 FROM pipeline_members pm
						WHERE pm.pipeline_id = {{alias}}.id
						  AND pm.user_id     = {{param}}
						  AND pm.status      = 'active'
						  AND pm.deleted_at IS NULL
					)
					OR NOT EXISTS (
						SELECT 1 FROM pipeline_members pm2
						WHERE pm2.pipeline_id = {{alias}}.id
						  AND pm2.status      = 'active'
					)
				)
			)`,
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					bypass := "false"
					if rs.CanBypassPipelineMembership {
						bypass = "true"
					}
					return []any{rs.TenantID, bypass, rs.UserID}, nil
				})
			},
		},

		// ── pipeline_versions ─────────────────────────────────────────────────
		{
			Table: "pipeline_versions",
			Predicate: `{{alias}}.pipeline_id IN (
				SELECT id FROM pipelines p
				WHERE p.tenant_id = {{param}} AND p.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── pipeline_members ──────────────────────────────────────────────────
		{
			Table: "pipeline_members",
			Predicate: `(
				{{alias}}.pipeline_id IN (
					SELECT id FROM pipelines p
					WHERE p.tenant_id = {{param}} AND p.deleted_at IS NULL
				)
				AND {{alias}}.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── stages ────────────────────────────────────────────────────────────
		{
			Table: "stages",
			Predicate: `(
				{{alias}}.pipeline_id IN (
					SELECT id FROM pipelines p
					WHERE p.tenant_id = {{param}} AND p.deleted_at IS NULL
				)
				AND {{alias}}.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── stage_role_bindings ───────────────────────────────────────────────
		tenantRule("stage_role_bindings"),

		// ── stage_transitions ─────────────────────────────────────────────────
		{
			Table: "stage_transitions",
			Predicate: `{{alias}}.pipeline_id IN (
				SELECT id FROM pipelines p
				WHERE p.tenant_id = {{param}} AND p.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── entries ───────────────────────────────────────────────────────────
		// Entries are scoped to pipelines the user can access.
		// When WorkspaceID is set on the RequestScope the predicate also filters
		// to that workspace; otherwise all workspaces are included.
		{
			Table: "entries",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND {{alias}}.deleted_at IS NULL
				AND {{alias}}.pipeline_id IN (
					SELECT id FROM pipelines p
					WHERE p.tenant_id   = {{param}}
					  AND p.deleted_at IS NULL
					  AND (
						{{param}} = 'true'
						OR EXISTS (
							SELECT 1 FROM pipeline_members pm
							WHERE pm.pipeline_id = p.id
							  AND pm.user_id     = {{param}}
							  AND pm.status      = 'active'
							  AND pm.deleted_at IS NULL
						)
						OR NOT EXISTS (
							SELECT 1 FROM pipeline_members pm2
							WHERE pm2.pipeline_id = p.id AND pm2.status = 'active'
						)
					  )
				)
				AND ({{param}} = '' OR {{alias}}.workspace_id = {{param}}::uuid)
			)`,
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					bypass := "false"
					if rs.CanBypassPipelineMembership {
						bypass = "true"
					}
					return []any{rs.TenantID, rs.TenantID, bypass, rs.UserID, rs.WorkspaceID}, nil
				})
			},
		},

		// ── entry_data ────────────────────────────────────────────────────────
		{
			Table: "entry_data",
			Predicate: `{{alias}}.entry_id IN (
				SELECT id FROM entries e
				WHERE e.tenant_id = {{param}} AND e.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── entry_stages ──────────────────────────────────────────────────────
		{
			Table: "entry_stages",
			Predicate: `{{alias}}.entry_id IN (
				SELECT id FROM entries e
				WHERE e.tenant_id = {{param}} AND e.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── entry_files ───────────────────────────────────────────────────────
		tenantRule("entry_files"),

		// ── entry_comments ────────────────────────────────────────────────────
		// Public comments: visible to everyone in the tenant.
		// Non-public comments: visible only to the author and users who are
		// members of the same tenant (no role check hardcoded – the DB join
		// is sufficient: if you are in the tenant you can see internal comments).
		{
			Table: "entry_comments",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND (
					{{alias}}.visibility = 'public'
					OR {{alias}}.author_id = {{param}}
					OR EXISTS (
						SELECT 1 FROM users u
						WHERE u.id        = {{param}}
						  AND u.tenant_id = {{param}}
						  AND u.deleted_at IS NULL
					)
				)
			)`,
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					return []any{rs.TenantID, rs.UserID, rs.UserID, rs.TenantID}, nil
				})
			},
		},

		// ── entry_events ──────────────────────────────────────────────────────
		tenantRule("entry_events"),

		// ── entry_fingerprints ────────────────────────────────────────────────
		tenantRule("entry_fingerprints"),

		// ── entry_snapshots ───────────────────────────────────────────────────
		{
			Table: "entry_snapshots",
			Predicate: `{{alias}}.entry_id IN (
				SELECT id FROM entries e
				WHERE e.tenant_id = {{param}} AND e.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── entry_signatures ──────────────────────────────────────────────────
		tenantRule("entry_signatures"),

		// ── forms ─────────────────────────────────────────────────────────────
		tenantSoftDeleteRule("forms"),

		// ── page_builder_pages ────────────────────────────────────────────────
		// The pages.roles column stores an array of role UUIDs (not names).
		// A page is visible when:
		//   - roles is empty (open to all tenant users), OR
		//   - the user holds at least one of those role UUIDs via
		//     user_role_assignments.
		// No role names are compared here; access is purely by UUID presence.
		{
			Table: "page_builder_pages",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND {{alias}}.deleted_at IS NULL
				AND (
					{{alias}}.roles = '{}'
					OR EXISTS (
						SELECT 1 FROM user_role_assignments ura
						WHERE ura.user_id   = {{param}}
						  AND ura.tenant_id = {{param}}
						  AND ura.role_id::text = ANY({{alias}}.roles)
					)
				)
			)`,
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					return []any{rs.TenantID, rs.UserID, rs.TenantID}, nil
				})
			},
		},

		// ── page_builder_groups ───────────────────────────────────────────────
		tenantRule("page_builder_groups"),

		// ── page_builder_blocks ───────────────────────────────────────────────
		tenantRule("page_builder_blocks"),

		// ── page_entries ──────────────────────────────────────────────────────
		tenantRule("page_entries"),

		// ── page_entry_files ──────────────────────────────────────────────────
		tenantRule("page_entry_files"),

		// ── workspaces ────────────────────────────────────────────────────────
		// A user sees workspaces they have been explicitly added to.
		// Broader visibility (e.g. "see all workspaces in tenant") must be
		// granted by adding the user to workspace_users, not by changing this rule.
		{
			Table: "workspaces",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND EXISTS (
					SELECT 1 FROM workspace_users wu
					WHERE wu.workspace_id = {{alias}}.id
					  AND wu.user_id      = {{param}}
				)
			)`,
			ResolveArgs: tenantUser,
		},

		// ── workspace_users ───────────────────────────────────────────────────
		{
			Table: "workspace_users",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_roles ───────────────────────────────────────────────────
		{
			Table: "workspace_roles",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_pipelines ───────────────────────────────────────────────
		{
			Table: "workspace_pipelines",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_forms ───────────────────────────────────────────────────
		{
			Table: "workspace_forms",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_pages ───────────────────────────────────────────────────
		{
			Table: "workspace_pages",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_lookups ─────────────────────────────────────────────────
		{
			Table: "workspace_lookups",
			Predicate: `{{alias}}.workspace_id IN (
				SELECT id FROM workspaces w WHERE w.tenant_id = {{param}}
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── workspace_files / workspace_documents ─────────────────────────────
		tenantRule("workspace_files"),
		tenantRule("workspace_documents"),

		// ── registry_module_records ───────────────────────────────────────────
		tenantRule("registry_module_records"),

		// ── module_files ──────────────────────────────────────────────────────
		tenantRule("module_files"),

		// ── lookup_tables ─────────────────────────────────────────────────────
		tenantSoftDeleteRule("lookup_tables"),

		// ── lookup_entries ────────────────────────────────────────────────────
		{
			Table: "lookup_entries",
			Predicate: `{{alias}}.lookup_id IN (
				SELECT id FROM lookup_tables lt
				WHERE lt.tenant_id   = {{param}}
				  AND lt.deleted_at IS NULL
			)`,
			ResolveArgs: tenantOnly,
		},

		// ── audit_logs ────────────────────────────────────────────────────────
		// When CanReadAllAuditLogs is true (set at session-build after verifying
		// an appropriate permission), the actor filter is dropped.
		// When false, users see only rows where they are the actor.
		{
			Table: "audit_logs",
			Predicate: `(
				{{alias}}.tenant_id = {{param}}
				AND ({{param}} = 'true' OR {{alias}}.actor_id = {{param}})
			)`,
			ResolveArgs: func(ctx context.Context) ([]any, error) {
				return resolve(ctx, func(rs *RequestScope) ([]any, error) {
					readAll := "false"
					if rs.CanReadAllAuditLogs {
						readAll = "true"
					}
					return []any{rs.TenantID, readAll, rs.UserID}, nil
				})
			},
		},

		// ── notifications ─────────────────────────────────────────────────────
		tenantUserRule("notifications", "user_id"),

		// ── webhooks ──────────────────────────────────────────────────────────
		tenantRule("webhooks"),

		// ── service_connectors ────────────────────────────────────────────────
		tenantRule("service_connectors"),

		// ── ai_agents ─────────────────────────────────────────────────────────
		tenantRule("ai_agents"),

		// ── ai_agent_datasets ─────────────────────────────────────────────────
		tenantRule("ai_agent_datasets"),

		// ── integration_channels / tokens / executions ────────────────────────
		tenantRule("integration_channels"),
		tenantRule("integration_tokens"),
		tenantRule("integration_executions"),

		// ── rule_groups / assignments ─────────────────────────────────────────
		tenantRule("rule_groups"),
		tenantRule("rule_group_assignments"),

		// ── pipeline_deployments ──────────────────────────────────────────────
		tenantRule("pipeline_deployments"),

		// ── entry_id_sequences ────────────────────────────────────────────────
		tenantRule("entry_id_sequences"),

		// ── snippet_assignments ───────────────────────────────────────────────
		tenantRule("snippet_assignments"),

		// ── user_certificates ─────────────────────────────────────────────────
		tenantUserRule("user_certificates", "user_id"),

		// ── per-user private tables (no tenant_id column) ─────────────────────
		userColRule("user_mfa_configs", "user_id"),
		userColRule("user_profile_settings", "user_id"),
		userColRule("user_security_policies", "user_id"),
		userColRule("user_token_revocations", "user_id"),

		// ── tenant_maintenance_modes ──────────────────────────────────────────
		tenantRule("tenant_maintenance_modes"),

		// ── data_sync_jobs / logs ─────────────────────────────────────────────
		tenantRule("data_sync_jobs"),

		// ── connector_datasets ────────────────────────────────────────────────
		tenantRule("connector_datasets"),

		// ── policies ──────────────────────────────────────────────────────────
		tenantRule("policies"),

		// ── user_consents ─────────────────────────────────────────────────────
		tenantUserRule("user_consents", "user_id"),
	}
}

// ---------------------------------------------------------------------------
// Sinks – replace with your own telemetry / structured logger
// ---------------------------------------------------------------------------

func defaultAuditSink(_ context.Context, d hooks.ScopeDecision) {
	if d.Action == hooks.ScopeDecisionPassthrough {
		return
	}
	fmt.Printf("[scope-audit] action=%-12s code=%-30s tables=%v rules=%v\n",
		d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules)
}

func defaultBudgetSink(_ context.Context, s hooks.ScopeBudgetSnapshot) {
	if !s.Exceeded {
		return
	}
	fmt.Printf("[scope-budget] EXCEEDED: passthrough_ratio=%.4f threshold=%.4f total=%d passthroughs=%d\n",
		s.Ratio, s.Threshold, s.TotalDecisions, s.Passthroughs)
}
