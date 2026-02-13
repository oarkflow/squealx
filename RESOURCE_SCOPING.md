# Resource Scoping (Row-Level Access Control)

This project supports application-level query scoping through hooks.

## Quick start

```go
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
SetAuditSink(func(ctx context.Context, d hooks.ScopeDecision) {
  // send to logger/telemetry
  // d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules
})

db.Use(scope)

ctx := context.WithValue(context.Background(), userIDKey, userID)
err := db.SelectContext(ctx, &rows, "SELECT * FROM pipelines")
```

## Production profile (copy-paste)

```go
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
SetStrictAllTables(false).
SetAllowTrustedBypass(true).
SetRequireBypassToken(true).
SetBypassToken("/* scope:bypass */").
SetAuditSink(func(ctx context.Context, d hooks.ScopeDecision) {
  // send to logger/telemetry
  // d.Action, d.ReasonCode, d.MatchedTables, d.AppliedRules
})

db.Use(scope)

ctx := context.WithValue(context.Background(), userIDKey, userID)
if err := db.SelectContext(ctx, &rows, "SELECT * FROM pipelines"); err != nil {
  if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
    // map `code` in API responses / metrics
  }
}
```

## Security model

- This is **application-level** scoping, not database-native authorization.
- For strong defense in depth, combine with DB-native controls:
  - PostgreSQL RLS policies
  - Restricted DB credentials per service role
  - Views/stored procedures for sensitive access
- `SetStrictMode(true)` is recommended so missing context or invalid rule resolution fails closed.
- `SetRejectUnknownShapes(true)` is recommended for high-security workloads to reject unrecognized query patterns instead of best-effort rewriting.
- `SetStrictAllTables(true)` can be added if every discovered top-level table must have an explicit rule.

## What is supported today

- `SELECT`, `UPDATE`, `DELETE`, and `WITH ...` (CTE main statement)
- Multi-statement SQL strings (split by top-level `;`)
- Top-level `FROM` and `JOIN` table detection (`SELECT`)
- `UPDATE` target table detection and `DELETE FROM` target table detection
- Existing `WHERE` augmentation (`AND (...)`) or insertion of new `WHERE (...)`
- Driver-aware placeholder injection (`?`, `$n`, `@pN`)
- Correct argument insertion ordering for `?` placeholders
- Hook propagation for `DB` and `Tx` query/exec methods

## Predicate template variables

`ScopeRule.Predicate` supports template placeholders that are expanded by the hook.

- `{{alias}}`
  - Meaning: **the SQL identifier for the current table reference**.
  - Replaced with the discovered SQL table alias (`p`, `e`, etc.).
  - If no alias exists in SQL, it falls back to the table name.
  - You do **not** provide this value manually.
  - Typical use: `{{alias}}.org_id = ...`

- `{{param}}`
  - Meaning: **a bind-parameter slot**, not a literal value.
  - Replaced with a driver-aware placeholder (`?`, `$n`, `@pN`).
  - Actual values come from `ResolveArgs` (rule) or the hook default resolver.
  - One `{{param}}` maps to one resolver value.

### What you put vs what hook fills

- You write SQL shape in `Predicate`:
  - Example: `"{{alias}}.org_id = {{param}}"`
- Hook fills:
  - `{{alias}}` → table alias/name from parsed SQL
  - `{{param}}` → placeholder token for current driver/query
- Resolver provides actual argument values in order.

Example:

```go
hooks.ScopeRule{
  Table: "pipelines",
  Predicate: "{{alias}}.org_id = {{param}} AND {{alias}}.owner_user_id = {{param}}",
  ResolveArgs: func(ctx context.Context) ([]any, error) {
    // value order matches {{param}} order above
    return []any{orgID, userID}, nil
  },
}
```

If query is MySQL-style, this becomes effectively:

```sql
p.org_id = ? AND p.owner_user_id = ?
```

If query is Postgres-style, this becomes effectively:

```sql
p.org_id = $1 AND p.owner_user_id = $2
```

### Complete replacement walkthrough (end-to-end)

This section shows exactly what gets replaced when `{{alias}}` and `{{param}}` are used.

Rule:

```go
hooks.ScopeRule{
  Table: "pipelines",
  Predicate: "({{alias}}.org_id = {{param}} AND {{alias}}.owner_user_id = {{param}})",
  ResolveArgs: func(ctx context.Context) ([]any, error) {
    return []any{42, 9001}, nil // orgID, userID
  },
}
```

Incoming query:

```sql
SELECT p.pipeline_id, p.name FROM pipelines p WHERE p.status = ?
```

Incoming args:

```go
[]any{"active"}
```

Detected table alias:

- For table `pipelines`, alias is `p`

Replacements performed:

- `{{alias}}` → `p`
- first `{{param}}` → first new bind placeholder
- second `{{param}}` → second new bind placeholder

Final rewritten query (MySQL/SQLite `?` style):

```sql
SELECT p.pipeline_id, p.name
FROM pipelines p
WHERE p.status = ?
  AND (p.org_id = ? AND p.owner_user_id = ?)
```

Final args (MySQL/SQLite):

```go
[]any{"active", 42, 9001}
```

Final rewritten query (Postgres `$n` style):

```sql
SELECT p.pipeline_id, p.name
FROM pipelines p
WHERE p.status = $1
  AND (p.org_id = $2 AND p.owner_user_id = $3)
```

Final args (Postgres):

```go
[]any{"active", 42, 9001}
```

Notes:

- `{{alias}}` always becomes the SQL alias/table name, never a runtime value.
- `{{param}}` always becomes a placeholder token; values are appended from resolver output.
- Resolver value order must match `{{param}}` order in the predicate.

### Rules and constraints

- Do not quote `{{param}}` manually.
  - Correct: `{{alias}}.org_id = {{param}}`
  - Incorrect: `{{alias}}.org_id = '{{param}}'`

- Do not hardcode alias names unless intentional.
  - Prefer `{{alias}}.column` over `p.column`.

- `{{alias}}` is optional.
  - If omitted, the predicate is inserted as-is.

- `{{param}}` is optional.
  - If omitted, no resolver args are consumed for that rule.
  - Use with care; literal-only predicates can be valid but less flexible.

- If `Predicate` is empty, `Column` mode is used:
  - Generated shape: `<alias>.<column> = {{param}}`
  - Requires resolver to provide exactly 1 value.

- If `Predicate` has N `{{param}}` placeholders:
  - Resolver must return N values.
  - Special case: if resolver returns exactly 1 value and N > 1, value is auto-repeated N times.

- Unknown placeholders are not interpreted.
  - Example: `{{tenant}}` is treated as plain text and will not be replaced.

### Valid examples

```go
// 1) Simple column mode (no custom predicate)
hooks.ScopeRule{Table: "pipelines", Column: "user_id"}

// 2) Alias-aware custom predicate
hooks.ScopeRule{
    Table: "entries",
    Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.user_id = {{param}})",
}

// 3) Multiple params in one predicate
hooks.ScopeRule{
    Table: "pipelines",
    Predicate: "({{alias}}.user_id = {{param}} OR {{alias}}.owner_id = {{param}})",
}
```

### Invalid / risky examples

```go
// Invalid intent: unknown placeholder `{{tenant}}` will NOT be replaced.
hooks.ScopeRule{
    Table: "pipelines",
    Predicate: "{{alias}}.user_id = {{tenant}}",
}

// Risky: literal interpolation should NOT be used for user-controlled data.
// Prefer {{param}} + resolver args instead.
hooks.ScopeRule{
    Table: "pipelines",
    Predicate: "{{alias}}.user_id = 123",
}
```

### Resolver behavior summary

- `ResolveArgs` on the rule has highest priority.
- If rule `ResolveArgs` is nil, hook default resolver is used.
- If no resolver is available and `{{param}}` is required, request is denied with code `resolver_required`.

## Common predicate recipes

### 1) Owner-only access (single-user tenancy)

```go
hooks.ScopeRule{Table: "pipelines", Column: "user_id"}
```

- Use when each row has a direct `user_id` owner.

### 2) Organization/tenant access

```go
hooks.ScopeRule{Table: "projects", Column: "org_id"}
```

- Default resolver should provide `org_id` from context.

### 3) Owner OR delegated owner

```go
hooks.ScopeRule{
  Table: "pipelines",
  Predicate: "({{alias}}.user_id = {{param}} OR {{alias}}.owner_id = {{param}})",
}
```

- Uses two `{{param}}` values; single value is auto-repeated.

### 4) Parent-child scoping (child table inherits parent visibility)

```go
hooks.ScopeRule{
  Table: "entries",
  Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.user_id = {{param}})",
}
```

- Use for child rows that should be visible only if parent is visible.

### 5) Org + role-constrained visibility

```go
hooks.ScopeRule{
  Table: "documents",
  Predicate: "({{alias}}.org_id = {{param}} AND {{alias}}.classification <= {{param}})",
}
```

- Resolver can return `[orgID, maxClassification]`.

### 6) Soft-delete aware scoping (always hide deleted rows)

```go
hooks.ScopeRule{
  Table: "pipelines",
  Predicate: "({{alias}}.user_id = {{param}} AND {{alias}}.deleted_at IS NULL)",
}
```

### 7) Write-path parity (UPDATE/DELETE protection)

Use the same table rule for reads and writes:

```go
hooks.ScopeRule{Table: "pipelines", Column: "user_id"}
```

- This ensures `UPDATE pipelines ...` and `DELETE FROM pipelines ...` are also constrained.

### 8) Trusted maintenance bypass (guarded)

```go
scope.SetAllowTrustedBypass(true).SetRequireBypassToken(true)

ctx := hooks.WithTrustedScopeBypass(context.Background(), "backfill-job")
_, _ = db.ExecContext(ctx, "/* scope:bypass */ UPDATE pipelines SET ...")
```

- Requires both trusted context and token when token requirement is enabled.

## Current limitations

- SQL rewriting is token-based, not a full SQL AST parser.
- Vendor-specific syntax edge cases may require custom predicates/rules.
- For strongest guarantees on highly sensitive workloads, use DB-native RLS as primary enforcement.

## Enforcement modes

- `SetStrictMode(true)`
  - Fails when resolver/context values are missing.
  - Fails when a protected statement cannot be scoped.
- `SetRejectUnknownShapes(true)`
  - Fails when parser cannot confidently interpret statement shape.
- `SetStrictAllTables(true)`
  - Fails when a discovered top-level table has no matching `ScopeRule`.
  - Useful in tightly controlled schemas.
- `SetAllowTrustedBypass(true)`
  - Enables bypass requests from trusted context only.
- `SetRequireBypassToken(true)`
  - Requires per-query token (default `/* scope:bypass */`) in addition to trusted context.
  - Guardrail to prevent accidental bypass.

## Deterministic deny codes

- `missing_context`
- `unknown_shape`
- `missing_rule`
- `resolver_required`
- `resolver_failed`
- `param_mismatch`
- `unscoped_statement`
- `unsupported_statement`
- `bypass_not_allowed`
- `bypass_missing_reason`
- `bypass_token_required`

Map errors in APIs:

```go
if code, ok := hooks.ScopeDenyCodeFromError(err); ok {
    // map code to HTTP / gRPC status
}
```

## Trusted bypass controls

Bypass requires a trusted context marker and (by default) a per-query token.

```go
ctx := hooks.WithTrustedScopeBypass(context.Background(), "internal-maintenance-job")
_, err := db.ExecContext(ctx, "/* scope:bypass */ UPDATE pipelines SET name = ?", "system-update")
```

If token is missing while `SetRequireBypassToken(true)` is enabled, deny code is `bypass_token_required`.

## Production guidance

1. Always use `*Context` query methods and attach tenant/user identity in context.
2. Use `SetStrictMode(true)` in production.
3. Use `SetRejectUnknownShapes(true)` for fail-closed query-shape enforcement.
4. Define explicit `ScopeRule` for every protected table.
5. Add integration tests for your real query shapes (joins, unions, report queries).
6. For highly sensitive multi-tenant data, enforce DB-native RLS in addition to this hook.

## Examples

See runnable example: `examples/sqlite.go`

See fail-closed example: `examples/reject_unknown.go`

See positive/negative matrix: `RESOURCE_SCOPING_TEST_CASES.md`
