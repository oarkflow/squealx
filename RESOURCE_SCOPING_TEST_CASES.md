# Resource Scoping Test Cases

This document lists practical positive and negative test cases for the resource scoping hook.

## Setup baseline

- Register scope hook with rules for protected tables.
- Use context key carrying tenant/user identifier.
- Use context-aware query APIs.

Recommended production setup:

```go
scope := hooks.NewResourceScopeHook(
    hooks.ArgsFromContextValue(userIDKey),
    hooks.ScopeRule{Table: "pipelines", Column: "user_id"},
    hooks.ScopeRule{Table: "entries", Predicate: "{{alias}}.pipeline_id IN (SELECT p.pipeline_id FROM pipelines p WHERE p.user_id = {{param}})"},
).
SetStrictMode(true).
SetRejectUnknownShapes(true).
SetAllowTrustedBypass(true).
SetRequireBypassToken(true)

// Optional extra strictness
scope.SetStrictAllTables(true)

db.Use(scope)
```

## Positive cases (should pass)

1. Plain select
- Input: `SELECT * FROM pipelines`
- Expectation: rows limited to context user only.

2. Select with where clause
- Input: `SELECT * FROM pipelines WHERE name LIKE ?`
- Expectation: existing predicate preserved, scope predicate appended with `AND`.

3. Select with joins
- Input: `SELECT ... FROM entries e JOIN pipelines p ...`
- Expectation: both protected tables get scoped predicates (as configured by rules).

4. Nested subquery
- Input: `SELECT ... WHERE id IN (SELECT ... FROM entries ...)`
- Expectation: nested select gets scoped; outer select gets scoped.

5. CTE
- Input: `WITH cte AS (...) SELECT ... FROM pipelines ...`
- Expectation: CTE/select blocks are scoped where applicable.

6. Update
- Input: `UPDATE pipelines SET name=? WHERE pipeline_id=?`
- Expectation: update affects only rows belonging to context user.

7. Delete
- Input: `DELETE FROM entries WHERE pipeline_id=?`
- Expectation: delete affects only rows visible to context user scope.

8. Transaction path
- Input: `tx.SelectContext(...)`, `tx.ExecContext(...)`
- Expectation: same scoping behavior as DB-level calls.

9. Placeholder styles
- Input: SQL using `?`, `$1..$n`, `@p1..@pn`
- Expectation: new scoped params are injected with correct style and argument order.

## Negative cases (should fail closed)

1. Missing context value
- Mode: `SetStrictMode(true)`
- Input: query without user value in context
- Expectation: resolver error (no query execution).

2. Resolver returns wrong arg count
- Mode: strict on
- Input: rule expects N `{{param}}` but resolver returns M args
- Expectation: explicit arg-count error.

3. Unknown/unsupported statement shape
- Mode: `SetRejectUnknownShapes(true)`
- Input: statement parser cannot classify confidently
- Expectation: explicit reject error.

4. Missing rule for discovered table
- Mode: `SetStrictAllTables(true)`
- Input: statement references table without `ScopeRule`
- Expectation: explicit missing-rule error.

5. Statement with no scoping target
- Mode: strict or reject-unknown enabled
- Input: protected operation with no resolvable scoped table
- Expectation: explicit rejection.

6. Bypass requested without trusted context
- Mode: bypass enabled
- Input: query contains bypass token but context has no trusted bypass marker
- Expectation: reject with `bypass_not_allowed`.

7. Trusted bypass without token
- Mode: `SetRequireBypassToken(true)`
- Input: trusted bypass context but no per-query token
- Expectation: reject with `bypass_token_required`.

8. Trusted bypass with token
- Mode: bypass enabled
- Input: trusted bypass context + token in query
- Expectation: query passes through unmodified; audit action `bypassed`.

9. Deny-code mapping
- Input: capture returned error and call `hooks.ScopeDenyCodeFromError(err)`
- Expectation: deterministic code for API mapping.

## Audit assertions

- Register `SetAuditSink(...)` and assert:
  - `Action` is one of: `scoped`, `rejected`, `bypassed`, `passthrough`
  - `ReasonCode` is populated for rejected decisions
  - `MatchedTables` and `AppliedRules` are populated for scoped decisions

## Mode behavior summary

- Strict mode only:
  - Rejects missing context/resolver failures and unscoped protected statements.
  - Does not require every discovered top-level table to have a rule.

- Strict mode + reject unknown:
  - Adds fail-closed behavior for parser-ambiguous SQL shapes.

- Strict mode + strict all tables + reject unknown:
  - Maximum fail-closed behavior.
  - Best for regulated/high-risk multi-tenant workloads.

## Manual verification checklist

- Run positive example:
  - `cd examples && go run sqlite.go`
- Run negative/fail-closed example:
  - `cd examples && go run reject_unknown.go`
- Validate root build:
  - `cd .. && go test ./...`

## Notes

- This is application-level enforcement, not a substitute for database-native authorization.
- For strong security posture, combine with database RLS and least-privilege DB roles.
