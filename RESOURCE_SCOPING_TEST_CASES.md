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

6a. Update with source tables
- Input: `UPDATE pipelines p SET name=? FROM users u WHERE u.id = p.user_id`
- Expectation: both update target and source tables are rule-checked/scoped.

6b. Insert from query source
- Input: `INSERT INTO audits (pipeline_id) SELECT p.pipeline_id FROM pipelines p WHERE p.name = ?`
- Expectation: source query is scoped the same way as normal SELECT.

6c. Insert from query source with conflict handling tail
- Input: `INSERT INTO audits (pipeline_id) SELECT p.pipeline_id FROM pipelines p ON CONFLICT (pipeline_id) DO NOTHING`
- Expectation: scoped predicate is inserted before `ON CONFLICT` (tail clause preserved).

6d. Merge target/source scoping
- Input: `MERGE INTO pipelines p USING users u ON p.user_id = u.id WHEN MATCHED THEN UPDATE ...`
- Expectation: both target/source tables are rule-checked and scope predicates are appended to the `ON` clause.

6e. Merge with `USING (SELECT ...)`
- Input: `MERGE INTO audits a USING (SELECT p.pipeline_id FROM pipelines p) s ON ... WHEN ...`
- Expectation: nested source query is scoped and merge target is scoped in `ON`.

7. Delete
- Input: `DELETE FROM entries WHERE pipeline_id=?`
- Expectation: delete affects only rows visible to context user scope.

7a. Delete with `USING`/`JOIN` (multi-table forms)
- Input: `DELETE FROM entries e USING pipelines p WHERE e.pipeline_id = p.pipeline_id`
- Input: `DELETE e FROM entries e JOIN pipelines p ON p.pipeline_id = e.pipeline_id WHERE ...`
- Expectation: all discovered tables in delete source graph are rule-checked/scoped.

8. Transaction path
- Input: `tx.SelectContext(...)`, `tx.ExecContext(...)`
- Expectation: same scoping behavior as DB-level calls.

9. Placeholder styles
- Input: SQL using `?`, `$1..$n`, `@p1..@pn`
- Expectation: new scoped params are injected with correct style and argument order.

10. Set operations (`UNION`, `INTERSECT`, `EXCEPT`)
- Input: branch-based SELECTs joined by set operators, with or without `ALL`/`DISTINCT`.
- Expectation: each branch is scoped independently; args are injected in branch order.

## Regression-focused examples (new behavior)

1. Nested rewrite is single-pass (no duplicate re-scoping)
- Input:
  `SELECT p.pipeline_id FROM pipelines p WHERE EXISTS (SELECT 1 FROM entries e WHERE e.pipeline_id = p.pipeline_id)`
- Expected rewritten shape:
  - one `p.user_id = ?` predicate at outer level
  - one `e.user_id = ?` predicate inside `EXISTS`
  - no duplicate predicate copies from repeated rescans

2. Scalar-expression subquery is not rewritten
- Input:
  `SELECT COALESCE((SELECT MAX(p.pipeline_id) FROM pipelines p), 0) AS max_id`
- Expectation:
  - scalar-expression nested subquery stays unchanged
  - no malformed rewrite in expression contexts

3. Predicate insertion keeps SQL spacing before trailing clauses
- Input:
  `SELECT * FROM pipelines ORDER BY pipeline_id`
- Expected rewritten fragment:
  `... WHERE (pipelines.user_id = ?) ORDER BY ...`
- Expectation: no malformed `)ORDER` token joins.

4. Strict mode allows CTE-only outer select when nested body is already scoped
- Input:
  `WITH scoped AS (SELECT * FROM pipelines p) SELECT * FROM scoped`
- Expectation:
  - inner CTE body gets scope predicate on `pipelines`
  - outer `SELECT * FROM scoped` is accepted (not rejected as unscoped)
  - useful for CTE wrappers where scoping happened in nested level

5. Set operation branches are scoped independently
- Input:
  `SELECT p.pipeline_id FROM pipelines p WHERE p.name = ? UNION ALL SELECT q.pipeline_id FROM pipelines q WHERE q.name = ? ORDER BY pipeline_id`
- Expectation:
  - first branch gets `p.user_id = ?`
  - second branch gets `q.user_id = ?`
  - final SQL remains well-formed around `UNION ALL` and trailing `ORDER BY`

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

3a. Insert values unsupported in strict unknown-shape mode
- Mode: `SetRejectUnknownShapes(true)`
- Input: `INSERT INTO pipelines (name) VALUES (?)`
- Expectation: rejected as unsupported/unknown insert shape for scoping.

3b. Merge without ON-clause boundary
- Mode: `SetRejectUnknownShapes(true)`
- Input: malformed/non-standard MERGE shape without top-level `ON`
- Expectation: rejected as unknown shape (cannot safely inject scope predicate).

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
