# Squealx Production Hardening Audit

Audit baseline: uploaded repository snapshot, locally initialized as Git commit `e0e2972`.

## Implemented corrections

### Query and row lifecycle

- `Row.Err` no longer closes or consumes a successful row.
- Nil/deferred row errors now return safely instead of panicking.
- Hook failures from `QueryRow`/`QueryRowx` are preserved in an error row.
- Result rows are closed when an after-hook rejects a query result.
- `ConnectContext` closes the newly opened pool when ping fails.
- `MustExecContext` now panics consistently on sanitization errors.
- Strict missing-column detection is restored; `Unsafe` is now meaningful.
- Nil and non-pointer select destinations return errors rather than reflection panics.
- Generic pointer results and nil generic type discovery are handled correctly.

### Hooks and observability

- Hook registration is concurrency-safe and uses an atomic immutable snapshot on the query hot path.
- Hook panics are recovered as `HookPanicError` with phase and stack trace.
- Error hooks transform/chains errors deterministically.
- `DB.Unsafe` preserves database identity, cached metadata, mapper, and hooks.
- Logger timing no longer panics when context state is absent.
- Named sensitive arguments are redacted by default; argument logging can be disabled or customized.
- Added a dependency-free lock-free metrics hook with query/error/slow/in-flight/latency counters.

### SQL correctness and portability

- Corrected the reversed SQLite and SQL Server database-name queries.
- Added unsupported-driver errors and database-name caching.
- Replaced regex/string clause mutation with a lexical scanner that ignores strings, comments, nested queries, quoted identifiers, and PostgreSQL dollar-quoted bodies.
- Added dialect-correct one-row limiting for PostgreSQL/MySQL/SQLite, SQL Server, and Oracle-style drivers.
- `RETURNING` normalization now targets only the outer statement.
- `IN` expansion ignores question marks inside SQL text and comments.
- Template render errors are returned and safety checks evaluate the rendered SQL.
- SQLite write-return operations now use native atomic `RETURNING`.
- Write-return primary keys may be numeric, string, or UUID-like values and honor `db` tags.

### Context and repository behavior

- Added `SmartSelectContext` and `SelectTypedContext` for context-preserving positional, named, and `IN` queries.
- Repository reads, raw reads, relation preloads, and write-return operations now preserve caller cancellation/deadlines.
- Relation preload query values preserve their original types rather than coercing every key to a string.
- Relation lookup keys include the Go type to prevent collisions such as integer `1` and string `"1"`.

### Resolver and pool behavior

- Resolver construction now rejects missing primaries.
- Resolver selection returns errors internally rather than panicking.
- A default database is used only when it belongs to the operation's read/write candidate set.
- Registration and lookup are concurrency-safe and duplicate IDs are suppressed.
- Random balancing is lock-free and avoids package-global `math/rand` contention.
- Empty round-robin/random candidate sets are safe.
- Connection-failure detection recognizes `driver.ErrBadConn`, EOF, network errors, SQLSTATE class `08`, and common driver messages.
- Context execution now calls the underlying context-aware API.
- Write-return and lazy write operations are routed to masters, not read replicas.
- Pool-wide setters, ping, close, and mapper updates use stable snapshots.
- Added validated `PoolConfig` and `HealthContext` snapshots.

### Resilience

- Added explicit bounded retries with exponential backoff, jitter, context cancellation, and transient error classification.
- Writes are never retried implicitly.
- Transaction retry refuses to replay after an ambiguous commit error.
- Added a concurrency-safe circuit breaker with open/half-open/closed states and one half-open probe.

### Streaming cursor

- Added `Cursor[T]`, `QueryCursor`, `QueryCursorConfig`, `NamedQueryCursor`, `InQueryCursor`, and cursor construction from existing rows.
- The cursor caches its scan plan, reuses row storage, is O(1) memory, auto-closes, supports defensive limits, and exposes statistics.
- Added integration tests and comparative benchmarks.

## Validation completed

- `go test ./...`: pass.
- `go test -race ./...`: pass.
- `go vet ./...`: pass.
- SQLite cursor, named/IN query, hook, strict scan, row lifecycle, database-name, and write-return integration tests: pass.
- Resolver concurrent registration/selection race tests: pass.

The build environment could not access the public Go proxy or GitHub over DNS. Validation therefore used the repository's cached dependencies plus a local test-only SQLite `database/sql` adapter backed by the system SQLite library. That adapter and all local dependency shims are excluded from the delivery archive.

## Deployment checks still required

These are environment/infrastructure validations rather than unimplemented source placeholders:

1. Run the included GitHub Actions database-integration workflow against real supported PostgreSQL, MySQL, SQL Server, and modernc SQLite drivers.
2. Run benchmarks with the production schema, driver configuration, network latency, and pool limits.
3. Configure database-native statement/lock timeouts, least-privilege accounts, TLS verification, secrets rotation, backups, and failover.
4. For non-`RETURNING` drivers, `ExecWithReturnContext` uses metadata plus a follow-up primary-key read. Use an explicit application transaction when the fetched state must be isolated from concurrent updates.
5. Choose and add a license before external distribution; selecting a license is a legal/product-owner decision.
6. Define an organization-specific support window for Go and database server versions.

## Recommended CI gates

```bash
go mod tidy
git diff --exit-code -- go.mod go.sum
go test ./...
go test -race ./...
go vet ./...
(cd drivers/sqlite && go test -run '^$' -bench . -benchmem)
```

Add driver-specific integration jobs, fuzz targets for bind/query parsers, vulnerability scanning, and API-compatibility checks before tagging a release.
