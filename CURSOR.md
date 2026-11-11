# Squealx Streaming Cursor

`Cursor[T]` is the low-memory query path for large result sets. It consumes `database/sql` rows directly, builds the reflection/scan plan once, and reuses one destination plus one scan-argument slice for the lifetime of the cursor.

## Properties

- O(1) row memory unless the caller collects rows.
- Pull-based; no goroutine, channel, queue, or hidden buffering.
- Context-aware query opening and cancellation.
- Struct, pointer-to-struct, scalar, pointer-scalar, and string-keyed map results.
- Strict column-to-field validation by default; `db.Unsafe()` opts into ignoring unknown columns.
- Named and positional `IN` query variants.
- Automatic close at EOF and on decode failure.
- Defensive `MaxRows` limit and progress statistics.
- Cursor elapsed time stops accumulating when the cursor closes.
- Invalid negative row limits return `ErrInvalidCursorConfig` instead of silently removing the bound.
- Reusable pointer/map storage with an explicit `Copy` method for retained rows.

## Typed cursor

```go
cursor, err := squealx.QueryCursor[User](ctx, db,
    "SELECT id, name FROM users WHERE id > ? ORDER BY id", lastID)
if err != nil {
    return err
}
defer cursor.Close()

for cursor.Next() {
    user := cursor.Value()
    if err := consume(user); err != nil {
        return err
    }
}
return cursor.Err()
```

## Bounded query

```go
cursor, err := squealx.QueryCursorConfig[Event](ctx, db,
    squealx.CursorConfig{MaxRows: 100_000},
    "SELECT id, payload FROM events ORDER BY id")
```

Attempting to advance beyond `MaxRows` returns `ErrCursorLimit` and closes the rows.

## Pointer and map lifetimes

For `Cursor[*T]` and `Cursor[map[string]any]`, `Value` is reused on the next `Next` call. This eliminates a row allocation. Process the value immediately or retain an independent shallow copy:

```go
for cursor.Next() {
    saved = append(saved, cursor.Copy())
}
```

`Collect` automatically uses independent copies.

## Named and `IN` cursors

```go
cursor, err := squealx.NamedQueryCursor[User](ctx, db,
    "SELECT id,name FROM users WHERE tenant_id=:tenant AND id IN :ids",
    map[string]any{"tenant": tenantID, "ids": ids})

cursor, err = squealx.InQueryCursor[User](ctx, db,
    "SELECT id,name FROM users WHERE id IN (?)", ids)
```

## Performance validation

The included SQLite end-to-end benchmark decodes 1,000 rows. On the validation host, the cursor used approximately 39 KB and 3,768 allocations per query versus approximately 227 KB and 10,768 allocations for the prior callback scanner. Driver, CGO, database, schema, and hardware costs dominate absolute timings; run the benchmarks on the deployment platform:

```bash
cd drivers/sqlite && go test -run '^$' \
  -bench 'CursorStruct1000|SelectEachStruct1000' -benchmem -count=5
```

## Iterators, sinks and pagination

```go
// Range-over-func; the cursor closes when the loop ends or breaks.
for user, err := range squealx.QueryIter[User](ctx, db, "SELECT id,name FROM users") {
    if err != nil { return err }
    use(user)
}
```

- `Cursor.All()`, `TxQueryCursor`, `ConnQueryCursor`, `StmtQueryCursor` open streaming cursors from any handle; each has a `*Config` variant for row limits.
- `FindCursor(ctx, repo, conditions)` streams a built-in repository query with its filters and context query parameters. It returns `ErrRepositoryCursorPreload` when relation preloads are configured. The helper keeps cursor support optional for custom `Repository` implementations.
- `FindPreloadedCursor` streams preloaded repository rows in batches of 500 parents by default, bounding parent-row buffering while preserving batched relation queries. `FindPreloadedCursorConfig` accepts cursor row limits; `FindPreloadedCursorWithConfig` also allows tuning `BatchSize`. Because related queries run while the parent cursor is open, the DB pool must allow at least two open connections; a one-connection pool returns `ErrPreloadedCursorPool` before opening the query.
- `PreloadedCursor.Each` and `PreloadedCursor.All` consume preloaded rows and close the underlying cursor on completion, callback errors, or early range-loop exit.
- `RawCursor(ctx, repo, queryKey, args...)` and `RawCursorConfig` stream repository raw queries only after the existing raw-query allowlist resolves the key.
- `NamedQueryCursorConfig`, `InQueryCursorConfig` and `FindCursorConfig` accept the same `CursorConfig`, including `MaxRows`, as positional cursors.
- `SelectEach` / `ScanEach` build the scan plan once and reuse it per row. `SelectEachContext` also propagates cancellation to the database query.
- `WriteJSONLines`, `WriteCSV` and `WriteMapCursorJSONLines` stream rows to an `io.Writer` holding one row at a time.
- `LimitedBytes` caps a scanned column and clears stale data when a scan fails. Set `Reuse: true` for repeated scans to reuse buffers up to 64 KiB; in that mode `Data` is valid only until the next scan. `RawColumnScanner` reuses scan targets and routes unselected columns through one discard slot, retaining only the selected column's borrowed bytes between scans. Call `Close` when finished to release its references; this does not close the caller-owned rows. `RawColumn` uses a temporary scanner for the one-off case and returns an `io.Reader`. `database/sql` cannot stream a single column from the wire; use driver features (e.g. pgx large objects) for values larger than memory.
- `datatypes.MaxGzipDecompressedSize` (default 256 MB, 0 = unlimited) bounds `GzippedText` decompression.
- `KeysetPaginate` gives O(1)-per-page seek pagination over a unique, non-NULL key and streams through the cursor. Prefer it to `Pages` (OFFSET) for deep pages. Keyset tokens are capped at 64 KiB on encode and decode; encoding preflights scalar values and rejects composites or custom JSON marshalers before allocating the complete payload.
- `BenchmarkKeysetVsOffsetDeepPage` compares equivalent 100-row pages near row 49,000 of a 50,000-row SQLite table. On Apple M2 Pro, three 200 ms runs measured a median 48.4 µs and 9.5 KB for keyset versus 533.2 µs and 13.5 KB for OFFSET. Keyset used 365 allocations versus 335; its lower scan and retained memory came with a small per-page allocation increase for cursor handling and token generation. This single-driver benchmark is a local illustration, not a cross-driver guarantee.

## Row lifetime guarantees

Value rows (`Cursor[T]`, `SelectEach`) are safe to retain: the reused destination is cleared between rows, so pointer fields and embedded pointer structs are re-allocated per row and never alias later rows or carry stale values. `Cursor[*T]` and map rows reuse storage; call `Copy()` to retain them. `Value` is valid until the next `Next` call or `Close`; `ValueRef` and `NextRef` expose the reusable value slot without copying a struct row. Closing releases the cursor's last row storage, column schema, and `Rows` wrapper; `Columns()` returns nil after close.

## Scanning semantics

- NULL into a field yields its zero value (`nil` for pointer fields). Unparseable values yield zero rather than an error, for struct scans.
- Fast paths avoid reflection for native `database/sql` types; anything else falls back to the lenient path.
- Map/slice scans: integers are `int64`, `DECIMAL` is an exact string (set `squealx.DecimalAsFloat = true` for `float64`), and values that fail conversion keep their original string instead of becoming zero.
- Typed maps with `[]byte` values preserve binary SQL columns such as `BLOB` and `BYTEA`; the returned bytes are copied so they remain valid after the next row is scanned.

## Performance notes

- `Select` into `[]T` scans rows in place into the slice's backing array; scan arguments are retargeted per row instead of rebuilt (no per-row `reflect.New`/`Append`, no per-row wrapper allocations).
- Typed map `Select` paths decode directly into their destination maps instead of building a temporary `map[string]any` per row. On the local 100-row SQLite benchmark this reduced allocations from 1,336 to 1,136 and bytes from about 82.7 KB to 48.7 KB per query; median runtime moved from about 90.5 µs to 75.7 µs across three 250 ms runs on Apple M2 Pro.
- Compiled named queries (`:name` parsing) are cached (bounded, concurrency-safe); `IsNamedQuery` is allocation-free for queries without a `:name` candidate.
- Named `IN` expansion reuses the generated placeholder string as map-key storage and avoids a temporary values slice. On an Apple M2 Pro, the 500-item benchmark improved from 51.6 µs, 84.7 KB and 1,412 allocations to 27.9 µs, 65.2 KB and 511 allocations (local medians; run against application workloads before tuning).
- With no hooks registered, `Query`/`Exec`/`Get` skip the hook pipeline and driver-name context entirely.
- `Get`/`Select` only fetch `ColumnTypes` for map destinations.
- `[]*scalar` destinations map NULL to a nil element.
- Benchmarks: `cd drivers/sqlite && go test -run '^$' -bench 'Op|Cursor|SelectEach|Repository' -benchmem` (`ops_bench_test.go` compares squealx with raw `database/sql`).
- `BenchmarkRepositoryRead1000` compares `Repository.Find` with `FindCursor`. On an Apple M2 Pro with SQLite, three 200 ms runs used about 30.2 KB/op for `FindCursor` versus 101.4 KB/op for `Find`; runtime was 298–300 µs/op versus 396–402 µs/op. Allocation counts stayed similar (2,774 vs 2,785), so the main measured gain is lower retained/result-slice memory. Treat these as a local baseline, not a cross-driver guarantee.
- `BenchmarkSQLiteFilePoolConcurrentRead` compares file-backed SQLite read throughput with pool limits 1, 2, 4, and 8. Three 1 s runs on an Apple M2 Pro measured median 16.0, 11.2, 6.7, and 5.4 µs/op respectively, with roughly 1.3–1.4 KB/op and 27–28 allocations. The read-only result favors more connections, but does not establish a safe default for write-heavy workloads; tune pool limits for the application workload.
- `BenchmarkSQLiteFilePoolMixedReadWrite` adds one update per nine reads, with WAL and a 5 s busy timeout. Three 1 s runs on the same host measured median 11.1, 7.8, 6.0, and 4.8 µs/op for pools of 1, 2, 4, and 8 connections, with 735–773 B/op and 22 allocations; no lock errors occurred. `drivers/sqlite.Open` now bounds file/shared-memory pools at four open and two idle connections; increase the limit when measured workload throughput warrants it.
- For the same 1,000-row integer/text query, `WriteJSONLines` streaming directly from `Rows` measured a median 607 µs/op, 96.4 KB, and 2,769 allocations, versus 783 µs/op, 161 KB, and 6,771 allocations for `WriteMapCursorJSONLines` (five 500 ms runs on Apple M2 Pro with SQLite). Use the `Rows` sink when raw scalar output is sufficient; choose the map cursor path when its map conversions are needed.
- `BenchmarkRepositoryPreloadRead1000` compares `Find` with `FindPreloadedCursor` for 1,000 parents and related rows. On the Apple M2 Pro with SQLite, map rows measured about 1.73 MB/op, 32,246 allocations, and 3.11 ms/op for the cursor versus 3.31 MB/op, 76,279 allocations, and 6.49 ms/op for `Find`. Struct rows measured about 2.16 MB/op and 5.00 ms/op versus 2.26 MB/op and 5.30 ms/op, with roughly 1,000 more allocations for the cursor path. Each figure is the median of 3 runs at 200 ms; other drivers and workloads need separate measurements.
- After relation matching switched from formatted string keys to normalized comparable map keys, `PreloadedCursor` map rows measured 1.46 MB/op, 25,648 allocations, and 2.60 ms/op (median of 5 runs at 200 ms, same machine and SQLite benchmark). That is about 16% less allocated memory, 20% fewer allocations, and 16% lower runtime than the earlier cursor result; treat this as a local benchmark, not a cross-driver guarantee.
- A local 1,000-row SQLite comparison (`CursorStruct1000` vs `RawSQLRowsStruct1000`, three 200 ms runs on Apple M2 Pro) measured a median 581 µs, 31.0 KB and 2,764 allocations for the cursor, versus 613 µs, 61.7 KB and 4,659 allocations for direct `database/sql` scanning. Driver and host affect these figures; run the same benchmarks on the deployment workload before tuning.
- Reusing `RawColumnScanner` for 1,000 SQLite rows measured a median 468 µs, 32.7 KB and 2,015 allocations, versus 560 µs, 176.6 KB and 5,013 allocations for calling `RawColumn` per row (three 300 ms runs on Apple M2 Pro). The returned bytes are borrowed until the next `Rows.Next` or `Rows.Close`.
- The wide-row scanner benchmark compares the shared discard target with one `RawBytes` slot per column. On an Apple M2 Pro, three 300 ms runs at 64 columns measured median 4.07 ms and 413.5 KB for the shared target versus 4.13 ms and 415.1 KB for per-column clearing; at 128 columns it measured 7.64 ms and 800.2 KB versus 7.76 ms and 803.3 KB. Allocations were equal, while the shared target retains fewer unrelated value references.
- The JSON sink's direct string appender measured 65 ns/op with 0 allocations versus 144 ns/op, 80 B and 2 allocations for `json.Marshal` on a representative string (3 runs on Apple M2 Pro). Its escaping output is checked against `encoding/json`.
- Reusing `LimitedBytes` for a 1 KiB value measured 29–30 ns/op with 0 allocations, versus 147–159 ns/op, 1 KiB and one allocation with the default copy-per-scan behavior (3 runs on Apple M2 Pro). Reuse is opt-in because `Data` then aliases the same buffer until the next scan.
- Direct JSON appends for a timestamp and invalid binary value measured 38 ns / 11.8 ns with zero allocations, versus 177–182 ns / 121–126 ns and 128 B / 56 B with four allocations for formatting plus `json.Marshal` (3 runs on the same host).
- Direct float appends measured 30.7 ns/op with zero allocations versus 109–110 ns/op, 24 B and 2 allocations for `json.Marshal` (3 runs on the same host). Float formatting follows `encoding/json` thresholds; non-finite values still return an unsupported-value error.
- Valid UTF-8 `[]byte` values now append directly without converting to `string`; the benchmark measured 54 ns/op and zero allocations, versus 65 ns/op, 48 B and one allocation before the change (3 runs on the same host).
- `WriteMapCursorJSONLines` measured 772–795 µs, 161 KB and 6,771 allocations for 1,000 rows, versus 1.22–1.25 ms, 200 KB and 14,767 allocations for encoding each map with `json.Encoder` (3 runs on the same host). Starting from SQL rows directly, `WriteJSONLines` remains lighter because it skips map materialization.
- One-shot struct scans (`Get`, `Select`) draw their scan arguments, object context and NULL-safe wrappers from a pooled scratch, so they allocate none of it; field traversals are cached per mapper (`Mapper.TraversalsByNameCached`) by type and column list, with per-shape limits and a 4 MiB estimated aggregate budget. Struct type mappings also have per-type limits and an 8 MiB estimated aggregate budget.
- `reflectx.BenchmarkTypeMapCachedLookupParallel` measures concurrent cached mapper lookups; on an Apple M2 Pro it measured 116.6 ns/op with zero allocations. The mapper uses a read lock for cache hits and serializes only misses/evictions. This is a local baseline, not a before/after comparison.
- On the same SQLite benchmark, single-row `Get` into a typed struct measured a median 5.98 µs, 748 B, and 24 allocations versus 6.32 µs, 1.58 KB, and 39 allocations for `Get` into `map[string]any` (three 200 ms runs on Apple M2 Pro). Prefer typed destinations on hot paths when the result shape is known.
- `SanitizeQuery`/`SafeQuery` no longer force the caller's variadic argument slice onto the heap when `EnableSafeQuery` is off.
- Remaining per-call allocations are the driver/`database/sql` ones plus the `Row`/`Rows` objects and the variadic slice passed through the `SQLDB` interface.
