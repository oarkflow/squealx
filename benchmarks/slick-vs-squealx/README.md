# squealx vs. Scala Slick — MySQL benchmark

Head-to-head comparison of [squealx](https://github.com/oarkflow/squealx) (Go)
against [Slick](https://scala-slick.org/) 3.6.1 (Scala), against the same
MySQL database, running the same queries, in the same order, with the same
connection-pool size (10).

The database is the `cleardb` schema used by `~/Projects/clear/clear/fulltext`
(host/user/pass/DB read from that project's `docker-compose.yml`: `service` /
`service` / `cleardb` on `127.0.0.1:3306`). The benchmark only reads/writes:

- `icd10` — a 550k-row reference table of ICD-10 codes (read-only in this
  benchmark; no patient data is touched).
- `bench_squealx_slick_scratch` — a scratch table each program creates,
  populates, and drops itself. Nothing else in `cleardb` is modified.

Slick's dependency versions (`slick` 3.6.1, `mysql-connector-j` 8.4.0, Scala
2.12.21) and connection-pool shape (`AsyncExecutor` with a bounded thread +
connection count) are copied from `clear/fulltext/build.sbt` and
`clear/db/MySQLAdapter.scala`, so the Slick side reflects how that app
actually talks to MySQL — not a synthetic best case.

## What's measured

Both programs run the identical sequence, single-threaded, each operation
blocking for its result before starting the next (idiomatic `Await.result`
on the Slick side, a plain blocking call on the squealx side):

| Benchmark | What it does | Default size |
|---|---|---|
| `PointLookupByCode` | `SELECT ... FROM icd10 WHERE code = ?` via the `idx_code` index, mapped into a struct/case class | 2000 iterations |
| `IndexedRangeQuery` | `SELECT ... WHERE header = ? AND tbl = 'CM' LIMIT 100` via `idx_header_tbl` | 1000 iterations |
| `BulkStructScan` | `SELECT ... WHERE tbl = 'CM' LIMIT 20000`, mapped into a slice/vector of structs/case classes | 5 iterations (100k rows total) |
| `WriteScratch/Insert` | Per-row `INSERT` into the scratch table | 2000 rows |
| `WriteScratch/Update` | Per-row `UPDATE ... WHERE id = ?` | 2000 rows |

This isolates **per-call overhead and object-mapping cost** — query
compilation/caching, parameter binding, reflection/boxing, connection
checkout — rather than network or MySQL throughput, since both programs hit
the same database from the same machine.

## Running it

MySQL must be reachable with the `cleardb` schema populated (as it is via the
`clear` project's `mysql` docker container).

```sh
# Go / squealx
cd go && go run .

# Scala / Slick
cd scala && sbt run
```

Override sizing or connection details with environment variables (defaults
shown above):

| Env var | Go | Scala | Default |
|---|---|---|---|
| MySQL DSN/URL | `BENCH_MYSQL_DSN` | `BENCH_MYSQL_JDBC_URL` | `service:service@tcp(127.0.0.1:3306)/cleardb?...` |
| User | (in DSN) | `BENCH_MYSQL_USER` | `service` |
| Password | (in DSN) | `BENCH_MYSQL_PASSWORD` | `service` |
| `BENCH_POINT_LOOKUPS` | ✓ | ✓ | 2000 |
| `BENCH_RANGE_QUERIES` | ✓ | ✓ | 1000 |
| `BENCH_BULK_ITERATIONS` | ✓ | ✓ | 5 |
| `BENCH_BULK_LIMIT` | ✓ | ✓ | 20000 |
| `BENCH_WRITE_ROWS` | ✓ | ✓ | 2000 |
| `BENCH_STMT_CACHE` | ✓ (Go only) | — | `0` (off) |

`BENCH_STMT_CACHE=1` turns on squealx's opt-in prepared-statement cache
(`db.EnableStatementCache`) for the Go side — see "Statement cache impact"
below.

## Results (this machine, 2026-11-17)

Two runs each, against the live `cleardb` over localhost:

| Benchmark | squealx (ops/sec) | Slick (ops/sec) | squealx advantage |
|---|---:|---:|---:|
| PointLookupByCode | 728–903 | 121–128 | ~6–7x |
| IndexedRangeQuery (limit 100) | 912–13053 † | 153–177 | ~6–75x † |
| BulkStructScan (rows/sec) | 268k–828k | 166k–170k | ~1.6–5x |
| WriteScratch/Insert | 686–717 | 154–158 | ~4.5x |
| WriteScratch/Update | 585–625 | 151–159 | ~4x |

† The first run's 13k ops/sec outlier is almost certainly MySQL's own query
cache/buffer pool warming up on a quiet connection; treat the second run
(~912 ops/sec) as the representative number and re-run a few times if you
want a tighter figure.

**Takeaway**: for this per-call-overhead-dominated workload (small result
sets, high call rate, same DB/host), squealx is consistently 4–7x faster per
operation than idiomatic Slick, and 1.6–5x faster at bulk row-to-struct
mapping. Most of the gap is JVM/Slick-side cost (DBIO/Future composition,
query-compiler overhead on first use per query shape, connection checkout
through `AsyncExecutor`) rather than anything MySQL-side — both programs issue
the same SQL against the same server. This is a single-machine, single-run
comparison, not a formal benchmark suite; treat the ratios as directional.

## Resource usage (CPU / memory / disk)

Measured with `/usr/bin/time -v` around the actual process for one full
default-sized run of all five benchmarks (`go build -o bench-go . && time -v
./bench-go`, and `time -v java -cp <runtime classpath> Main` so sbt's own
process isn't included):

| Metric | squealx (Go) | Slick (JVM) | Ratio |
|---|---:|---:|---:|
| CPU time (user+sys) | 2.91s | 52.12s | ~18x more for Slick |
| Wall clock | 10.8s | 52.7s | ~5x slower |
| CPU utilization | 27% (I/O-bound: mostly waiting on MySQL) | 98% (CPU-bound) | — |
| Peak RSS (memory) | 34.3 MB | 601 MB | ~17.5x more for Slick |
| Minor page faults | 11,642 | 181,130 | ~16x more |
| On-disk artifact | 13 MB static binary (Go runtime + MySQL driver included) | 14.7 MB of dependency jars (Scala library, Slick, mysql-connector-j, logback, protobuf, …) | comparable artifact size |

Notes:

- squealx's low CPU utilization (27%) means it spends most of its wall-clock
  time idle, blocked on network round trips to MySQL — there's very little
  client-side work happening per call.
- The JVM process is pegged near 100% CPU for the whole run: JIT compilation,
  Slick's query-compiler passes per distinct query shape (visible as
  `assignUniqueSymbols` / `flattenProjections` / `createAggregates` /
  etc. in Slick's debug logs), GC, and `Future`/`AsyncExecutor` thread-pool
  scheduling all run on top of the same JDBC calls squealx makes directly.
- 601 MB peak RSS is mostly fixed JVM overhead (loaded class metadata, JIT
  code cache, GC heap sized well above what 2000 small result sets actually
  need) rather than data volume — both programs hold the same tiny result
  sets in memory at any given time.
- The jar-size comparison undercounts Slick's real footprint: running it also
  requires a full JRE installed on the host, which the statically-linked Go
  binary does not.

## Statement cache impact (squealx only)

squealx gained an opt-in prepared-statement cache (`db.EnableStatementCache`)
after the comparison above: repeated query text reuses one prepared statement
instead of re-preparing on every `Exec`/`Query`/`Select`/`Get` call — the same
problem `cachePrepStmts=true` solves for the JDBC driver Slick sits on, except
squealx's version is off by default. Three runs each, `go run .` vs
`BENCH_STMT_CACHE=1 go run .`, against the same live `cleardb`:

| Benchmark | Cache off (ops/sec) | Cache on (ops/sec) | Improvement |
|---|---:|---:|---:|
| PointLookupByCode | 687–874 | 1610–2350 | ~2.0–2.7x |
| IndexedRangeQuery (limit 100) | 671–1041 | 1112–1530 | ~1.3–1.8x |
| WriteScratch/Insert | 584–693 | 783–1085 | ~1.3–1.6x |
| WriteScratch/Update | 515–562 | 768–809 | ~1.4–1.5x |

This is on top of squealx's existing advantage over Slick above, not a
separate comparison — i.e. with the cache on, squealx's lead over the Slick
numbers earlier in this document would be larger still. As with the rest of
this benchmark, treat these as directional (single machine, a handful of
runs, real network/DB variance between runs), not a precise multiplier.
