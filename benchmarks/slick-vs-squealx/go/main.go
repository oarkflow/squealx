// Command bench-squealx runs the same read/write workload as the Scala/Slick
// counterpart (../scala) against the same MySQL database, so the two numbers
// at the end are comparable. See ../README.md for how to run both sides and
// what each benchmark actually exercises.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/oarkflow/squealx"
)

// Icd10Row mirrors the Scala Icd10Row case class field-for-field.
type Icd10Row struct {
	Ord            int       `db:"ord"`
	Code           string    `db:"code"`
	ID             int       `db:"id"`
	Header         string    `db:"header"`
	ShortDesc      string    `db:"short_desc"`
	MedDesc        string    `db:"med_desc"`
	LongDesc       string    `db:"long_desc"`
	EffectiveDate  time.Time `db:"effective_date"`
	Tbl            string    `db:"tbl"`
	CodeWithPeriod string    `db:"code_with_period"`
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envIntOr(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	dsn := envOr("BENCH_MYSQL_DSN", "service:service@tcp(127.0.0.1:3306)/cleardb?parseTime=true&loc=UTC")
	pointLookups := envIntOr("BENCH_POINT_LOOKUPS", 2000)
	rangeQueries := envIntOr("BENCH_RANGE_QUERIES", 1000)
	bulkIterations := envIntOr("BENCH_BULK_ITERATIONS", 5)
	bulkLimit := envIntOr("BENCH_BULK_LIMIT", 20000)
	writeRows := envIntOr("BENCH_WRITE_ROWS", 2000)

	db, err := squealx.Open("mysql", dsn, "bench")
	must(err)
	defer db.Close()
	must(db.ApplyPoolConfig(squealx.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 10}))
	must(db.Ping())

	stmtCache := envIntOr("BENCH_STMT_CACHE", 0) != 0
	if stmtCache {
		db.EnableStatementCache(0)
	}

	fmt.Println("squealx benchmark")
	fmt.Printf("stmt cache: %v\n", stmtCache)
	fmt.Println("==================")

	runPointLookup(db, pointLookups)
	runIndexedRange(db, rangeQueries)
	runBulkStructScan(db, bulkIterations, bulkLimit)
	runWriteScratch(db, writeRows)
}

func sampleCodes(db *squealx.DB, n int) []string {
	var codes []string
	must(db.Select(&codes, "SELECT code FROM icd10 WHERE tbl = 'CM' ORDER BY RAND() LIMIT ?", n))
	return codes
}

func report(name string, n int, elapsed time.Duration) {
	perOp := elapsed / time.Duration(n)
	fmt.Printf("%-28s n=%-7d total=%-12s avg=%-12s ops/sec=%.1f\n",
		name, n, elapsed.Round(time.Millisecond), perOp, float64(n)/elapsed.Seconds())
}

func runPointLookup(db *squealx.DB, n int) {
	codes := sampleCodes(db, max(n, 200))
	const q = `SELECT ord, code, id, header, short_desc, med_desc, long_desc, effective_date, tbl, code_with_period
		FROM icd10 WHERE code = ? LIMIT 1`
	start := time.Now()
	for i := 0; i < n; i++ {
		var row Icd10Row
		if err := db.Get(&row, q, codes[i%len(codes)]); err != nil {
			panic(err)
		}
	}
	report("PointLookupByCode", n, time.Since(start))
}

func runIndexedRange(db *squealx.DB, n int) {
	headers := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"}
	const q = `SELECT ord, code, id, header, short_desc, med_desc, long_desc, effective_date, tbl, code_with_period
		FROM icd10 WHERE header = ? AND tbl = 'CM' LIMIT 100`
	start := time.Now()
	for i := 0; i < n; i++ {
		var rows []Icd10Row
		if err := db.Select(&rows, q, headers[i%len(headers)]); err != nil {
			panic(err)
		}
	}
	report("IndexedRangeQuery(limit100)", n, time.Since(start))
}

func runBulkStructScan(db *squealx.DB, iterations, limit int) {
	const q = `SELECT ord, code, id, header, short_desc, med_desc, long_desc, effective_date, tbl, code_with_period
		FROM icd10 WHERE tbl = 'CM' LIMIT ?`
	var totalRows int
	start := time.Now()
	for i := 0; i < iterations; i++ {
		var rows []Icd10Row
		if err := db.Select(&rows, q, limit); err != nil {
			panic(err)
		}
		totalRows += len(rows)
	}
	elapsed := time.Since(start)
	fmt.Printf("%-28s n=%-7d total=%-12s avg=%-12s rows/sec=%.0f (%d rows)\n",
		"BulkStructScan", iterations, elapsed.Round(time.Millisecond), elapsed/time.Duration(iterations),
		float64(totalRows)/elapsed.Seconds(), totalRows)
}

func exec1(db *squealx.DB, query string, args ...any) {
	_, err := db.Exec(query, args...)
	must(err)
}

func runWriteScratch(db *squealx.DB, n int) {
	exec1(db, `DROP TABLE IF EXISTS bench_squealx_slick_scratch`)
	exec1(db, `CREATE TABLE bench_squealx_slick_scratch (
		id INT PRIMARY KEY AUTO_INCREMENT,
		code VARCHAR(16) NOT NULL,
		val1 INT NOT NULL,
		val2 VARCHAR(64) NOT NULL
	) ENGINE=InnoDB`)
	defer db.Exec(`DROP TABLE IF EXISTS bench_squealx_slick_scratch`)

	rng := rand.New(rand.NewSource(1))

	start := time.Now()
	for i := 0; i < n; i++ {
		exec1(db, `INSERT INTO bench_squealx_slick_scratch (code, val1, val2) VALUES (?, ?, ?)`,
			fmt.Sprintf("C%06d", i), rng.Intn(1_000_000), fmt.Sprintf("val-%d", i))
	}
	insertElapsed := time.Since(start)
	report("WriteScratch/Insert", n, insertElapsed)

	start = time.Now()
	for i := 1; i <= n; i++ {
		exec1(db, `UPDATE bench_squealx_slick_scratch SET val1 = ?, val2 = ? WHERE id = ?`,
			rng.Intn(1_000_000), fmt.Sprintf("updated-%d", i), i)
	}
	updateElapsed := time.Since(start)
	report("WriteScratch/Update", n, updateElapsed)

	var count int
	must(db.Get(&count, `SELECT COUNT(*) FROM bench_squealx_slick_scratch`))
	if count != n {
		panic(fmt.Sprintf("expected %d scratch rows, got %d", n, count))
	}
}
