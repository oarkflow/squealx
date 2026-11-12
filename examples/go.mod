module examples

go 1.26.0

require (
	github.com/go-sql-driver/mysql v1.10.1
	github.com/jackc/pgx/v5 v5.12.0
	github.com/jedib0t/go-pretty/v6 v6.8.3
	github.com/oarkflow/squealx v0.0.82
	github.com/oarkflow/squealx/drivers/cqlite v0.0.82
	github.com/oarkflow/squealx/drivers/mysql v0.0.82
	github.com/oarkflow/squealx/drivers/postgres v0.0.82
	github.com/oarkflow/squealx/drivers/sqlite v0.0.82
	github.com/oarkflow/squealx/jsonbq v0.0.81
	github.com/oarkflow/cqlite v1.0.0
	modernc.org/sqlite v1.62.0
)

// Keep the in-repository examples compiling against the APIs in this checkout.
replace github.com/oarkflow/squealx => ..

replace github.com/oarkflow/squealx/drivers/mysql => ../drivers/mysql

replace github.com/oarkflow/squealx/drivers/postgres => ../drivers/postgres

replace github.com/oarkflow/squealx/drivers/sqlite => ../drivers/sqlite

replace github.com/oarkflow/squealx/drivers/cqlite => ../drivers/cqlite

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.31 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/oarkflow/date v0.0.5 // indirect
	github.com/oarkflow/jet v0.0.4 // indirect
	github.com/oarkflow/zlog v0.0.4 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	modernc.org/libc v1.81.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)
