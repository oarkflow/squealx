module github.com/oarkflow/squealx/hooks

go 1.25.7

require (
	github.com/oarkflow/log v1.0.84
	github.com/oarkflow/squealx v0.0.0
	github.com/oarkflow/squealx/drivers/mssql v0.0.0
	github.com/oarkflow/squealx/drivers/mysql v0.0.0
	github.com/oarkflow/squealx/drivers/postgres v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.0 // indirect
	github.com/goccy/go-reflect v1.2.0 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/microsoft/go-mssqldb v1.10.0 // indirect
	github.com/oarkflow/date v0.0.4 // indirect
	github.com/oarkflow/expr v0.0.11 // indirect
	github.com/oarkflow/jet v0.0.4 // indirect
	github.com/oarkflow/json v0.0.28 // indirect
	github.com/oarkflow/xid v1.2.5 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.36.0 // indirect
)

replace github.com/oarkflow/squealx => ..

replace github.com/oarkflow/squealx/drivers/mssql => ../drivers/mssql

replace github.com/oarkflow/squealx/drivers/mysql => ../drivers/mysql

replace github.com/oarkflow/squealx/drivers/postgres => ../drivers/postgres
