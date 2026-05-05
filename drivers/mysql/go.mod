module github.com/oarkflow/squealx/drivers/mysql

go 1.25.7

require (
	github.com/go-sql-driver/mysql v1.10.0
	github.com/oarkflow/squealx v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/goccy/go-reflect v1.2.0 // indirect
	github.com/oarkflow/date v0.0.4 // indirect
	github.com/oarkflow/expr v0.0.11 // indirect
	github.com/oarkflow/jet v0.0.4 // indirect
	github.com/oarkflow/json v0.0.28 // indirect
)

replace github.com/oarkflow/squealx => ../..
