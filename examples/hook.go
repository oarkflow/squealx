package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	masterDSN := "host=localhost user=postgres password=postgres dbname=clear sslmode=disable"
	db, err := postgres.Open(masterDSN, "id")
	if err != nil {
		log.Fatal(err)
	}
	db.UseBefore(func(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
		fmt.Println(query, args)
		st, err := getQueryPlan(db.DB(), ctx, query)
		suggestIndexes(st)
		return ctx, err
	})
	callback := func(row map[string]any) error {
		fmt.Println(row)
		return nil
	}
	if err := squealx.SelectEach(db, callback, `SELECT * FROM charge_master WHERE client_internal_code LIKE '%763' LIMIT 10`); err != nil {
		log.Fatal(err)
	}

}

// getQueryPlan runs EXPLAIN ANALYZE on the provided query and returns the query plan
func getQueryPlan(db *sql.DB, ctx context.Context, query string) (string, error) {
	explainQuery := "EXPLAIN ANALYZE " + query
	rows, err := db.QueryContext(ctx, explainQuery)
	if err != nil {
		return "", fmt.Errorf("failed to run EXPLAIN ANALYZE: %w", err)
	}
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", fmt.Errorf("failed to read EXPLAIN output: %w", err)
		}
		plan.WriteString(line)
		plan.WriteRune('\n')
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading rows: %w", err)
	}

	return plan.String(), nil
}

// suggestIndexes parses the query plan and suggests possible indexes
func suggestIndexes(plan string) {
	fmt.Println("Query Plan:")
	fmt.Println(plan)

	// Check for Seq Scan and extract table name and filter conditions
	if strings.Contains(plan, "Seq Scan") {
		fmt.Println("Sequential scan detected. Suggesting index creation...")

		// Use regular expression to extract the table name
		tableRegex := regexp.MustCompile(`Seq Scan on (\w+)`)
		tableMatches := tableRegex.FindStringSubmatch(plan)
		if len(tableMatches) > 1 {
			tableName := tableMatches[1]
			fmt.Printf("Table involved in sequential scan: %s\n", tableName)

			// Extract filter conditions
			filterRegex := regexp.MustCompile(`Filter: \((.*)\)`)
			filterMatches := filterRegex.FindStringSubmatch(plan)
			if len(filterMatches) > 1 {
				filterCondition := filterMatches[1]
				fmt.Printf("Filter condition found: %s\n", filterCondition)

				// Extract column names from the filter condition
				columnRegex := regexp.MustCompile(`\b(\w+)\b`)
				columns := columnRegex.FindAllString(filterCondition, -1)
				uniqueColumns := make(map[string]bool)
				for _, col := range columns {
					if col != "AND" && col != "OR" && col != "NOT" {
						uniqueColumns[col] = true
					}
				}

				// Suggest index creation for the extracted columns
				if len(uniqueColumns) > 0 {
					var columnList []string
					for col := range uniqueColumns {
						columnList = append(columnList, col)
					}
					fmt.Printf("Suggested index: CREATE INDEX idx_%s ON %s (%s);\n", strings.Join(columnList, "_"), tableName, strings.Join(columnList, ", "))
				}
			}
		}
	} else {
		fmt.Println("No sequential scan detected. No immediate index suggestions.")
	}
}
