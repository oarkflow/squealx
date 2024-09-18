package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Connect to the MySQL database
func connectMySQL() (*sql.DB, error) {
	// Update with your MySQL credentials
	connStr := "root:root@tcp(127.0.0.1:3306)/cleardb"
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Query to get index information from MySQL
const indexInfoQuery = `
	SELECT
		table_name,
		index_name,
		group_concat(column_name ORDER BY seq_in_index) as columns
	FROM
		information_schema.statistics
	WHERE
		table_schema = 'cleardb'  -- Replace with your database name
	GROUP BY
		table_name, index_name
`

// Fetch index information from MySQL
func fetchIndexes(db *sql.DB) (map[string]map[string][]string, error) {
	rows, err := db.Query(indexInfoQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]map[string][]string)
	for rows.Next() {
		var tableName, indexName, columnsStr string
		if err := rows.Scan(&tableName, &indexName, &columnsStr); err != nil {
			return nil, err
		}

		columns := strings.Split(columnsStr, ",")
		if _, exists := indexes[tableName]; !exists {
			indexes[tableName] = make(map[string][]string)
		}
		indexes[tableName][indexName] = columns
	}

	return indexes, nil
}

// Generate PostgreSQL CREATE INDEX statements
func generatePostgresIndexes(indexes map[string]map[string][]string) []string {
	var statements []string

	for tableName, indexMap := range indexes {
		for indexName, columns := range indexMap {
			columnsStr := strings.Join(columns, ", ")
			statement := fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, tableName, columnsStr)
			statements = append(statements, statement)
		}
	}

	return statements
}

func main() {
	mySQLDB, err := connectMySQL()
	if err != nil {
		log.Fatal(err)
	}
	defer mySQLDB.Close()

	indexes, err := fetchIndexes(mySQLDB)
	if err != nil {
		log.Fatal(err)
	}

	postgresIndexes := generatePostgresIndexes(indexes)
	for _, index := range postgresIndexes {
		fmt.Println(index)
	}
}
