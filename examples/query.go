package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

func connectDB() (*sql.DB, error) {
	connStr := "user=postgres password=postgres dbname=clear port=5432 sslmode=disable" // Update with your credentials
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getExplainOutput(db *sql.DB, query string) (string, error) {
	explainQuery := fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", query)
	rows, err := db.Query(explainQuery)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var explainOutput strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		explainOutput.WriteString(line)
	}
	return explainOutput.String(), nil
}

type Plan struct {
	Plan ExplainNode `json:"Plan"`
}

type ExplainNode struct {
	NodeType           string        `json:"Node Type"`
	ParentRelationship string        `json:"Parent Relationship"`
	ParallelAware      bool          `json:"Parallel Aware"`
	RelationName       string        `json:"Relation Name"`
	StartupCost        float64       `json:"Startup Cost"`
	AsyncCapable       bool          `json:"Async Capable"`
	TotalCost          float64       `json:"Total Cost"`
	PlanRows           int           `json:"Plan Rows"`
	Senders            int           `json:"Senders"`
	Slice              int           `json:"Slice"`
	Segments           int           `json:"Segments"`
	GangType           int           `json:"Gang Type"`
	Receivers          int           `json:"Receivers"`
	PlanWidth          int           `json:"Plan Width"`
	Alias              string        `json:"Alias"`
	JoinType           string        `json:"Join Type"`
	InnerUnique        bool          `json:"Inner Unique"`
	HashCond           string        `json:"Hash Cond"`
	IndexCond          string        `json:"Index Cond"`
	RecheckCond        string        `json:"Recheck Cond"`
	ScanDirection      string        `json:"Scan Direction"`
	IndexName          string        `json:"Index Name"`
	Filter             string        `json:"Filter"`
	JoinFilter         string        `json:"Join Filter"`
	SortKey            []string      `json:"Sort Key"`
	Plans              []ExplainNode `json:"Plans"`
}

func parseExplainOutput(output string) (indexes []string) {
	var plans []Plan
	err := json.Unmarshal([]byte(output), &plans)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	for _, plan := range plans {
		indexes = append(indexes, analyzeNodes([]ExplainNode{plan.Plan})...)
	}
	return
}

// Extract relevant fields from complex conditions
func extractFieldNames(condition string) []string {
	// First, remove any casts or operators like ::text, ~~, and others
	cleanedCondition := removeCastsAndOperators(condition)

	// Split the cleaned condition by logical operators or delimiters to isolate field names
	parts := strings.FieldsFunc(cleanedCondition, func(r rune) bool {
		return r == '=' || r == '<' || r == '>' || r == '!' || r == '(' || r == ')' || r == ',' || r == ' ' || r == '~'
	})

	// Filter out non-field values (constants, numbers, or empty parts)
	var fieldNames []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Skip empty, constant, or invalid parts
		if part == "" || strings.HasPrefix(part, "'") || strings.ContainsAny(part, "0123456789") {
			continue
		}
		// Only keep valid field names (assuming they don't contain numbers or special chars)
		if isValidFieldName(part) {
			fieldNames = append(fieldNames, part)
		}
	}
	return fieldNames
}

// Remove any casts (e.g., "::text", "::integer") and operators from the condition
func removeCastsAndOperators(cond string) string {
	// Use a loop to remove all occurrences of "::<type>" by finding "::" and removing subsequent type text
	for {
		// Find the index of "::" indicating a cast
		idx := strings.Index(cond, "::")
		if idx == -1 {
			break
		}

		// Find the next space or the end of the string to remove the cast completely
		endIdx := idx + 2
		for endIdx < len(cond) && (cond[endIdx] != ' ' && cond[endIdx] != ')' && cond[endIdx] != ',' && cond[endIdx] != ';') {
			endIdx++
		}

		// Remove the cast part from the condition
		cond = cond[:idx] + cond[endIdx:]
	}

	// Remove other operators like "~" or any additional operators, if needed
	cleaned := strings.ReplaceAll(cond, "~", "")
	return cleaned
}

// Check if a string is a valid field name (basic checks for simplicity)
func isValidFieldName(field string) bool {
	// We assume valid field names are alphanumeric with underscores
	for _, char := range field {
		if !(char == '_' || (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')) {
			return false
		}
	}
	return true
}

// GenerateCreateIndex generates CREATE INDEX statements based on various conditions
func GenerateCreateIndex(node ExplainNode, indexes map[string]bool) []string {
	var createIndexStatements []string

	// Generate index based on IndexCond, Filter, JoinFilter, SortKey
	if node.RelationName != "" {
		// Handle IndexCond
		if node.IndexCond != "" {
			fieldNames := extractFieldNames(node.IndexCond)
			indexName := generateIndexStatement(node.RelationName, fieldNames, "index_cond", indexes)
			if indexName != "" {
				createIndexStatements = append(createIndexStatements, indexName)
			}
		}

		// Handle Filter
		if node.Filter != "" {
			fieldNames := extractFieldNames(node.Filter)
			indexName := generateIndexStatement(node.RelationName, fieldNames, "filter", indexes)
			if indexName != "" {
				createIndexStatements = append(createIndexStatements, indexName)
			}
		}

		// Handle JoinFilter
		if node.JoinFilter != "" {
			fieldNames := extractFieldNames(node.JoinFilter)
			indexName := generateIndexStatement(node.RelationName, fieldNames, "join_filter", indexes)
			if indexName != "" {
				createIndexStatements = append(createIndexStatements, indexName)
			}
		}

		// Handle SortKey
		if len(node.SortKey) > 0 {
			var sortKeyFields []string
			for _, sortKey := range node.SortKey {
				sortKeyFields = append(sortKeyFields, extractFieldNames(sortKey)...)
			}
			indexName := generateIndexStatement(node.RelationName, sortKeyFields, "sort_key", indexes)
			if indexName != "" {
				createIndexStatements = append(createIndexStatements, indexName)
			}
		}
	}

	// Recursively handle subplans
	for _, subPlan := range node.Plans {
		createIndexStatements = append(createIndexStatements, GenerateCreateIndex(subPlan, indexes)...)
	}
	return createIndexStatements
}

// generateIndexStatement generates a CREATE INDEX statement if the index doesn't already exist
func generateIndexStatement(relation string, fieldNames []string, indexType string, indexes map[string]bool) string {
	// Join the field names to create a unique index name and statement
	fieldNamesStr := strings.Join(fieldNames, ", ")
	sanitizedFieldNames := sanitizeFieldNames(fieldNames)
	if _, exists := indexes[sanitizedFieldNames]; !exists {
		indexName := fmt.Sprintf("idx_%s_%s_%s", relation, sanitizedFieldNames, indexType)
		createIndex := fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, relation, fieldNamesStr)
		indexes[sanitizedFieldNames] = true
		return createIndex
	}
	return ""
}

// sanitizeFieldNames converts field names to a valid format for indexing
func sanitizeFieldNames(fieldNames []string) string {
	return strings.Join(fieldNames, "_")
}

func analyzeNodes(nodes []ExplainNode) (indexes []string) {
	uniqueIndexes := make(map[string]bool)
	for _, node := range nodes {
		indexes = append(indexes, GenerateCreateIndex(node, uniqueIndexes)...)
	}
	return
}

func main() {
	db, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	query := `
SELECT
	  pr.provider_id,
	  pr.provider_lov,
	  pr.provider_email,
	  pr.display_name,
	  pr.type_id,
	  pr.work_item_id
	FROM
	  (
	    SELECT DISTINCT
	      ON (provider_id) *
	    FROM
	      vw_provider_wi
	     WHERE provider_lov LIKE 'A%' AND work_item_id=29
	  ) pr
	ORDER BY
	  (pr.first_name <> 'Unknown') DESC,
	  pr.last_name,
	  pr.first_name
`

	explainOutput, err := getExplainOutput(db, query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("EXPLAIN output:")
	fmt.Println(explainOutput)

	for _, index := range parseExplainOutput(explainOutput) {
		fmt.Println(index)
	}
}
