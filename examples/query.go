package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
)

func connectDB() (*squealx.DB, error) {
	connStr := "user=postgres password=postgres host=localhost dbname=clear_dev port=5432 sslmode=disable"
	db, err := postgres.Open(connStr, "postgres")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getExplainOutput(db *squealx.DB, query string) (string, error) {
	explainQuery := fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", query)
	rows, err := db.Query(explainQuery)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var output string
	if rows.Next() {
		if err := rows.Scan(&output); err != nil {
			return "", err
		}
	}
	return output, nil
}

type Plan struct {
	Plan ExplainNode `json:"Plan"`
}

type ExplainNode struct {
	NodeType           string        `json:"Node Type"`
	ParentRelationship string        `json:"Parent Relationship,omitempty"`
	ParallelAware      bool          `json:"Parallel Aware,omitempty"`
	RelationName       string        `json:"Relation Name,omitempty"`
	Alias              string        `json:"Alias,omitempty"`
	StartupCost        float64       `json:"Startup Cost,omitempty"`
	TotalCost          float64       `json:"Total Cost,omitempty"`
	PlanRows           int           `json:"Plan Rows,omitempty"`
	PlanWidth          int           `json:"Plan Width,omitempty"`
	AsyncCapable       bool          `json:"Async Capable,omitempty"`
	Segments           int           `json:"Segments,omitempty"`
	GangType           int           `json:"Gang Type,omitempty"`
	Senders            int           `json:"Senders,omitempty"`
	Slice              int           `json:"Slice,omitempty"`
	Receivers          int           `json:"Receivers,omitempty"`
	JoinType           string        `json:"Join Type,omitempty"`
	InnerUnique        bool          `json:"Inner Unique,omitempty"`
	HashCond           string        `json:"Hash Cond,omitempty"`
	IndexCond          string        `json:"Index Cond,omitempty"`
	RecheckCond        string        `json:"Recheck Cond,omitempty"`
	ScanDirection      string        `json:"Scan Direction,omitempty"`
	IndexName          string        `json:"Index Name,omitempty"`
	Filter             string        `json:"Filter,omitempty"`
	JoinFilter         string        `json:"Join Filter,omitempty"`
	SortKey            []string      `json:"Sort Key,omitempty"`
	Plans              []ExplainNode `json:"Plans,omitempty"`
}

func parseExplainOutput(output string) (indexes []string) {
	var plans []Plan
	err := json.Unmarshal([]byte(output), &plans)
	if err != nil {
		log.Printf("Error parsing JSON: %v", err)
		return nil
	}
	for _, plan := range plans {
		indexes = append(indexes, analyzeNodes([]ExplainNode{plan.Plan})...)
	}
	return
}

func extractFieldNames(condition string) []string {
	parts := strings.FieldsFunc(removeCastsAndOperators(condition), func(r rune) bool {
		return r == '=' || r == '<' || r == '>' || r == '!' || r == '(' || r == ')' || r == ',' || r == ' ' || r == '~'
	})
	var fieldNames []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" && !strings.HasPrefix(part, "'") && !strings.ContainsAny(part, "0123456789") && isValidFieldName(part) {
			fieldNames = append(fieldNames, part)
		}
	}
	return fieldNames
}

func removeCastsAndOperators(cond string) string {
	for {
		idx := strings.Index(cond, "::")
		if idx == -1 {
			break
		}
		endIdx := idx + 2
		for endIdx < len(cond) && (cond[endIdx] != ' ' && cond[endIdx] != ')' && cond[endIdx] != ',' && cond[endIdx] != ';') {
			endIdx++
		}
		cond = cond[:idx] + cond[endIdx:]
	}
	return strings.ReplaceAll(cond, "~", "")
}

func isValidFieldName(field string) bool {
	for _, char := range field {
		if !(char == '_' || (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')) {
			return false
		}
	}
	return true
}

type Filter struct {
	Condition string
	Type      string
}

func GenerateCreateIndex(node ExplainNode, indexes map[string]bool) []string {
	var createIndexStatements []string
	if node.RelationName != "" {
		fields := []Filter{
			{node.IndexCond, "index_cond"},
			{node.Filter, "filter"},
			{node.JoinFilter, "join_filter"},
		}
		for _, f := range fields {
			if f.Condition != "" {
				fieldNames := extractFieldNames(f.Condition)
				if index := generateIndexStatement(node.RelationName, fieldNames, f.Type, indexes); index != "" {
					createIndexStatements = append(createIndexStatements, index)
				}
			}
		}
		if len(node.SortKey) > 0 {
			sortFields := extractFieldNames(strings.Join(node.SortKey, ", "))
			if index := generateIndexStatement(node.RelationName, sortFields, "sort_key", indexes); index != "" {
				createIndexStatements = append(createIndexStatements, index)
			}
		}
	}
	for _, subPlan := range node.Plans {
		createIndexStatements = append(createIndexStatements, GenerateCreateIndex(subPlan, indexes)...)
	}
	return createIndexStatements
}

func generateIndexStatement(relation string, fieldNames []string, indexType string, indexes map[string]bool) string {
	fieldNamesStr := sanitizeFieldNames(fieldNames)
	if _, exists := indexes[fieldNamesStr]; !exists {
		indexName := fmt.Sprintf("idx_%s_%s_%s", relation, fieldNamesStr, indexType)
		createIndex := fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, relation, strings.Join(fieldNames, ", "))
		indexes[fieldNamesStr] = true
		return createIndex
	}
	return ""
}

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
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()

	query, err := os.ReadFile("get-procedures.sql")
	if err != nil {
		panic(err)
	}
	var dst []map[string]any
	err = db.Select(&dst, string(query), map[string]any{
		"alternate_client": false,
		"mips_as_changes":  false,
		"split_data_entry": false,
		"live_code_split":  false,
		"work_item_id":     228,
		"encounter_id":     221632078,
		"user_id":          23171,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(dst)
	/* fmt.Println(qry)
	explainOutput, err := getExplainOutput(db, qry)
	if err != nil {
		log.Fatalf("Error executing EXPLAIN: %v", err)
	}

	fmt.Println("EXPLAIN output:")
	fmt.Println(explainOutput)

	for _, index := range parseExplainOutput(explainOutput) {
		fmt.Println(index)
	} */
}
