package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/postgres"
	"log"
	"regexp"
	"strings"
	"time"
)

// DBLogger handles logging and suggesting indexes for queries.
type DBLogger struct {
	db *squealx.DB
}

// NewDBLogger initializes a new DBLogger.
func NewDBLogger(db *squealx.DB) *DBLogger {
	return &DBLogger{db: db}
}

// logAndSuggestIndexes logs the query and suggests indexes based on the EXPLAIN output.
func (l *DBLogger) logAndSuggestIndexes(ctx context.Context, query string, args ...interface{}) {
	// Start time to measure query execution time
	start := time.Now()

	// Log the query
	fmt.Printf("Executing query: %s\n", query)
	fmt.Printf("With arguments: %v\n", args)

	// Use a transaction to run EXPLAIN without affecting the actual query
	tx, err := l.db.Beginx()
	if err != nil {
		log.Printf("Failed to start transaction: %v\n", err)
		return
	}
	defer tx.Rollback()

	explainQuery := fmt.Sprintf("EXPLAIN %s", query)
	rows, err := tx.QueryxContext(ctx, explainQuery, args...)
	if err != nil {
		log.Printf("Failed to run EXPLAIN: %v\n", err)
		return
	}
	defer rows.Close()

	var explanation []string
	for rows.Next() {
		var explainOutput string
		err = rows.Scan(&explainOutput)
		if err != nil {
			log.Printf("Failed to scan EXPLAIN output: %v\n", err)
			return
		}
		explanation = append(explanation, explainOutput)
	}

	// Log the EXPLAIN output
	fmt.Printf("EXPLAIN output:\n%s\n", strings.Join(explanation, "\n"))

	// Analyze the EXPLAIN output for optimization suggestions
	l.analyzeExplainOutput(explanation)

	fmt.Printf("Query execution took %s\n", time.Since(start))
}

// analyzeExplainOutput processes the EXPLAIN output to suggest specific indexes.
func (l *DBLogger) analyzeExplainOutput(explanation []string) {
	// Regular expression to match sequential scans and extract table and column info
	seqScanPattern := regexp.MustCompile(`Seq Scan on (\w+) .*?Filter: \((.*?)\)`)

	for _, line := range explanation {
		if matches := seqScanPattern.FindStringSubmatch(line); matches != nil {
			table := matches[1]
			filter := matches[2]

			// Extract column names from the filter condition
			columnPattern := regexp.MustCompile(`\b(\w+)\b`)
			columnMatches := columnPattern.FindAllStringSubmatch(filter, -1)

			columns := make([]string, 0)
			for _, colMatch := range columnMatches {
				columns = append(columns, colMatch[1])
			}

			// Remove duplicates
			columnSet := make(map[string]struct{})
			for _, col := range columns {
				columnSet[col] = struct{}{}
			}
			fmt.Println("**************", columnSet)
			// Suggest index based on the table and columns
			if len(columnSet) > 0 {
				columnList := strings.Join(getKeys(columnSet), ", ")
				indexSuggestion := fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s(%s);", table, strings.Join(getKeys(columnSet), "_"), table, columnList)
				fmt.Printf("Suggestion: Consider adding the following index to optimize the query: %s\n", indexSuggestion)
			}
		}
	}
}

// getKeys returns a slice of keys from the given map.
func getKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// QueryHook is a middleware function that intercepts squealx.Exec, squealx.Get, squealx.Select, etc.
func (l *DBLogger) QueryHook(ctx context.Context, query string, args ...interface{}) {
	// Call the logging and index suggestion function
	l.logAndSuggestIndexes(ctx, query, args...)
}

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	// Create a new DBLogger
	logger := NewDBLogger(db)

	// Example query to be executed
	ctx := context.Background()
	query := `
SELECT DISTINCT v.encounter_id, v.DOS, v.DOB, v.patient_name, v.patient_fin, v.patient_mrn,
                         v.Age, v.facility_name, v.work_item_description, v.encounter_type,
                         e.load_time, d.encounter_status, class,e.encounter_disch_disp, short_label, label,
                         21281 IN (d.code_assigned, d.qa_assigned, d.dataentry_assigned) as assigned,
                         d.work_item_id,
                         d.qa_flag AND NOT d.qa_complete as qa,
                         d.qa_requested,
                         d.code_assigned,
                         v.is_imbargo
         FROM encounter_details d
                  JOIN vw_queue_base v USING (work_item_id,encounter_id)
                  JOIN encounters e ON e.encounter_id = d.encounter_id
                  JOIN encounter_status ON encounter_status.encounter_status = d.encounter_status
                  LEFT JOIN suspend_events es ON es.work_item_id = d.work_item_id AND es.encounter_id = d.encounter_id
                  LEFT JOIN event_em_pro ep  ON ep.work_item_id = d.work_item_id AND ep.encounter_id = d.encounter_id
                  LEFT JOIN event_em_fac ef  ON ef.work_item_id = d.work_item_id AND ef.encounter_id = d.encounter_id
                  LEFT JOIN event_cpt_pro cp ON cp.work_item_id = d.work_item_id AND cp.encounter_id = d.encounter_id
                  LEFT JOIN event_cpt_fac cf ON cf.work_item_id = d.work_item_id AND cf.encounter_id = d.encounter_id
                  LEFT JOIN event_dx_pro dp  ON dp.work_item_id = d.work_item_id AND dp.encounter_id = d.encounter_id
                  LEFT JOIN event_dx_fac df  ON df.work_item_id = d.work_item_id AND df.encounter_id = d.encounter_id
                  LEFT JOIN event_icd10_dx_pro d10p ON d10p.work_item_id = d.work_item_id AND d10p.encounter_id = d.encounter_id
                  LEFT JOIN event_icd10_dx_fac d10f ON d10f.work_item_id = d.work_item_id AND d10f.encounter_id = d.encounter_id
                  LEFT JOIN attending_providers ap ON ap.encounter_id = e.encounter_id
                  LEFT JOIN providers pr ON pr.provider_id = ap.provider_id
                  LEFT JOIN encounter_scribes ls ON ls.encounter_id = e.encounter_id
         WHERE v.code_assigned = 21281
`

	// Call the hook manually before executing a query (or integrate with a global hook)
	logger.QueryHook(ctx, query)

	// Now, execute the actual query (using squealx)
	var results []map[string]any
	err = db.Select(&results, query)
	if err != nil {
		log.Printf("Failed to execute query: %v\n", err)
	}
}
