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
	connStr := "user=postgres password=postgres dbname=clear_dev port=5432 sslmode=disable" // Update with your credentials
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

func parseExplainOutput(output string) {
	var plans []Plan
	err := json.Unmarshal([]byte(output), &plans)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}
	for _, plan := range plans {
		analyzeNodes([]ExplainNode{plan.Plan})
	}
}

func analyzeNodes(nodes []ExplainNode) {
	for _, node := range nodes {
		if node.NodeType == "Seq Scan" {
			fmt.Printf("Consider adding an index. Found Seq Scan on relation: %s\n", node.RelationName)
		}
		if len(node.Plans) > 0 {
			analyzeNodes(node.Plans)
		}
	}
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

	parseExplainOutput(explainOutput)
}

/*
[
  {
    "Plan": {
      "Node Type": "Sort",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 279.28,
      "Total Cost": 279.28,
      "Plan Rows": 1,
      "Plan Width": 283,
      "Sort Key": ["(((pr.first_name)::text <> 'Unknown'::text)) DESC", "pr.last_name", "pr.first_name"],
      "Plans": [
        {
          "Node Type": "Subquery Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Alias": "pr",
          "Startup Cost": 279.25,
          "Total Cost": 279.27,
          "Plan Rows": 1,
          "Plan Width": 283,
          "Plans": [
            {
              "Node Type": "Unique",
              "Parent Relationship": "Subquery",
              "Parallel Aware": false,
              "Async Capable": false,
              "Startup Cost": 279.25,
              "Total Cost": 279.26,
              "Plan Rows": 1,
              "Plan Width": 1044,
              "Plans": [
                {
                  "Node Type": "Sort",
                  "Parent Relationship": "Outer",
                  "Parallel Aware": false,
                  "Async Capable": false,
                  "Startup Cost": 279.25,
                  "Total Cost": 279.25,
                  "Plan Rows": 1,
                  "Plan Width": 1044,
                  "Sort Key": ["vw_provider_wi.provider_id"],
                  "Plans": [
                    {
                      "Node Type": "Subquery Scan",
                      "Parent Relationship": "Outer",
                      "Parallel Aware": false,
                      "Async Capable": false,
                      "Alias": "vw_provider_wi",
                      "Startup Cost": 279.18,
                      "Total Cost": 279.24,
                      "Plan Rows": 1,
                      "Plan Width": 1044,
                      "Plans": [
                        {
                          "Node Type": "Unique",
                          "Parent Relationship": "Subquery",
                          "Parallel Aware": false,
                          "Async Capable": false,
                          "Startup Cost": 279.18,
                          "Total Cost": 279.23,
                          "Plan Rows": 1,
                          "Plan Width": 630,
                          "Plans": [
                            {
                              "Node Type": "Sort",
                              "Parent Relationship": "Outer",
                              "Parallel Aware": false,
                              "Async Capable": false,
                              "Startup Cost": 279.18,
                              "Total Cost": 279.19,
                              "Plan Rows": 1,
                              "Plan Width": 630,
                              "Sort Key": ["providers.provider_lov", "providers.display_name", "providers.provider_id", "providers.provider_email", "providers.first_name", "providers.last_name", "providers.middle_name", "providers.title", "providers.npi", "facilities.facility_name", "work_item_types.work_item_description", "provider_types.provider_type_id", "provider_types.category", "providers.client_ref", "provider_wi.client_ref", "provider_wi.alt_client_ref"],
                              "Plans": [
                                {
                                  "Node Type": "Nested Loop",
                                  "Parent Relationship": "Outer",
                                  "Parallel Aware": false,
                                  "Async Capable": false,
                                  "Join Type": "Inner",
                                  "Startup Cost": 96.69,
                                  "Total Cost": 279.17,
                                  "Plan Rows": 1,
                                  "Plan Width": 630,
                                  "Inner Unique": true,
                                  "Join Filter": "(work_items.work_item_type_id = work_item_types.work_item_type_id)",
                                  "Plans": [
                                    {
                                      "Node Type": "Nested Loop",
                                      "Parent Relationship": "Outer",
                                      "Parallel Aware": false,
                                      "Async Capable": false,
                                      "Join Type": "Inner",
                                      "Startup Cost": 96.69,
                                      "Total Cost": 277.97,
                                      "Plan Rows": 1,
                                      "Plan Width": 470,
                                      "Inner Unique": true,
                                      "Join Filter": "(work_items.facility_id = facilities.facility_id)",
                                      "Plans": [
                                        {
                                          "Node Type": "Nested Loop",
                                          "Parent Relationship": "Outer",
                                          "Parallel Aware": false,
                                          "Async Capable": false,
                                          "Join Type": "Inner",
                                          "Startup Cost": 96.69,
                                          "Total Cost": 269.87,
                                          "Plan Rows": 1,
                                          "Plan Width": 459,
                                          "Inner Unique": true,
                                          "Plans": [
                                            {
                                              "Node Type": "Nested Loop",
                                              "Parent Relationship": "Outer",
                                              "Parallel Aware": false,
                                              "Async Capable": false,
                                              "Join Type": "Inner",
                                              "Startup Cost": 96.56,
                                              "Total Cost": 269.15,
                                              "Plan Rows": 1,
                                              "Plan Width": 377,
                                              "Inner Unique": false,
                                              "Plans": [
                                                {
                                                  "Node Type": "Hash Join",
                                                  "Parent Relationship": "Outer",
                                                  "Parallel Aware": false,
                                                  "Async Capable": false,
                                                  "Join Type": "Inner",
                                                  "Startup Cost": 96.56,
                                                  "Total Cost": 265.44,
                                                  "Plan Rows": 1,
                                                  "Plan Width": 361,
                                                  "Inner Unique": false,
                                                  "Hash Cond": "(providers.provider_id = provider_wi.provider_id)",
                                                  "Plans": [
                                                    {
                                                      "Node Type": "Seq Scan",
                                                      "Parent Relationship": "Outer",
                                                      "Parallel Aware": false,
                                                      "Async Capable": false,
                                                      "Relation Name": "providers",
                                                      "Alias": "providers",
                                                      "Startup Cost": 0.00,
                                                      "Total Cost": 166.73,
                                                      "Plan Rows": 26,
                                                      "Plan Width": 289,
                                                      "Filter": "((provider_lov)::text ~~ 'A%'::text)"
                                                    },
                                                    {
                                                      "Node Type": "Hash",
                                                      "Parent Relationship": "Inner",
                                                      "Parallel Aware": false,
                                                      "Async Capable": false,
                                                      "Startup Cost": 95.76,
                                                      "Total Cost": 95.76,
                                                      "Plan Rows": 64,
                                                      "Plan Width": 80,
                                                      "Plans": [
                                                        {
                                                          "Node Type": "Bitmap Heap Scan",
                                                          "Parent Relationship": "Outer",
                                                          "Parallel Aware": false,
                                                          "Async Capable": false,
                                                          "Relation Name": "provider_wi",
                                                          "Alias": "provider_wi",
                                                          "Startup Cost": 4.78,
                                                          "Total Cost": 95.76,
                                                          "Plan Rows": 64,
                                                          "Plan Width": 80,
                                                          "Recheck Cond": "(work_item_id = 29)",
                                                          "Plans": [
                                                            {
                                                              "Node Type": "Bitmap Index Scan",
                                                              "Parent Relationship": "Outer",
                                                              "Parallel Aware": false,
                                                              "Async Capable": false,
                                                              "Index Name": "idx_provider_wi_work_item_id_client_ref",
                                                              "Startup Cost": 0.00,
                                                              "Total Cost": 4.77,
                                                              "Plan Rows": 64,
                                                              "Plan Width": 0,
                                                              "Index Cond": "(work_item_id = 29)"
                                                            }
                                                          ]
                                                        }
                                                      ]
                                                    }
                                                  ]
                                                },
                                                {
                                                  "Node Type": "Seq Scan",
                                                  "Parent Relationship": "Inner",
                                                  "Parallel Aware": false,
                                                  "Async Capable": false,
                                                  "Relation Name": "work_items",
                                                  "Alias": "work_items",
                                                  "Startup Cost": 0.00,
                                                  "Total Cost": 3.70,
                                                  "Plan Rows": 1,
                                                  "Plan Width": 24,
                                                  "Filter": "(work_item_id = 29)"
                                                }
                                              ]
                                            },
                                            {
                                              "Node Type": "Index Scan",
                                              "Parent Relationship": "Inner",
                                              "Parallel Aware": false,
                                              "Async Capable": false,
                                              "Scan Direction": "Forward",
                                              "Index Name": "provider_types_pkey",
                                              "Relation Name": "provider_types",
                                              "Alias": "provider_types",
                                              "Startup Cost": 0.13,
                                              "Total Cost": 0.61,
                                              "Plan Rows": 1,
                                              "Plan Width": 90,
                                              "Index Cond": "(provider_type_id = providers.provider_type_id)"
                                            }
                                          ]
                                        },
                                        {
                                          "Node Type": "Seq Scan",
                                          "Parent Relationship": "Inner",
                                          "Parallel Aware": false,
                                          "Async Capable": false,
                                          "Relation Name": "facilities",
                                          "Alias": "facilities",
                                          "Startup Cost": 0.00,
                                          "Total Cost": 6.38,
                                          "Plan Rows": 138,
                                          "Plan Width": 27
                                        }
                                      ]
                                    },
                                    {
                                      "Node Type": "Seq Scan",
                                      "Parent Relationship": "Inner",
                                      "Parallel Aware": false,
                                      "Async Capable": false,
                                      "Relation Name": "work_item_types",
                                      "Alias": "work_item_types",
                                      "Startup Cost": 0.00,
                                      "Total Cost": 1.09,
                                      "Plan Rows": 9,
                                      "Plan Width": 176
                                    }
                                  ]
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  }
]

*/
