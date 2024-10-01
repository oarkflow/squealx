package main

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/monitor"
	"os"
)

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "test")
	if err != nil {
		panic(err)
	}

	data, err := monitor.GetPostgresStats(db)
	if err != nil {
		panic(err)
	}

	// Accordion-like functionality
	var choice string
	for {
		fmt.Println("\nAvailable tables:")
		for tableName := range data {
			fmt.Println("- " + tableName)
		}

		fmt.Print("Enter the table name to expand (or type 'exit' to quit): ")
		fmt.Scanln(&choice)

		if choice == "exit" {
			break
		}

		if rows, exists := data[choice]; exists {
			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout) // Output to stdout
			t.SetStyle(table.StyleColoredDark)

			// Extract headers
			var headers []any
			if len(rows) > 0 {
				for key := range rows[0] {
					headers = append(headers, key)
				}
				t.AppendHeader(headers) // Append the extracted headers
			}

			// Populate the table with data
			for _, row := range rows {
				var rowData []any
				for _, header := range headers {
					rowData = append(rowData, row[header.(string)]) // Append values based on headers
				}
				t.AppendRow(rowData)
			}

			// Display the table
			fmt.Printf("\nTable: %s\n", choice)
			fmt.Println(t.Render())
		} else {
			fmt.Printf("Table '%s' does not exist.\n", choice)
		}
	}
}
