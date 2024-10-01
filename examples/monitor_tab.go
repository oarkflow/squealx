package main

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/monitor"
	"os"
	"strings"
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

	var choice string
	for {
		fmt.Println("\nAvailable tables (tabs):")
		for tableName := range data {
			fmt.Println("- " + tableName)
		}

		fmt.Print("Enter the table name to view (or type 'exit' to quit): ")
		fmt.Scanln(&choice)

		choice = strings.TrimSpace(choice)

		if choice == "exit" {
			break
		}

		if rows, exists := data[choice]; exists {
			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.SetStyle(table.StyleColoredDark)

			var headers []any
			if len(rows) > 0 {
				for key := range rows[0] {
					headers = append(headers, key)
				}
				t.AppendHeader(headers)
			}

			for _, row := range rows {
				var rowData []any
				for _, header := range headers {
					rowData = append(rowData, row[header.(string)])
				}
				t.AppendRow(rowData)
			}

			fmt.Printf("\nTable: %s\n", choice)
			fmt.Println(t.Render())
		} else {
			fmt.Printf("Table '%s' does not exist.\n", choice)
		}
	}
}
