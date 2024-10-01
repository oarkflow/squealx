package main

import (
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/monitor"
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
	for tableName, rows := range data {
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout) // Output to stdout
		t.SetStyle(table.StyleColoredDark)
		var headers []any
		if len(rows) > 0 {
			for key := range rows[0] {
				headers = append(headers, key)
			}
			t.AppendHeader(headers) // Append the extracted headers
		}
		for _, row := range rows {
			var rowData []any
			for _, header := range headers {
				rowData = append(rowData, row[header.(string)]) // Append values based on headers
			}
			t.AppendRow(rowData)
		}
		fmt.Printf("\nTable: %s\n", tableName)
		fmt.Println(t.Render())
	}
}
