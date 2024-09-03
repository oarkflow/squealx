package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	
	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver for PostgreSQL
	
	"github.com/mackerelio/go-osstat/memory"
	
	"github.com/oarkflow/squealx/drivers/postgres"
)

func main() {
	// Database connection details (replace with yours)
	dbUser := "postgres"
	dbPassword := "postgres"
	dbName := "clear"
	dbHost := "localhost"
	dbPort := "5432"
	
	// Define a large query (replace with your actual query)
	query := "SELECT charge_master_id, work_item_id, cpt_hcpcs_code,client_proc_desc FROM charge_master"
	
	// Benchmark with pgx
	fmt.Println("Benchmarking with pgx:")
	pgxMemoryUsage(query, dbUser, dbPassword, dbName, dbHost, dbPort)
	fmt.Println("Benchmarking with squealx:")
	squealxMemoryUsage(query, dbUser, dbPassword, dbName, dbHost, dbPort)
}

func pgxMemoryUsage(query string, user, password, dbname, host, port string) {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbname)
	db, err := sql.Open("pgx", connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	
	ctx := context.Background()
	
	// Measure memory usage before query execution
	memBefore := getMemory()
	fmt.Printf("Memory usage before query (pgx): %f MB\n", memBefore)
	
	// Execute the query with streaming
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		panic(err)
	}
	row := 0
	defer rows.Close()
	// Stream data (no processing here to minimize impact)
	for rows.Next() {
		var id, workItemId int
		var code, desc string
		err := rows.Scan(&id, &workItemId, &code, &desc) // Scan into empty interface slice
		if err != nil {
			panic(err)
		}
		row++
	}
	fmt.Println(row)
	// Measure memory usage after query execution
	memAfter := getMemory()
	fmt.Printf("Memory usage after query (pqx): %f MB\n", memAfter)
	fmt.Printf("Memory difference (pqx): %f MB\n\n", memAfter-memBefore)
}

func squealxMemoryUsage(query string, user, password, dbname, host, port string) {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbname)
	db, err := postgres.Open(connString, "test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	
	ctx := context.Background()
	
	// Measure memory usage before query execution
	memBefore := getMemory()
	fmt.Printf("Memory usage before query (squealx): %f MB\n", memBefore)
	
	// Execute the query with streaming
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	row := 0
	// Stream data (no processing here to minimize impact)
	for rows.Next() {
		var id, workItemId int
		var code, desc string
		err := rows.Scan(&id, &workItemId, &code, &desc) // Scan into empty interface slice
		if err != nil {
			panic(err)
		}
		row++
	}
	fmt.Println(row)
	// Measure memory usage after query execution
	memAfter := getMemory()
	fmt.Printf("Memory usage after query (squealx): %f MB\n", memAfter)
	fmt.Printf("Memory difference (squealx): %f MB\n\n", memAfter-memBefore)
}

func getMemory() float32 {
	memory, err := memory.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 0
	}
	return float32(memory.Used) / (1 << 20)
	// fmt.Printf("memory total: %d bytes\n", memory.Total)
	// fmt.Printf("memory used: %v MB\n", float32(memory.Used)/(1<<20))
	// fmt.Printf("memory cached: %d bytes\n", memory.Cached)
	// fmt.Printf("memory free: %d bytes\n", memory.Free)
}
