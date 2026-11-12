// Command engine_native shows cqlite's engine-specific Go APIs and storage
// controls. These APIs are outside SQLite's SQL language and are not portable
// across SQLite implementations.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	cqlite "github.com/oarkflow/cqlite"
)

func main() {
	if err := run(":memory:", "native-memory"); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(os.TempDir(), "squealx-cqlite-native.db")
	_ = os.Remove(path)
	defer os.Remove(path)
	if err := run(path, "native-file"); err != nil {
		log.Fatal(err)
	}
}

func run(path, label string) error {
	db, err := cqlite.Open(path, cqlite.Options{
		BufferPoolSize:          32,
		SyncPolicy:              cqlite.SyncNormal,
		CheckpointIntervalTx:    2,
		CheckpointIntervalBytes: 1 << 20,
	})
	if err != nil {
		return err
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE samples (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		_ = db.Close()
		return err
	}
	for _, value := range []string{"first", "second"} {
		if _, err := db.Exec(ctx, `INSERT INTO samples(value) VALUES (?)`, value); err != nil {
			_ = db.Close()
			return err
		}
	}

	// Stats and Checkpoint are cqlite Go APIs; SQLite has related concepts but
	// these concrete methods and option fields belong to cqlite's engine.
	stats := db.Stats()
	if err := db.Checkpoint(ctx); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	fmt.Printf("%s: tables=%d transactions=%d opened=%s\n", label, stats.Tables, stats.Transactions, stats.OpenedAt.Format("15:04:05"))
	if path != ":memory:" {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		fmt.Printf("%s: packed database file size=%d bytes; sync=%s; checkpoint interval=2 transactions\n", label, info.Size(), cqlite.SyncNormal)
	}
	return nil
}
