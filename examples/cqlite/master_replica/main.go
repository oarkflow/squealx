package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/cqlite"
)

func main() {
	if err := run(":memory:", ":memory:", "routing-memory"); err != nil {
		log.Fatal(err)
	}
	base := filepath.Join(os.TempDir(), "squealx-cqlite-routing")
	masterPath, replicaPath := base+"-master.db", base+"-replica.db"
	defer os.Remove(masterPath)
	defer os.Remove(replicaPath)
	if err := run(masterPath, replicaPath, "routing-file"); err != nil {
		log.Fatal(err)
	}
}

func run(masterDSN, replicaDSN, id string) error {
	master, err := cqlite.Open(masterDSN, id+"-master")
	if err != nil {
		return err
	}
	defer master.Close()
	replica, err := cqlite.Open(replicaDSN, id+"-replica")
	if err != nil {
		return err
	}
	defer replica.Close()
	if masterDSN == ":memory:" {
		master.SetMaxOpenConns(1)
		replica.SetMaxOpenConns(1)
	}
	for _, db := range []*squealx.DB{master, replica} {
		if _, err = db.Exec(`CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
			return err
		}
	}
	// Seed this demonstration replica. Separate files/memory databases are not
	// automatically synchronized; production needs a replication process.
	if _, err = replica.Exec(`INSERT INTO messages VALUES (1, 'replica seed')`); err != nil {
		return err
	}
	resolver, err := dbresolver.New(dbresolver.WithMasterDBs(master), dbresolver.WithReplicaDBs(replica), dbresolver.WithDefaultDB(master), dbresolver.WithReadWritePolicy(dbresolver.ReadWrite))
	if err != nil {
		return err
	}
	defer resolver.Close()
	if _, err = resolver.Exec(`INSERT INTO messages VALUES (1, 'written to master')`); err != nil {
		return err
	}
	var body string
	if err = resolver.Get(&body, `SELECT body FROM messages WHERE id = ?`, 1); err != nil {
		return err
	}
	fmt.Printf("%s (master=%s, replica=%s), routed read: %s\n", id, masterDSN, replicaDSN, body)
	return nil
}
