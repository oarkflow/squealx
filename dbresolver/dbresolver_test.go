package dbresolver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/oarkflow/squealx"
)

func resolverDB(id string) *squealx.DB { return &squealx.DB{ID: id} }

func TestNewRequiresPrimary(t *testing.T) {
	if _, err := New(); !errors.Is(err, errNoPrimaryDB) {
		t.Fatalf("New() error = %v, want %v", err, errNoPrimaryDB)
	}
}

func TestResolveDBKeepsDefaultInsideCandidateSet(t *testing.T) {
	master := resolverDB("master")
	replica := resolverDB("replica")
	r, err := New(WithMasterDBs(master), WithReplicaDBs(replica), WithDefaultDB(replica))
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := r.ResolveDB(context.Background(), []string{"master"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved != master {
		t.Fatalf("resolved %q, want master", resolved.ID)
	}
}

func TestResolveDBUnknownCandidates(t *testing.T) {
	r, err := New(WithMasterDBs(resolverDB("master")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ResolveDB(context.Background(), []string{"missing"}); !errors.Is(err, errNoDBToRead) {
		t.Fatalf("ResolveDB error = %v, want %v", err, errNoDBToRead)
	}
}

func TestRoundRobinEmptyAndOrder(t *testing.T) {
	lb := NewRoundRobinLoadBalancer()
	if got := lb.Select(context.Background(), nil); got != "" {
		t.Fatalf("empty Select = %q", got)
	}
	ids := []string{"a", "b"}
	if got := lb.Select(context.Background(), ids); got != "a" {
		t.Fatalf("first Select = %q", got)
	}
	if got := lb.Select(context.Background(), ids); got != "b" {
		t.Fatalf("second Select = %q", got)
	}
}

func TestConcurrentRegisterAndResolve(t *testing.T) {
	raw, err := New(WithMasterDBs(resolverDB("master")))
	if err != nil {
		t.Fatal(err)
	}
	r := raw.(*dbResolver)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.RegisterRead(resolverDB("read"))
		}()
		go func() {
			defer wg.Done()
			_, _ = r.ResolveDB(context.Background(), []string{"master"})
			_ = r.ReadDBs()
		}()
	}
	wg.Wait()
	if got := len(r.ReadDBs()); got != 2 { // master + deduplicated read.
		t.Fatalf("ReadDBs length = %d, want 2", got)
	}
}

func TestConnectionErrorClassification(t *testing.T) {
	if !isDBConnectionError(driver.ErrBadConn) {
		t.Fatal("driver.ErrBadConn must be retryable")
	}
	if !isDBConnectionError(errors.New("write: broken pipe")) {
		t.Fatal("broken pipe must be retryable")
	}
	if isDBConnectionError(context.DeadlineExceeded) {
		t.Fatal("deadline exceeded must not trigger failover")
	}
}

func TestWriteOnlyReadReturnsErrorInsteadOfPanic(t *testing.T) {
	r, err := New(WithMasterDBs(resolverDB("master")), WithReadWritePolicy(WriteOnly))
	if err != nil {
		t.Fatal(err)
	}
	var value int
	if err := r.Get(&value, "SELECT 1"); !errors.Is(err, errNoDBToRead) {
		t.Fatalf("Get error = %v", err)
	}
	if err := r.QueryRow("SELECT 1").Scan(&value); !errors.Is(err, errNoDBToRead) {
		t.Fatalf("QueryRow Scan error = %v", err)
	}
}
