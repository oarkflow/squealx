package jsonbq

import (
	"context"
	"testing"
)

func TestInsertQueryRowContextBuildErrorDoesNotPanic(t *testing.T) {
	iq := &InsertQuery{
		table: "patients",
		data:  make(chan int), // json marshal error: unsupported type
	}

	row := iq.QueryRowContext(context.Background())
	if row == nil {
		t.Fatalf("expected non-nil row")
	}
	if row.Err() == nil {
		t.Fatalf("expected build error in row.Err()")
	}
	if err := row.Scan(); err == nil {
		t.Fatalf("expected scan to return build error")
	}
}
