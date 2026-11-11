package squealx

import (
	"errors"
	"testing"
)

func TestCursorAllYieldsCloseErrorAfterRows(t *testing.T) {
	closeErr := errors.New("rows close failed")
	rows := &cursorCloseRows{rowCount: 1, closeErr: closeErr}
	cursor, err := CursorFromRowsConfig[int](&Rows{SQLRows: rows}, CursorConfig{KeepOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	var values []int
	var yieldedErr error
	for value, err := range cursor.All() {
		if err != nil {
			yieldedErr = err
			continue
		}
		values = append(values, value)
	}
	if len(values) != 1 || values[0] != 1 {
		t.Fatalf("iterated values = %v, want [1]", values)
	}
	if !errors.Is(yieldedErr, closeErr) {
		t.Fatalf("iterator error = %v, want close error", yieldedErr)
	}
	if rows.closed != 1 {
		t.Fatalf("rows closed %d times, want 1", rows.closed)
	}
}
