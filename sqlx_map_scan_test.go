package squealx

import (
	"context"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func newMapScanTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open("sqlite", ":memory:", "map-scan-test")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec(`CREATE TABLE forms (id INTEGER, name TEXT, active INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO forms (id, name, active) VALUES (1, 'alpha', 1)`); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	return db
}

func TestSelectIntoSliceOfMapAny(t *testing.T) {
	db := newMapScanTestDB(t)

	var got []map[string]any
	if err := db.Select(&got, `SELECT id, name, active FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("select map slice: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["name"] != "alpha" {
		t.Fatalf("expected name alpha, got %#v", got[0]["name"])
	}
}

func TestSelectIntoSliceOfMapString(t *testing.T) {
	db := newMapScanTestDB(t)

	var got []map[string]string
	if err := db.Select(&got, `SELECT CAST(id AS TEXT) AS id, name, CAST(active AS TEXT) AS active FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("select typed map slice: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["id"] != "1" || got[0]["active"] != "1" {
		t.Fatalf("unexpected typed map contents: %#v", got[0])
	}
}

func TestGetIntoTypedMap(t *testing.T) {
	db := newMapScanTestDB(t)

	got := map[string]string{}
	if err := db.Get(&got, `SELECT CAST(id AS TEXT) AS id, name FROM forms LIMIT 1`); err != nil {
		t.Fatalf("get typed map: %v", err)
	}
	if got["id"] != "1" || got["name"] != "alpha" {
		t.Fatalf("unexpected map values: %#v", got)
	}
}

func TestGetContextIntoTypedMap(t *testing.T) {
	db := newMapScanTestDB(t)

	got := map[string]string{}
	if err := db.GetContext(context.Background(), &got, `SELECT CAST(id AS TEXT) AS id, name FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("get context typed map: %v", err)
	}
	if got["id"] != "1" || got["name"] != "alpha" {
		t.Fatalf("unexpected map values: %#v", got)
	}
}

func TestSelectContextIntoSliceOfMapAny(t *testing.T) {
	db := newMapScanTestDB(t)

	var got []map[string]any
	if err := db.SelectContext(context.Background(), &got, `SELECT id, name FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("select context map slice: %v", err)
	}
	if len(got) != 1 || got[0]["name"] != "alpha" {
		t.Fatalf("unexpected select context result: %#v", got)
	}
}

func TestSelectIntoSliceOfPointerMapString(t *testing.T) {
	db := newMapScanTestDB(t)

	var got []*map[string]string
	if err := db.Select(&got, `SELECT CAST(id AS TEXT) AS id, name FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("select pointer map slice: %v", err)
	}
	if len(got) != 1 || got[0] == nil {
		t.Fatalf("unexpected pointer map slice: %#v", got)
	}
	if (*got[0])["id"] != "1" || (*got[0])["name"] != "alpha" {
		t.Fatalf("unexpected pointer map values: %#v", *got[0])
	}
}

func TestSelectIntoSliceOfMapInt(t *testing.T) {
	db := newMapScanTestDB(t)

	var got []map[string]int
	if err := db.Select(&got, `SELECT id, active FROM forms WHERE id = 1`); err != nil {
		t.Fatalf("select int map slice: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["id"] != 1 || got[0]["active"] != 1 {
		t.Fatalf("unexpected int map values: %#v", got[0])
	}
}

func TestScanEachTypedMap(t *testing.T) {
	db := newMapScanTestDB(t)

	rows, err := db.Queryx(`SELECT CAST(id AS TEXT) AS id, name FROM forms`)
	if err != nil {
		t.Fatalf("queryx: %v", err)
	}
	defer rows.Close()

	count := 0
	if err := ScanEach(rows, false, func(row map[string]string) error {
		count++
		if row["id"] != "1" || row["name"] != "alpha" {
			t.Fatalf("unexpected row: %#v", row)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan each typed map: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 callback, got %d", count)
	}
}

func TestScanEachPointerTypedMap(t *testing.T) {
	db := newMapScanTestDB(t)

	rows, err := db.Queryx(`SELECT CAST(id AS TEXT) AS id, name FROM forms WHERE id = 1`)
	if err != nil {
		t.Fatalf("queryx: %v", err)
	}
	defer rows.Close()

	count := 0
	if err := ScanEach(rows, false, func(row *map[string]string) error {
		count++
		if row == nil || (*row)["id"] != "1" || (*row)["name"] != "alpha" {
			t.Fatalf("unexpected row: %#v", row)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan each pointer typed map: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 callback, got %d", count)
	}
}

func TestGetIntoMapWithNonStringKeyReturnsError(t *testing.T) {
	db := newMapScanTestDB(t)

	got := map[int]string{}
	err := db.Get(&got, `SELECT CAST(id AS TEXT) AS id, name FROM forms WHERE id = 1`)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "map[string]T") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRowsStructScanWithMapReturnsError(t *testing.T) {
	db := newMapScanTestDB(t)

	rows, err := db.Queryx(`SELECT id, name FROM forms WHERE id = 1`)
	if err != nil {
		t.Fatalf("queryx: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row")
	}

	dest := map[string]any{}
	err = rows.StructScan(&dest)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "expected struct") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPointerTypedMapNullConversions(t *testing.T) {
	db := newMapScanTestDB(t)

	t.Run("select", func(t *testing.T) {
		testCases := []struct {
			name    string
			query   string
			wantNil bool
			wantVal int
		}{
			{
				name:    "null pointer int",
				query:   `SELECT CAST(NULL AS INTEGER) AS n`,
				wantNil: true,
			},
			{
				name:    "non-null pointer int",
				query:   `SELECT CAST(42 AS INTEGER) AS n`,
				wantNil: false,
				wantVal: 42,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var got []map[string]*int
				if err := db.Select(&got, tc.query); err != nil {
					t.Fatalf("select map[string]*int: %v", err)
				}
				if len(got) != 1 {
					t.Fatalf("expected 1 row, got %d", len(got))
				}
				ptr := got[0]["n"]
				if tc.wantNil {
					if ptr != nil {
						t.Fatalf("expected nil pointer, got %v", *ptr)
					}
					return
				}
				if ptr == nil || *ptr != tc.wantVal {
					t.Fatalf("expected %d, got %#v", tc.wantVal, ptr)
				}
			})
		}

		stringCases := []struct {
			name    string
			query   string
			wantNil bool
			wantVal string
		}{
			{
				name:    "null pointer string",
				query:   `SELECT CAST(NULL AS TEXT) AS s`,
				wantNil: true,
			},
			{
				name:    "non-null pointer string",
				query:   `SELECT CAST('beta' AS TEXT) AS s`,
				wantNil: false,
				wantVal: "beta",
			},
		}

		for _, tc := range stringCases {
			t.Run(tc.name, func(t *testing.T) {
				var got []map[string]*string
				if err := db.Select(&got, tc.query); err != nil {
					t.Fatalf("select map[string]*string: %v", err)
				}
				if len(got) != 1 {
					t.Fatalf("expected 1 row, got %d", len(got))
				}
				ptr := got[0]["s"]
				if tc.wantNil {
					if ptr != nil {
						t.Fatalf("expected nil pointer, got %q", *ptr)
					}
					return
				}
				if ptr == nil || *ptr != tc.wantVal {
					t.Fatalf("expected %q, got %#v", tc.wantVal, ptr)
				}
			})
		}
	})

	t.Run("get", func(t *testing.T) {
		intCases := []struct {
			name    string
			query   string
			wantNil bool
			wantVal int
		}{
			{
				name:    "null pointer int",
				query:   `SELECT CAST(NULL AS INTEGER) AS n`,
				wantNil: true,
			},
			{
				name:    "non-null pointer int",
				query:   `SELECT CAST(7 AS INTEGER) AS n`,
				wantVal: 7,
			},
		}

		for _, tc := range intCases {
			t.Run(tc.name, func(t *testing.T) {
				got := map[string]*int{}
				if err := db.Get(&got, tc.query); err != nil {
					t.Fatalf("get map[string]*int: %v", err)
				}

				ptr := got["n"]
				if tc.wantNil {
					if ptr != nil {
						t.Fatalf("expected nil pointer, got %v", *ptr)
					}
					return
				}
				if ptr == nil || *ptr != tc.wantVal {
					t.Fatalf("expected %d, got %#v", tc.wantVal, ptr)
				}
			})
		}

		stringCases := []struct {
			name    string
			query   string
			wantNil bool
			wantVal string
		}{
			{
				name:    "null pointer string",
				query:   `SELECT CAST(NULL AS TEXT) AS s`,
				wantNil: true,
			},
			{
				name:    "non-null pointer string",
				query:   `SELECT CAST('gamma' AS TEXT) AS s`,
				wantVal: "gamma",
			},
		}

		for _, tc := range stringCases {
			t.Run(tc.name, func(t *testing.T) {
				got := map[string]*string{}
				if err := db.Get(&got, tc.query); err != nil {
					t.Fatalf("get map[string]*string: %v", err)
				}

				ptr := got["s"]
				if tc.wantNil {
					if ptr != nil {
						t.Fatalf("expected nil pointer, got %q", *ptr)
					}
					return
				}
				if ptr == nil || *ptr != tc.wantVal {
					t.Fatalf("expected %q, got %#v", tc.wantVal, ptr)
				}
			})
		}
	})
}
