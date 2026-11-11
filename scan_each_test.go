package squealx

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/oarkflow/squealx/reflectx"
)

type scanEachMetadataRows struct {
	columnTypesCalled bool
	columnTypesErr    error
}

func (r *scanEachMetadataRows) Close() error               { return nil }
func (r *scanEachMetadataRows) Columns() ([]string, error) { return []string{"value"}, nil }
func (r *scanEachMetadataRows) Err() error                 { return nil }
func (r *scanEachMetadataRows) Next() bool                 { return false }
func (r *scanEachMetadataRows) Scan(...any) error          { return nil }
func (r *scanEachMetadataRows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.columnTypesCalled = true
	return nil, r.columnTypesErr
}

func TestScanEachLoadsColumnTypesOnlyForMaps(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	structRows := &scanEachMetadataRows{columnTypesErr: metadataErr}
	if err := ScanEach(structRows, false, func(row struct{ Value string }) error { return nil }); err != nil {
		t.Fatalf("struct ScanEach error = %v", err)
	}
	if structRows.columnTypesCalled {
		t.Fatal("struct ScanEach requested unused column type metadata")
	}

	mapRows := &scanEachMetadataRows{columnTypesErr: metadataErr}
	err := ScanEach(mapRows, false, func(row map[string]any) error { return nil })
	if !errors.Is(err, metadataErr) {
		t.Fatalf("map ScanEach error = %v, want %v", err, metadataErr)
	}
	if !mapRows.columnTypesCalled {
		t.Fatal("map ScanEach did not request column type metadata")
	}
}

func TestScanEachReusesDefaultMapperForGenericRows(t *testing.T) {
	rows := &scanEachMetadataRows{}
	if got := scanEachMapper(rows); got != mapper() {
		t.Fatal("generic rows did not use the shared default mapper")
	}
	custom := &Rows{Mapper: reflectx.NewMapper("db")}
	if got := scanEachMapper(custom); got != custom.Mapper {
		t.Fatal("Squealx rows did not retain their custom mapper")
	}
}

func TestScanEachRejectsNilInputs(t *testing.T) {
	if err := ScanEach[int](nil, false, func(int) error { return nil }); err == nil {
		t.Fatal("nil rows should return an error")
	}
	var typedNil *scanEachMetadataRows
	if err := ScanEach[int](typedNil, false, func(int) error { return nil }); err == nil {
		t.Fatal("typed nil rows should return an error")
	}
	if err := ScanEach[int](&scanEachMetadataRows{}, false, nil); err == nil {
		t.Fatal("nil callback should return an error")
	}
}

func TestMapperRebuildsWhenNameMapperChanges(t *testing.T) {
	originalNameMapper := NameMapper
	originalMapper := mapper()
	defer func() {
		NameMapper = originalNameMapper
		_ = mapper()
	}()

	NameMapper = func(name string) string { return "custom_" + name }
	customMapper := mapper()
	if customMapper == originalMapper {
		t.Fatal("mapper cache was not rebuilt after NameMapper changed")
	}
	if mapper() != customMapper {
		t.Fatal("mapper cache did not reuse the mapper for the active NameMapper")
	}
}
