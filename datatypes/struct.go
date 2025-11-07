package datatypes

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Struct[T] represents a struct of any type T that can be marshaled/unmarshaled
// to/from a database using JSON encoding.
type Struct[T any] struct {
	Data T
}

// Scan implements the sql.Scanner interface, unmarshaling JSON data from the database
// into the struct.
func (s *Struct[T]) Scan(val any) error {
	switch val := val.(type) {
	case []byte:
		return json.Unmarshal(val, &s.Data)
	case string:
		return json.Unmarshal([]byte(val), &s.Data)
	case nil:
		var zero T
		s.Data = zero
		return nil
	default:
		return json.Unmarshal([]byte(fmt.Sprintf("%v", val)), &s.Data)
	}
}

// Value implements the driver.Valuer interface, marshaling the struct to JSON
// for storage in the database.
func (s *Struct[T]) Value() (driver.Value, error) {
	return json.Marshal(s.Data)
}

// NullStruct[T] represents a Struct that may be null.
// NullStruct[T] implements the scanner interface so
// it can be used as a scan destination, similar to NullString.
type NullStruct[T any] struct {
	Data  T
	Valid bool // Valid is true if Struct is not NULL
}

// Scan implements the sql.Scanner interface, unmarshaling JSON data from the database
// into the struct, handling null values.
func (s *NullStruct[T]) Scan(val any) error {
	if s.Valid = (val != nil); !s.Valid {
		var zero T
		s.Data = zero
		return nil
	}

	switch val := val.(type) {
	case []byte:
		return json.Unmarshal(val, &s.Data)
	case string:
		return json.Unmarshal([]byte(val), &s.Data)
	default:
		return json.Unmarshal([]byte(fmt.Sprintf("%v", val)), &s.Data)
	}
}

// Value implements the driver.Valuer interface, marshaling the struct to JSON
// for storage in the database, returning nil if not valid.
func (s *NullStruct[T]) Value() (driver.Value, error) {
	if !s.Valid {
		return nil, nil
	}
	return json.Marshal(s.Data)
}
