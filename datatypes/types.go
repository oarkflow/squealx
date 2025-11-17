package datatypes

import (
	"bytes"
	"compress/gzip"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/oarkflow/date"
	"github.com/oarkflow/json"
)

// Serializable data marshal/unmarshal constraint for Binary type.
type Serializable[T any] interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) (T, error)
}

// Binary[T] is a []byte which transparently Binary[T] data being submitted to
// a database and unmarshal data being Scanned from a database.
type Binary[T Serializable[T]] struct {
	Data T
}

// NullBinary[T] represents a Binary that may be null.
// NullBinary[T] implements the scanner interface so
// it can be used as a scan destination, similar to NullString.
type NullBinary[T Serializable[T]] struct {
	Data  T
	Valid bool // Valid is true if Binary is not NULL
}

// Value implements the driver.Valuer interface, marshal the raw value of
// this Binary[T].
func (b *Binary[T]) Value() (driver.Value, error) {
	return b.Data.MarshalBinary()
}

// Scan implements the sql.Scanner interface, unmashal the value coming off
// the wire and storing the raw result in the Binary[T].
func (b *Binary[T]) Scan(src any) (err error) {
	var source []byte
	switch t := src.(type) {
	case string:
		source = []byte(t)
	case []byte:
		source = t
	case nil:
	default:
		return errors.New("incompatible type for Binary")
	}
	b.Data, err = b.Data.UnmarshalBinary(source)
	return
}

// Value implements the driver.Valuer interface, marshal the raw value of
// this Binary[T].
func (b *NullBinary[T]) Value() (driver.Value, error) {
	if !b.Valid {
		return nil, nil
	}
	return b.Data.MarshalBinary()
}

// Scan implements the sql.Scanner interface, unmashal the value coming off
// the wire and storing the raw result in the Binary[T].
func (b *NullBinary[T]) Scan(src any) (err error) {
	if b.Valid = (src != nil); !b.Valid {
		return nil
	}
	var source []byte
	switch t := src.(type) {
	case string:
		source = []byte(t)
	case []byte:
		source = t
	case nil:
	default:
		return errors.New("incompatible type for Binary")
	}
	b.Data, err = b.Data.UnmarshalBinary(source)
	return
}

// GzippedText is a []byte which transparently gzips data being submitted to
// a database and ungzips data being Scanned from a database.
type GzippedText []byte

// Value implements the driver.Valuer interface, gzipping the raw value of
// this GzippedText.
func (g GzippedText) Value() (driver.Value, error) {
	b := make([]byte, 0, len(g))
	buf := bytes.NewBuffer(b)
	w := gzip.NewWriter(buf)
	w.Write(g)
	w.Close()
	return buf.Bytes(), nil

}

// Scan implements the sql.Scanner interface, ungzipping the value coming off
// the wire and storing the raw result in the GzippedText.
func (g *GzippedText) Scan(src any) error {
	var source []byte
	switch src := src.(type) {
	case string:
		source = []byte(src)
	case []byte:
		source = src
	default:
		return errors.New("incompatible type for GzippedText")
	}
	reader, err := gzip.NewReader(bytes.NewReader(source))
	if err != nil {
		return err
	}
	defer reader.Close()
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	*g = GzippedText(b)
	return nil
}

// JSONText is a json.RawMessage, which is a []byte underneath.
// Value() validates the json format in the source, and returns an error if
// the json is not valid.  Scan does no validation.  JSONText additionally
// implements `Unmarshal`, which unmarshals the json within to an any
type JSONText json.RawMessage

var emptyJSON = JSONText("{}")

// MarshalJSON returns the *j as the JSON encoding of j.
func (j JSONText) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return emptyJSON, nil
	}
	return j, nil
}

// UnmarshalJSON sets *j to a copy of data
func (j *JSONText) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("JSONText: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

// Value returns j as a value.  This does a validating unmarshal into another
// RawMessage.  If j is invalid json, it returns an error.
func (j JSONText) Value() (driver.Value, error) {
	var m json.RawMessage
	var err = j.Unmarshal(&m)
	if err != nil {
		return []byte{}, err
	}
	return []byte(j), nil
}

// Scan stores the src in *j.  No validation is done.
func (j *JSONText) Scan(src any) error {
	var source []byte
	switch t := src.(type) {
	case string:
		source = []byte(t)
	case []byte:
		if len(t) == 0 {
			source = emptyJSON
		} else {
			source = t
		}
	case nil:
		*j = emptyJSON
	default:
		return errors.New("incompatible type for JSONText")
	}
	*j = append((*j)[0:0], source...)
	return nil
}

// Unmarshal unmarshal's the json in j to v, as in json.Unmarshal.
func (j *JSONText) Unmarshal(v any) error {
	if len(*j) == 0 {
		*j = emptyJSON
	}
	return json.Unmarshal([]byte(*j), v)
}

// String supports pretty printing for JSONText types.
func (j JSONText) String() string {
	return string(j)
}

// Time is a wrapper around time.Time that supports scanning from string values.
type Time struct {
	time.Time
}

// Scan implements the Scanner interface, allowing scanning from string or time.Time.
func (t *Time) Scan(value any) error {
	if value == nil {
		t.Time = time.Time{}
		return nil
	}

	switch v := value.(type) {
	case time.Time:
		t.Time = v
		return nil
	case string:
		if parsed, err := date.Parse(v); err == nil {
			t.Time = parsed
			return nil
		}
		// Try parsing as Go time.Time.String() format with duplicated timezone
		if parsed, err := time.Parse("2006-01-02 15:04:05.999999999 +0700 +0700 m=+0.000000001", v); err == nil {
			t.Time = parsed
			return nil
		}
		return fmt.Errorf("failed to parse time string: %s", v)
	case []byte:
		return t.Scan(string(v))
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type *Time", value)
	}
}

// Value implements the driver Valuer interface.
func (t Time) Value() (driver.Value, error) {
	return t.Time, nil
}

// NullTime is a wrapper around time.Time that supports SQL NULL values.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (n *NullTime) Scan(value any) error {
	if value == nil {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}
	n.Valid = true

	switch v := value.(type) {
	case time.Time:
		n.Time = v
		return nil
	case string:
		if parsed, err := date.Parse(v); err == nil {
			n.Time = parsed
			return nil
		}
		return fmt.Errorf("failed to parse time string: %s", v)
	case []byte:
		return n.Scan(string(v))
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type *NullTime", value)
	}
}

// Value implements the driver Valuer interface.
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

// NullJSONText represents a JSONText that may be null.
// NullJSONText implements the scanner interface so
// it can be used as a scan destination, similar to NullString.
type NullJSONText struct {
	JSONText
	Valid bool // Valid is true if JSONText is not NULL
}

// Scan implements the Scanner interface.
func (n *NullJSONText) Scan(value any) error {
	if value == nil {
		n.JSONText, n.Valid = emptyJSON, false
		return nil
	}
	n.Valid = true
	return n.JSONText.Scan(value)
}

// Value implements the driver Valuer interface.
func (n NullJSONText) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.JSONText.Value()
}

// BitBool is an implementation of a bool for the MySQL type BIT(1).
// This type allows you to avoid wasting an entire byte for MySQL's boolean type TINYINT.
type BitBool bool

// Value implements the driver.Valuer interface,
// and turns the BitBool into a bitfield (BIT(1)) for MySQL storage.
func (b BitBool) Value() (driver.Value, error) {
	if b {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

// Scan implements the sql.Scanner interface,
// and turns the bitfield incoming from MySQL into a BitBool
func (b *BitBool) Scan(src any) error {
	v, ok := src.([]byte)
	if !ok {
		return errors.New("bad []byte type assertion")
	}
	*b = v[0] == 1
	return nil
}

func DetectType(value string) string {
	if _, err := strconv.ParseInt(value, 0, 64); err == nil {
		return "int"
	}
	if _, err := strconv.ParseInt(value, 0, 32); err == nil {
		return "int32"
	}
	if _, err := strconv.ParseInt(value, 0, 16); err == nil {
		return "int16"
	}
	if _, err := strconv.ParseInt(value, 0, 8); err == nil {
		return "int8"
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "float"
	}
	if _, err := strconv.ParseFloat(value, 32); err == nil {
		return "float32"
	} else if _, err := strconv.ParseBool(value); err == nil {
		return "boolean"
	} else if _, err := time.Parse("2006-01-02", value); err == nil {
		return "time"
	} else if isSlice(value) {
		return "slice"
	} else if isMap(value) {
		return "map"
	} else {
		return "unknown"
	}
}

func isSlice(value string) bool {
	// Very basic check for slice format
	return len(value) > 2 && value[0] == '[' && value[len(value)-1] == ']'
}

func isMap(value string) bool {
	// Very basic check for map format
	return len(value) > 2 && value[0] == '{' && value[len(value)-1] == '}'
}
