package datatypes

import (
	"database/sql/driver"
	"fmt"

	"github.com/oarkflow/json"
)

type Map[K comparable, V any] map[K]V

func (s *Map[K, V]) Scan(val any) error {
	switch val := val.(type) {
	case []byte:
		return json.Unmarshal(val, s)
	case string:
		return json.Unmarshal([]byte(val), s)
	case nil:
		return nil
	default:
		return json.Unmarshal([]byte(fmt.Sprintf("%v", val)), s)
	}
}

func (s *Map[K, v]) Value() (driver.Value, error) {
	return s, nil
}
