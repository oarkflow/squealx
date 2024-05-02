package datatypes

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type StringArray []string

func (s *StringArray) Scan(val any) error {
	switch val := val.(type) {
	case []byte:
		return json.Unmarshal(val, s)
	default:
		return json.Unmarshal([]byte(fmt.Sprintf("%v", val)), s)
	}
}

func (s *StringArray) Value() (driver.Value, error) {
	return s, nil
}
