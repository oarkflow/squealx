package jsonbq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// DecryptSelect executes the query, scans into dest slice, and returns decrypted field maps
// for each row using the provided paths (dot notation).
func (s *SelectQuery) DecryptSelect(dest any, paths ...string) ([]map[string]any, error) {
	return s.DecryptSelectContext(context.Background(), dest, paths...)
}

// DecryptSelectContext executes the query with context, scans into dest slice, and returns
// decrypted field maps for each row using the provided paths (dot notation).
func (s *SelectQuery) DecryptSelectContext(ctx context.Context, dest any, paths ...string) ([]map[string]any, error) {
	if s.encrypted == nil {
		return nil, fmt.Errorf("encrypted mode is not enabled")
	}
	if err := s.ExecContext(ctx, dest); err != nil {
		return nil, err
	}

	slice, err := validateDestSlice(dest)
	if err != nil {
		return nil, err
	}

	decrypted := make([]map[string]any, 0, slice.Len())
	for i := 0; i < slice.Len(); i++ {
		dataJSON, err := extractDataJSONString(slice.Index(i))
		if err != nil {
			return nil, err
		}
		fields, err := decryptFieldsFromJSONWithConfig(s.encrypted, dataJSON, paths...)
		if err != nil {
			return nil, err
		}
		decrypted = append(decrypted, fields)
	}

	return decrypted, nil
}

// DecryptSelectTyped executes the query and returns typed decrypted DTOs for requested paths.
// dest must be a pointer to the row slice (for example *[]Athlete) containing a `data` field.
func DecryptSelectTyped[T any](s *SelectQuery, dest any, paths ...string) ([]T, error) {
	return DecryptSelectTypedContext[T](context.Background(), s, dest, paths...)
}

// DecryptSelectTypedContext executes the query with context and returns typed decrypted DTOs.
func DecryptSelectTypedContext[T any](ctx context.Context, s *SelectQuery, dest any, paths ...string) ([]T, error) {
	decryptedMaps, err := s.DecryptSelectContext(ctx, dest, paths...)
	if err != nil {
		return nil, err
	}

	out := make([]T, 0, len(decryptedMaps))
	for _, item := range decryptedMaps {
		var row T
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}

	return out, nil
}

func validateDestSlice(dest any) (reflect.Value, error) {
	if dest == nil {
		return reflect.Value{}, fmt.Errorf("dest cannot be nil")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return reflect.Value{}, fmt.Errorf("dest must be a non-nil pointer to slice")
	}
	slice := v.Elem()
	if slice.Kind() != reflect.Slice {
		return reflect.Value{}, fmt.Errorf("dest must point to a slice")
	}
	return slice, nil
}

func extractDataJSONString(item reflect.Value) (string, error) {
	if item.Kind() == reflect.Ptr {
		if item.IsNil() {
			return "", fmt.Errorf("row pointer is nil")
		}
		item = item.Elem()
	}

	switch item.Kind() {
	case reflect.Struct:
		t := item.Type()
		for i := 0; i < item.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}
			dbTag := strings.Split(field.Tag.Get("db"), ",")[0]
			if dbTag == "data" || strings.EqualFold(field.Name, "data") {
				fv := item.Field(i)
				if fv.Kind() == reflect.String {
					return fv.String(), nil
				}
				if fv.CanInterface() {
					if text, ok := fv.Interface().(string); ok {
						return text, nil
					}
				}
				return "", fmt.Errorf("data field must be string")
			}
		}
		return "", fmt.Errorf("could not find data field (db:\"data\") in row type %s", item.Type().String())
	case reflect.Map:
		if item.Type().Key().Kind() != reflect.String {
			return "", fmt.Errorf("map row keys must be string to read data field")
		}
		v := item.MapIndex(reflect.ValueOf("data"))
		if !v.IsValid() {
			return "", fmt.Errorf("map row missing key: data")
		}
		if v.Kind() == reflect.String {
			return v.String(), nil
		}
		if v.CanInterface() {
			if text, ok := v.Interface().(string); ok {
				return text, nil
			}
		}
		return "", fmt.Errorf("map row key data must be string")
	default:
		return "", fmt.Errorf("unsupported row kind %s for decrypt select", item.Kind())
	}
}
