package squealx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/oarkflow/squealx/utils/xstrings"
)

func DirtyFields(u any) (map[string]interface{}, error) {
	switch u := u.(type) {
	case map[string]any:
		return u, nil
	case *map[string]any:
		return *u, nil
	}
	v := reflect.ValueOf(u)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct or struct pointer, got %s", v.Kind())
	}
	setFields := make(map[string]interface{})
	t := v.Type()
	zero := reflect.Zero(v.Type()).Interface()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		fieldName := fieldType.Tag.Get("db")
		if fieldName == "" {
			fieldName = xstrings.ToSnakeCase(fieldType.Name)
		}
		zeroField := reflect.ValueOf(zero).Field(i)
		if !reflect.DeepEqual(field.Interface(), zeroField.Interface()) {
			setFields[fieldName] = field.Interface()
		}
	}
	return setFields, nil
}

func getAllColumns[T any]() []string {
	var t T
	var columns []string
	tValue := reflect.TypeOf(t)
	if tValue.Kind() == reflect.Ptr {
		tValue = tValue.Elem()
	}
	if tValue.Kind() == reflect.Struct {
		for i := 0; i < tValue.NumField(); i++ {
			field := tValue.Field(i)
			columnName := field.Tag.Get("db")
			if columnName == "" {
				columnName = xstrings.ToSnakeCase(field.Name)
			}
			columns = append(columns, columnName)
		}
	}
	return columns
}

func filterFields(fields map[string]any, allowed []string) map[string]any {
	filtered := make(map[string]any)
	for _, key := range allowed {
		if value, exists := fields[key]; exists {
			filtered[key] = value
		}
	}
	return filtered
}

func excludeFieldsSlice(fields []string, excluded []string) (f []string) {
	excludedSet := make(map[string]bool)
	for _, ex := range excluded {
		excludedSet[ex] = true
	}
	for _, key := range fields {
		if !excludedSet[key] {
			f = append(f, key)
		}
	}
	return
}

func excludeFields(fields map[string]any, excluded []string) map[string]any {
	filtered := make(map[string]any)
	for key, value := range fields {
		excludedSet := make(map[string]bool)
		for _, ex := range excluded {
			excludedSet[ex] = true
		}
		if !excludedSet[key] {
			filtered[key] = value
		}
	}
	return filtered
}

func GetFields(entity any) (map[string]any, error) {
	switch entity := entity.(type) {
	case map[string]any:
		return entity, nil
	case *map[string]any:
		return *entity, nil
	}
	fields := make(map[string]any)
	v := reflect.ValueOf(entity)
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct")
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i).Interface()
		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = xstrings.ToSnakeCase(field.Name)
		}
		fields[columnName] = value
	}
	return fields, nil
}

// buildWhereClause generates a WHERE clause from a condition struct or map, using DirtyFields for structs
func buildWhereClause(condition any) (string, map[string]any, error) {
	var whereClauses []string
	params := map[string]any{}
	fn := func(key string, value any) {
		paramName := ":" + key
		if value != nil && reflect.TypeOf(value).Kind() == reflect.Slice {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IN (%s)", key, paramName))
		} else if value == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", key))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", key, paramName))
		}
		params[key] = value
	}
	switch c := condition.(type) {
	case map[string]any:
		for key, value := range c {
			fn(key, value)
		}
	case *map[string]any:
		for key, value := range *c {
			fn(key, value)
		}
	default:
		fields, err := DirtyFields(condition)
		if err != nil {
			return "", nil, fmt.Errorf("expected map or struct for condition, got %T", condition)
		}
		for key, value := range fields {
			fn(key, value)
		}
	}
	return strings.Join(whereClauses, " AND "), params, nil
}
