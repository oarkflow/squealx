package squealx

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/oarkflow/squealx/utils/xstrings"
)

const NotNull = "__NOTNULL__"
const ExprPrefix = "__EXPR__"

var nonParamNameChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)
var safeSQLIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

func DirtyFields(u any) (map[string]any, error) {
	switch u := u.(type) {
	case map[string]any:
		return cloneMap(u), nil
	case *map[string]any:
		if u == nil {
			return nil, errors.New("expected a non-nil map pointer")
		}
		return cloneMap(*u), nil
	}
	v := reflect.ValueOf(u)
	if !v.IsValid() {
		return nil, errors.New("expected a non-nil struct or map")
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, errors.New("expected a non-nil struct pointer")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct or struct pointer, got %s", v.Kind())
	}
	setFields := make(map[string]any)
	t := v.Type()
	zero := reflect.Zero(v.Type()).Interface()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if fieldType.PkgPath != "" {
			continue
		}
		fieldName := fieldType.Tag.Get("db")
		if fieldName == "-" {
			continue
		}
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
			if field.PkgPath != "" {
				continue
			}
			columnName := field.Tag.Get("db")
			if columnName == "-" {
				continue
			}
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
	excludedSet := make(map[string]bool)
	for _, ex := range excluded {
		excludedSet[ex] = true
	}
	for key, value := range fields {
		if !excludedSet[key] {
			filtered[key] = value
		}
	}
	return filtered
}

func GetFields(entity any) (map[string]any, error) {
	switch entity := entity.(type) {
	case map[string]any:
		return cloneMap(entity), nil
	case *map[string]any:
		if entity == nil {
			return nil, errors.New("entity must be a non-nil map pointer")
		}
		return cloneMap(*entity), nil
	}
	fields := make(map[string]any)
	v := reflect.ValueOf(entity)
	if !v.IsValid() {
		return nil, errors.New("entity must be a non-nil struct")
	}
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, errors.New("entity must be a non-nil struct pointer")
		}
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct")
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		value := v.Field(i).Interface()
		columnName := field.Tag.Get("db")
		if columnName == "-" {
			continue
		}
		if columnName == "" {
			columnName = xstrings.ToSnakeCase(field.Name)
		}
		fields[columnName] = value
	}
	return fields, nil
}

// buildWhereClause generates a WHERE clause from a condition struct or map, using DirtyFields for structs
func buildWhereClause(condition any) (string, map[string]any, error) {
	return buildWhereClauseWithPrefix(condition, "")
}

func buildWhereClauseWithPrefix(condition any, prefix string) (string, map[string]any, error) {
	var whereClauses []string
	params := map[string]any{}
	fn := func(key string, value any) error {
		if expr, ok := value.(SQLExpression); ok {
			if strings.TrimSpace(string(expr)) == "" {
				return errors.New("empty SQL expression")
			}
			whereClauses = append(whereClauses, string(expr))
			return nil
		}
		if strings.HasPrefix(key, ExprPrefix) {
			return errors.New("ExprPrefix is disabled for raw conditions; use squealx.Expr with trusted application SQL")
		}
		if str, ok := value.(string); ok && strings.HasPrefix(str, ExprPrefix) {
			return errors.New("ExprPrefix is disabled for raw conditions; use squealx.Expr with trusted application SQL")
		}
		if err := validateIdentifier(key); err != nil {
			return err
		}
		paramKey := parameterName(prefix, key)
		paramName := ":" + paramKey
		if value != nil && isSliceOrArray(value) {
			if reflect.ValueOf(value).Len() == 0 {
				return fmt.Errorf("empty slice for condition %q", key)
			}
			whereClauses = append(whereClauses, fmt.Sprintf("%s IN (%s)", key, paramName))
			params[paramKey] = value
		} else if value == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", key))
		} else {
			val, ok := value.(string)
			if ok && val == NotNull {
				whereClauses = append(whereClauses, fmt.Sprintf("%s IS NOT NULL", key))
			} else {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", key, paramName))
				params[paramKey] = value
			}
		}
		return nil
	}
	switch c := condition.(type) {
	case map[string]any:
		for key, value := range c {
			if err := fn(key, value); err != nil {
				return "", nil, err
			}
		}
	case *map[string]any:
		if c == nil {
			return "", nil, errors.New("condition map pointer must be non-nil")
		}
		for key, value := range *c {
			if err := fn(key, value); err != nil {
				return "", nil, err
			}
		}
	default:
		fields, err := DirtyFields(condition)
		if err != nil {
			return "", nil, fmt.Errorf("expected map or struct for condition, got %T", condition)
		}
		for key, value := range fields {
			if err := fn(key, value); err != nil {
				return "", nil, err
			}
		}
	}
	return strings.Join(whereClauses, " AND "), params, nil
}

func validateIdentifier(identifier string) error {
	if !safeSQLIdentifier.MatchString(identifier) {
		return fmt.Errorf("unsafe SQL identifier %q", identifier)
	}
	return nil
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func isSliceOrArray(value any) bool {
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

func parameterName(prefix, key string) string {
	name := nonParamNameChars.ReplaceAllString(key, "_")
	name = strings.Trim(name, "_")
	if name == "" {
		name = "param"
	}
	if prefix != "" {
		return prefix + name
	}
	return key
}
