package sqlbuilder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/oarkflow/squealx/utils/xstrings"
)

func InsertQuery(table string, data any) string {
	fields := Fields(data)
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES (:%s)", table, strings.Join(fields, ", "), strings.Join(fields, ", :"))
}

func Fields(input any) []string {
	var result []string

	switch reflect.TypeOf(input).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(input)
		for i := 0; i < s.Len(); i++ {
			elem := s.Index(i)
			if elem.IsValid() {
				result = append(result, Fields(elem.Interface())...)
			}
		}
	case reflect.Map:
		s := reflect.ValueOf(input)
		for _, key := range s.MapKeys() {
			result = append(result, key.Interface().(string))
		}
	case reflect.Struct:
		value := reflect.ValueOf(input)
		valueType := reflect.TypeOf(input)
		for i := 0; i < value.NumField(); i++ {
			tag := valueType.Field(i).Tag.Get("db")
			if tag != "" {
				result = append(result, tag)
			} else {
				result = append(result, xstrings.ToSnakeCase(valueType.Field(i).Name))
			}
		}
	}

	return result
}
