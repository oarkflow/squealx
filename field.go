package squealx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

type Field struct {
	Name       string `json:"name"`
	OldName    string `json:"old_name"`
	Key        string `json:"key"`
	IsNullable string `json:"is_nullable"`
	DataType   string `json:"type"`
	Precision  int    `json:"precision"`
	Comment    string `json:"comment"`
	Default    any    `json:"default"`
	Length     int    `json:"length"`
	Extra      string `json:"extra"`
}

func (db *DB) GetTableFields(tableName, dbName string) ([]Field, error) {
	switch db.DriverName() {
	case "pgx":
		return getFieldsFromPostgres(db, tableName, dbName)
	case "mysql":
		return getFieldsFromMySQL(db, tableName, dbName)
	default:
		return nil, fmt.Errorf("%s driver not supported yet", db.DriverName())
	}
}

var (
	dbRE  = regexp.MustCompile(`(?i)\bdatabase\s*=\s*([^; ]+)`)
	tcpRE = regexp.MustCompile(`(?:tcp\([^)]+\)|unix\([^)]+\)|@[^/]+)?/([^/?]+)`)
)

func ParseDBName(dsn string) (string, error) {
	if strings.Contains(dsn, "://") {
		parsedURL, err := url.Parse(dsn)
		if err == nil {
			dbName := strings.Trim(parsedURL.Path, "/")
			if dbName != "" {
				return dbName, nil
			}
		}
	}
	re := regexp.MustCompile(`(?i)\bdbname\s*=\s*([^; ]+)`)
	if matches := re.FindStringSubmatch(dsn); len(matches) > 1 {
		return matches[1], nil
	}
	if strings.HasPrefix(dsn, "file:") {
		parts := strings.Split(dsn, "/")
		dbName := strings.TrimSuffix(parts[len(parts)-1], ".db")
		return dbName, nil
	}
	if strings.Contains(dsn, "server=") && strings.Contains(dsn, "database=") {
		if matches := dbRE.FindStringSubmatch(dsn); len(matches) > 1 {
			return matches[1], nil
		}
	}
	if strings.Contains(dsn, "@") {
		if matches := tcpRE.FindStringSubmatch(dsn); len(matches) > 1 {
			return matches[1], nil
		}
	}
	return "", errors.New("Unknown DBMS or invalid format")
}

func getFieldsFromPostgres(db *DB, tableName, dbName string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = db.Select(&fieldMaps, `
SELECT c.column_name as "name", column_default as "default", is_nullable as "is_nullable", data_type as "type", CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as "length", numeric_scale as "precision",a.column_key as "key", b.comment, '' as extra
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
select kcu.table_name,        'PRI' as column_key,        kcu.ordinal_position as position,        kcu.column_name as column_name
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name where tco.constraint_type = 'PRIMARY KEY' and kcu.table_catalog = :catalog AND kcu.table_schema = 'public' AND kcu.table_name = :table_name order by kcu.table_schema,          kcu.table_name,          position          ) a
ON c.table_name = a.table_name AND a.column_name = c.column_name
LEFT JOIN (
select
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    pgd.description as "comment"
from pg_catalog.pg_statio_all_tables as st
inner join pg_catalog.pg_description pgd on (
    pgd.objoid = st.relid
)
inner join information_schema.columns c on (
    pgd.objsubid   = c.ordinal_position and
    c.table_schema = st.schemaname and
    c.table_name   = st.relname
)
WHERE table_catalog = :catalog AND table_schema = 'public' AND c.table_name =  :table_name
) b ON c.table_name = b.table_name AND b.column_name = c.column_name
          WHERE c.table_catalog = :catalog AND c.table_schema = 'public' AND c.table_name =  :table_name
;`, map[string]any{
		"catalog":    dbName,
		"table_name": tableName,
	})
	if err != nil {
		return
	}
	bt, err := json.Marshal(fieldMaps)
	if err != nil {
		return
	}
	err = json.Unmarshal(bt, &fields)
	return
}

func getFieldsFromMySQL(db *DB, tableName, dbName string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = db.Select(&fieldMaps, "SELECT column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as type, CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as `length`, numeric_scale as `precision`, column_comment as `comment`, column_key as `key`, extra as extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  :table_name AND TABLE_SCHEMA = :schema;", map[string]any{
		"schema":     dbName,
		"table_name": tableName,
	})
	if err != nil {
		return
	}
	bt, err := json.Marshal(fieldMaps)
	if err != nil {
		return
	}
	err = json.Unmarshal(bt, &fields)
	return
}
