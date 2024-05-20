package squealx

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type Config struct {
	Name        string         `json:"name"`
	Key         string         `json:"key"`
	Host        string         `json:"host"`
	Port        int            `json:"port"`
	Driver      string         `json:"driver"`
	Username    string         `json:"username"`
	Password    string         `json:"password"`
	Database    string         `json:"database"`
	Params      map[string]any `json:"params"`
	MaxLifetime int64          `json:"max_lifetime"`
	MaxIdleTime int64          `json:"max_idle_time"`
	MaxOpenCons int            `json:"max_open_cons"`
	MaxIdleCons int            `json:"max_idle_cons"`
}

var keysToRemove = []string{"name", "key", "host", "port", "driver", "username", "password", "database", "params", "max_lifetime", "max_idle_time", "max_open_cons", "max_idle_cons"}

func DecodeConfig(data []byte) (cfg Config, err error) {
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return
	}
	var mapData map[string]any
	err = json.Unmarshal(data, &mapData)
	if err != nil {
		return
	}
	cfg.Params = make(map[string]any)
	for key, val := range mapData {
		if !slices.Contains(keysToRemove, key) {
			cfg.Params[key] = val
		}
	}
	return
}

func (config Config) ToString() string {
	switch config.Driver {
	case "mysql", "mariadb":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		if config.Port == 0 {
			config.Port = 3306
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.Username, config.Password, config.Host, config.Port, config.Database)
		var opts []string
		if config.Params != nil {
			for k, v := range config.Params {
				opts = append(opts, k+"="+fmt.Sprint(v))
			}
			if len(opts) > 0 {
				dsn = dsn + "?" + strings.Join(opts, "&")
			}
		}
		return dsn
	case "postgres", "psql", "postgresql":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		if config.Port == 0 {
			config.Port = 5432
		}
		dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d", config.Host, config.Username, config.Password, config.Database, config.Port)
		var opts []string
		if config.Params != nil {
			for k, v := range config.Params {
				opts = append(opts, k+"="+fmt.Sprint(v))
			}
			if len(opts) > 0 {
				dsn = dsn + " " + strings.Join(opts, " ")
			}
		}
		return dsn
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", config.Username, config.Password, config.Host, config.Port, config.Database)
		var opts []string
		if config.Params != nil {
			for k, v := range config.Params {
				opts = append(opts, k+"="+fmt.Sprint(v))
			}
			if len(opts) > 0 {
				dsn = dsn + "&" + strings.Join(opts, "&")
			}
		}
		return dsn
	}
	return ""
}
