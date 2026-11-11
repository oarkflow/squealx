// Package sqliteutil contains SQLite DSN helpers that do not import a driver.
package sqliteutil

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrPrivateMemoryPool = errors.New("sqlite: private in-memory databases require one persistent pooled connection")

const (
	DefaultMaxOpenConnections = 4
	DefaultMaxIdleConnections = 2
	DefaultBusyTimeoutMS      = 5000
)

// WithDefaultBusyTimeout adds the driver's busy timeout only when the DSN
// does not already set either supported timeout parameter. Invalid query
// strings are left unchanged so the driver reports its normal parse error.
func WithDefaultBusyTimeout(dsn string, timeoutMS int) string {
	if timeoutMS <= 0 {
		return dsn
	}
	queryStart := strings.IndexByte(dsn, '?')
	if queryStart >= 0 {
		params, err := url.ParseQuery(dsn[queryStart+1:])
		if err != nil {
			return dsn
		}
		if _, ok := params["_busy_timeout"]; ok {
			return dsn
		}
		if _, ok := params["_timeout"]; ok {
			return dsn
		}
	}
	separator := "?"
	if queryStart >= 0 {
		separator = "&"
		if strings.HasSuffix(dsn, "?") || strings.HasSuffix(dsn, "&") {
			separator = ""
		}
	}
	return dsn + separator + "_busy_timeout=" + strconv.Itoa(timeoutMS)
}

// IsPrivateMemoryDSN reports whether dsn names an in-memory database that is
// private to one connection. Shared-cache memory URIs can use multiple pool
// connections.
func IsPrivateMemoryDSN(dsn string) bool {
	normalized := strings.ToLower(dsn)
	if normalized == ":memory:" || strings.HasPrefix(normalized, ":memory:?") {
		return true
	}
	if !strings.HasPrefix(normalized, "file:") || hasParam(dsn, "cache", "shared") {
		return false
	}
	return strings.HasPrefix(normalized, "file::memory:") || hasParam(dsn, "mode", "memory")
}

// DefaultPoolLimits returns bounded defaults for file/shared-memory databases
// and a persistent single connection for private in-memory databases.
func DefaultPoolLimits(dsn string) (maxOpen, maxIdle int) {
	if IsPrivateMemoryDSN(dsn) {
		return 1, 1
	}
	return DefaultMaxOpenConnections, DefaultMaxIdleConnections
}

// ValidatePrivateMemoryPool ensures the pool will retain the sole connection
// that owns a private in-memory database between operations.
func ValidatePrivateMemoryPool(dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) error {
	if !IsPrivateMemoryDSN(dsn) {
		return nil
	}
	if maxOpen != 1 || maxIdle < 1 || maxLifetime != 0 || maxIdleTime != 0 {
		return ErrPrivateMemoryPool
	}
	return nil
}

func hasParam(dsn, name, value string) bool {
	queryStart := strings.IndexByte(dsn, '?')
	if queryStart < 0 {
		return false
	}
	query := dsn[queryStart+1:]
	params, err := url.ParseQuery(query)
	if err == nil {
		for key, values := range params {
			if !strings.EqualFold(key, name) {
				continue
			}
			for _, candidate := range values {
				if strings.EqualFold(candidate, value) {
					return true
				}
			}
		}
		return false
	}
	// Invalid query strings are rejected by the SQLite driver. Detect a
	// potentially shared/private option conservatively before driver parsing.
	return strings.Contains(strings.ToLower(query), strings.ToLower(name+"="+value))
}
