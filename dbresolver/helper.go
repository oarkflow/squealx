package dbresolver

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
)

// isDBConnectionError reports whether an operation can reasonably be retried
// against another database. It recognizes standard driver/network failures,
// SQLSTATE connection-class errors, and the common messages emitted by popular
// database drivers when a connection is lost after checkout.
func isDBConnectionError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var stateErr interface{ SQLState() string }
	if errors.As(err, &stateErr) {
		state := stateErr.SQLState()
		if len(state) >= 2 && state[:2] == "08" { // SQLSTATE connection exception class.
			return true
		}
	}
	message := strings.ToLower(err.Error())
	for _, marker := range [...]string{
		"bad connection",
		"broken pipe",
		"connection closed",
		"connection refused",
		"connection reset",
		"connection was closed",
		"driver: bad connection",
		"invalid connection",
		"server closed the connection",
		"unexpected eof",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
