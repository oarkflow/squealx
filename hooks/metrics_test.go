package hooks

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoggerAfterWithoutBeforeDoesNotPanic(t *testing.T) {
	h := NewLogger(nil, false, 0)
	if _, _, _, err := h.After(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestRedactSensitiveArgs(t *testing.T) {
	args := RedactSensitiveArgs([]any{map[string]any{"password": "secret", "name": "alice"}})
	got := args[0].(map[string]any)
	if got["password"] != "[REDACTED]" || got["name"] != "alice" {
		t.Fatalf("redacted = %#v", got)
	}
}

func TestMetricsHook(t *testing.T) {
	h := NewMetrics(time.Nanosecond)
	ctx, _, _, _ := h.Before(context.Background(), "SELECT 1")
	_, _, _, _ = h.After(ctx, "SELECT 1")
	ctx, _, _, _ = h.Before(context.Background(), "SELECT 2")
	_ = h.OnError(ctx, errors.New("failed"), "SELECT 2")
	got := h.Snapshot()
	if got.Queries != 2 || got.Errors != 1 || got.InFlight != 0 || got.MaxLatency <= 0 {
		t.Fatalf("snapshot = %+v", got)
	}
}
