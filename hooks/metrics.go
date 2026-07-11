package hooks

import (
	"context"
	"sync/atomic"
	"time"
)

type metricsStartKey struct{ hook *MetricsHook }

type MetricsSnapshot struct {
	Queries      uint64
	Errors       uint64
	SlowQueries  uint64
	InFlight     int64
	TotalLatency time.Duration
	MaxLatency   time.Duration
}

// MetricsHook provides dependency-free, lock-free query counters suitable for
// direct export to Prometheus/OpenTelemetry adapters.
type MetricsHook struct {
	slowThreshold time.Duration
	queries       atomic.Uint64
	errors        atomic.Uint64
	slow          atomic.Uint64
	inFlight      atomic.Int64
	totalNanos    atomic.Uint64
	maxNanos      atomic.Uint64
}

func NewMetrics(slowThreshold time.Duration) *MetricsHook {
	return &MetricsHook{slowThreshold: slowThreshold}
}

func (h *MetricsHook) Before(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	h.inFlight.Add(1)
	return context.WithValue(ctx, metricsStartKey{hook: h}, time.Now()), query, args, nil
}

func (h *MetricsHook) record(ctx context.Context, failed bool) {
	h.inFlight.Add(-1)
	h.queries.Add(1)
	if failed {
		h.errors.Add(1)
	}
	started, ok := ctx.Value(metricsStartKey{hook: h}).(time.Time)
	if !ok {
		return
	}
	elapsed := time.Since(started)
	nanos := uint64(elapsed)
	h.totalNanos.Add(nanos)
	for current := h.maxNanos.Load(); nanos > current; current = h.maxNanos.Load() {
		if h.maxNanos.CompareAndSwap(current, nanos) {
			break
		}
	}
	if h.slowThreshold > 0 && elapsed >= h.slowThreshold {
		h.slow.Add(1)
	}
}

func (h *MetricsHook) After(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	h.record(ctx, false)
	return ctx, query, args, nil
}

func (h *MetricsHook) OnError(ctx context.Context, err error, query string, args ...any) error {
	h.record(ctx, true)
	return err
}

func (h *MetricsHook) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Queries: h.queries.Load(), Errors: h.errors.Load(), SlowQueries: h.slow.Load(),
		InFlight: h.inFlight.Load(), TotalLatency: time.Duration(h.totalNanos.Load()),
		MaxLatency: time.Duration(h.maxNanos.Load()),
	}
}
