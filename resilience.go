package squealx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCircuitOpen    = errors.New("squealx: circuit breaker is open")
	ErrHalfOpenBusy   = errors.New("squealx: circuit breaker probe is already running")
	ErrInvalidPool    = errors.New("squealx: invalid pool configuration")
	ErrRetryExhausted = errors.New("squealx: retry attempts exhausted")
)

// RetryPolicy controls explicit retries. Writes are never retried implicitly;
// callers should use ExecRetryContext only for idempotent statements or when an
// idempotency key/transaction constraint makes replay safe.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
	RetryIf        func(error) bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     250 * time.Millisecond,
		Multiplier:     2,
		Jitter:         0.20,
		RetryIf:        IsTransientDBError,
	}
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.InitialBackoff < 0 {
		p.InitialBackoff = 0
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = p.InitialBackoff
	}
	if p.MaxBackoff < p.InitialBackoff {
		p.MaxBackoff = p.InitialBackoff
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	if p.RetryIf == nil {
		p.RetryIf = IsTransientDBError
	}
	return p
}

var retryJitterState atomic.Uint64

func init() {
	retryJitterState.Store(uint64(time.Now().UnixNano()) | 1)
}

func nextRetryUnit() float64 {
	x := retryJitterState.Add(0x9e3779b97f4a7c15)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return float64(x>>11) / float64(uint64(1)<<53)
}

func retryDelay(p RetryPolicy, attempt int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	factor := math.Pow(p.Multiplier, float64(attempt-1))
	d := float64(p.InitialBackoff) * factor
	if d > float64(p.MaxBackoff) {
		d = float64(p.MaxBackoff)
	}
	if p.Jitter > 0 {
		d *= 1 + ((nextRetryUnit()*2)-1)*p.Jitter
	}
	if d < 0 {
		return 0
	}
	return time.Duration(d)
}

// Retry executes fn synchronously until it succeeds, the policy rejects the
// error, the context ends, or MaxAttempts is reached. The returned error keeps
// the final database error available through errors.Is/errors.As.
func Retry(ctx context.Context, policy RetryPolicy, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("squealx: nil retry function")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	policy = policy.normalized()
	var last error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = fn(ctx)
		if last == nil {
			return nil
		}
		if attempt == policy.MaxAttempts || !policy.RetryIf(last) {
			break
		}
		delay := retryDelay(policy, attempt)
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("%w: %w", ErrRetryExhausted, last)
}

// IsTransientDBError recognizes transport failures, bad pooled connections,
// serialization failures, deadlocks, and common driver error messages.
func IsTransientDBError(err error) bool {
	var noRetry interface{ NonRetryable() bool }
	if errors.As(err, &noRetry) && noRetry.NonRetryable() {
		return false
	}
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
		if len(state) >= 2 && (state[:2] == "08" || state[:2] == "40") {
			return true
		}
	}
	message := strings.ToLower(err.Error())
	for _, marker := range [...]string{
		"bad connection", "broken pipe", "connection closed", "connection refused",
		"connection reset", "deadlock", "driver: bad connection", "invalid connection",
		"lock wait timeout", "serialization failure", "server closed the connection",
		"too many connections", "unexpected eof",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (db *DB) ExecRetryContext(ctx context.Context, policy RetryPolicy, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := Retry(ctx, policy, func(ctx context.Context) error {
		var err error
		result, err = db.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

func (db *DB) GetRetryContext(ctx context.Context, policy RetryPolicy, dest any, query string, args ...any) error {
	return Retry(ctx, policy, func(ctx context.Context) error {
		return db.GetContext(ctx, dest, query, args...)
	})
}

func (db *DB) SelectRetryContext(ctx context.Context, policy RetryPolicy, dest any, query string, args ...any) error {
	return Retry(ctx, policy, func(ctx context.Context) error {
		return db.SelectContext(ctx, dest, query, args...)
	})
}

// TransactionRetryContext retries only failures returned before commit. Commit
// errors are returned directly because the commit outcome can be ambiguous.
func (db *DB) TransactionRetryContext(ctx context.Context, policy RetryPolicy, opts *sql.TxOptions, fn func(*Tx) error) error {
	if fn == nil {
		return errors.New("squealx: nil transaction function")
	}
	return Retry(ctx, policy, func(ctx context.Context) error {
		tx, err := db.BeginTxx(ctx, opts)
		if err != nil {
			return err
		}
		if err = fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			// Prevent replay after an ambiguous commit result.
			return nonRetryableError{err}
		}
		return nil
	})
}

type nonRetryableError struct{ error }

func (e nonRetryableError) Unwrap() error      { return e.error }
func (e nonRetryableError) NonRetryable() bool { return true }

// BreakerState is the observable circuit state.
type BreakerState uint8

const (
	CircuitClosed BreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

type CircuitBreakerConfig struct {
	FailureThreshold uint32
	SuccessThreshold uint32
	OpenTimeout      time.Duration
	IsFailure        func(error) bool
}

type CircuitBreakerStats struct {
	State              BreakerState
	ConsecutiveFailure uint32
	ConsecutiveSuccess uint32
	TotalSuccess       uint64
	TotalFailure       uint64
	Rejected           uint64
	OpenedAt           time.Time
}

// CircuitBreaker is concurrency-safe. Only one probe is admitted while the
// breaker is half-open, avoiding a recovery thundering herd.
type CircuitBreaker struct {
	mu                  sync.Mutex
	config              CircuitBreakerConfig
	state               BreakerState
	failures            uint32
	successes           uint32
	totalSuccess        uint64
	totalFailure        uint64
	rejected            uint64
	openedAt            time.Time
	halfOpenProbeActive bool
}

func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold == 0 {
		config.SuccessThreshold = 1
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = 30 * time.Second
	}
	if config.IsFailure == nil {
		config.IsFailure = IsTransientDBError
	}
	return &CircuitBreaker{config: config}
}

func (b *CircuitBreaker) allow(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == CircuitOpen {
		if now.Sub(b.openedAt) < b.config.OpenTimeout {
			b.rejected++
			return ErrCircuitOpen
		}
		b.state = CircuitHalfOpen
		b.halfOpenProbeActive = false
	}
	if b.state == CircuitHalfOpen {
		if b.halfOpenProbeActive {
			b.rejected++
			return ErrHalfOpenBusy
		}
		b.halfOpenProbeActive = true
	}
	return nil
}

func (b *CircuitBreaker) record(err error, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == CircuitHalfOpen {
		b.halfOpenProbeActive = false
	}
	if err == nil || !b.config.IsFailure(err) {
		b.totalSuccess++
		b.failures = 0
		b.successes++
		if b.state == CircuitHalfOpen && b.successes >= b.config.SuccessThreshold {
			b.state = CircuitClosed
			b.successes = 0
		}
		return
	}
	b.totalFailure++
	b.successes = 0
	b.failures++
	if b.state == CircuitHalfOpen || b.failures >= b.config.FailureThreshold {
		b.state = CircuitOpen
		b.openedAt = now
		b.failures = 0
	}
}

func (b *CircuitBreaker) Do(ctx context.Context, fn func(context.Context) error) error {
	if b == nil {
		return errors.New("squealx: nil circuit breaker")
	}
	if fn == nil {
		return errors.New("squealx: nil circuit function")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if err := b.allow(now); err != nil {
		return err
	}
	err := fn(ctx)
	b.record(err, time.Now())
	return err
}

func (b *CircuitBreaker) Reset() {
	b.mu.Lock()
	b.state = CircuitClosed
	b.failures = 0
	b.successes = 0
	b.openedAt = time.Time{}
	b.halfOpenProbeActive = false
	b.mu.Unlock()
}

func (b *CircuitBreaker) Stats() CircuitBreakerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return CircuitBreakerStats{
		State: b.state, ConsecutiveFailure: b.failures, ConsecutiveSuccess: b.successes,
		TotalSuccess: b.totalSuccess, TotalFailure: b.totalFailure, Rejected: b.rejected,
		OpenedAt: b.openedAt,
	}
}

// PoolConfig centralizes database/sql pool controls with validation.
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func (c PoolConfig) Validate() error {
	if c.MaxOpenConns < 0 || c.MaxIdleConns < 0 || c.ConnMaxLifetime < 0 || c.ConnMaxIdleTime < 0 {
		return ErrInvalidPool
	}
	if c.MaxOpenConns > 0 && c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("%w: max idle connections (%d) exceed max open connections (%d)", ErrInvalidPool, c.MaxIdleConns, c.MaxOpenConns)
	}
	return nil
}

func (db *DB) ApplyPoolConfig(config PoolConfig) error {
	if db == nil || db.SQLDB == nil {
		return errors.New("squealx: nil database")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	return nil
}

type PoolHealth struct {
	Healthy bool
	Latency time.Duration
	Stats   sql.DBStats
	Error   error
}

func (db *DB) HealthContext(ctx context.Context) PoolHealth {
	if ctx == nil {
		ctx = context.Background()
	}
	health := PoolHealth{}
	if db == nil || db.SQLDB == nil {
		health.Error = errors.New("squealx: nil database")
		return health
	}
	health.Stats = db.Stats()
	started := time.Now()
	health.Error = db.PingContext(ctx)
	health.Latency = time.Since(started)
	health.Healthy = health.Error == nil
	return health
}
