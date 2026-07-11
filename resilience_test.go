package squealx

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryEventuallySucceeds(t *testing.T) {
	var calls atomic.Int32
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 3}, func(context.Context) error {
		if calls.Add(1) < 3 {
			return driver.ErrBadConn
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestRetryStopsForPermanentError(t *testing.T) {
	permanent := errors.New("syntax error")
	var calls atomic.Int32
	err := Retry(context.Background(), DefaultRetryPolicy(), func(context.Context) error {
		calls.Add(1)
		return permanent
	})
	if !errors.Is(err, permanent) {
		t.Fatalf("error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestRetryHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Retry(ctx, DefaultRetryPolicy(), func(context.Context) error { return driver.ErrBadConn }); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestCircuitBreakerLifecycle(t *testing.T) {
	b := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      time.Millisecond,
		IsFailure:        func(err error) bool { return err != nil },
	})
	failure := errors.New("down")
	for i := 0; i < 2; i++ {
		if err := b.Do(context.Background(), func(context.Context) error { return failure }); !errors.Is(err, failure) {
			t.Fatalf("failure = %v", err)
		}
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open error = %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := b.Stats().State; got != CircuitClosed {
		t.Fatalf("state = %v", got)
	}
}

func TestPoolConfigValidation(t *testing.T) {
	if err := (PoolConfig{MaxOpenConns: 2, MaxIdleConns: 3}).Validate(); !errors.Is(err, ErrInvalidPool) {
		t.Fatalf("error = %v", err)
	}
	if err := (PoolConfig{MaxOpenConns: 3, MaxIdleConns: 2}).Validate(); err != nil {
		t.Fatal(err)
	}
}
