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

func TestRetryCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Retry(ctx, RetryPolicy{
			MaxAttempts:    2,
			InitialBackoff: time.Hour,
		}, func(context.Context) error {
			close(started)
			return driver.ErrBadConn
		})
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Retry error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Retry did not leave backoff after context cancellation")
	}
}

func TestRetryReturnsContextErrorAfterCanceledFinalAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := Retry(ctx, RetryPolicy{MaxAttempts: 1}, func(context.Context) error {
		cancel()
		return driver.ErrBadConn
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Retry error = %v, want context.Canceled", err)
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

func TestCircuitBreakerCanceledHalfOpenProbeDoesNotSucceed(t *testing.T) {
	b := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Millisecond,
		IsFailure:        func(err error) bool { return err != nil },
	})
	failure := errors.New("backend down")
	if err := b.Do(context.Background(), func(context.Context) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("initial failure = %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	err := b.Do(ctx, func(context.Context) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe = %v, want context.Canceled", err)
	}
	stats := b.Stats()
	if stats.State != CircuitHalfOpen || stats.TotalSuccess != 0 || stats.ConsecutiveSuccess != 0 {
		t.Fatalf("canceled probe changed health state: %+v", stats)
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := b.Stats().State; got != CircuitClosed {
		t.Fatalf("state after successful probe = %v, want closed", got)
	}
}

func TestCircuitBreakerClassifierCanInspectStats(t *testing.T) {
	var breaker *CircuitBreaker
	breaker = NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		IsFailure: func(error) bool {
			_ = breaker.Stats()
			return true
		},
	})
	done := make(chan error, 1)
	go func() {
		done <- breaker.Do(context.Background(), func(context.Context) error { return errors.New("failure") })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected callback error")
		}
	case <-time.After(time.Second):
		t.Fatal("classifier deadlocked while inspecting breaker stats")
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

func TestDefaultPoolConfigPreservesIdleReuseWhenOverridingOpenLimit(t *testing.T) {
	config := DefaultPoolConfig()
	if config.MaxOpenConns != 0 || config.MaxIdleConns != 2 || config.ConnMaxLifetime != 0 || config.ConnMaxIdleTime != 0 {
		t.Fatalf("default pool config = %+v", config)
	}
	config.MaxOpenConns = 50
	if err := config.Validate(); err != nil {
		t.Fatalf("default-based pool config invalid: %v", err)
	}
}
