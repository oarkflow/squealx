package stack

import (
	"errors"
	"testing"
)

func add(a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, errors.New("inputs must be non-negative")
	}
	return a + b, nil
}

// Benchmark wrapped function
func BenchmarkWrappedAdd(b *testing.B) {
	preHook := func(args ...any) error { return nil }
	postHook := func(results ...any) error { return nil }
	errorHook := func(err error) {}
	wrappedAdd := WrapFunc(add, WithPreHook(preHook), WithPostHook(postHook), WithErrorHook(errorHook))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wrappedAdd(5, 7)
	}
}

// Benchmark non-wrapped function
func BenchmarkNonWrappedAdd(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		add(5, 7)
	}
}
