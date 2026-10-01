package squealx

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Hook is the hook callback signature
type Hook func(ctx context.Context, query string, args ...interface{}) (context.Context, string, []interface{}, error)

// ErrorHook is the error handling callback signature
type ErrorHook func(ctx context.Context, err error, query string, args ...interface{}) error

type BeforeHook interface {
	Before(ctx context.Context, query string, args ...interface{}) (context.Context, string, []interface{}, error)
}

type AfterHook interface {
	After(ctx context.Context, query string, args ...interface{}) (context.Context, string, []interface{}, error)
}

type ErrorerHook interface {
	OnError(ctx context.Context, err error, query string, args ...interface{}) error
}

// HookPanicError reports a panic raised by a query hook. Hooks execute in the
// caller's request path, so allowing an observability or policy hook to panic
// would otherwise terminate the whole process. The original panic value and a
// stack trace are retained for diagnostics.
type HookPanicError struct {
	Phase string
	Value any
	Stack []byte
}

func (e *HookPanicError) Error() string {
	return fmt.Sprintf("squealx: %s hook panicked: %v", e.Phase, e.Value)
}

type hookRegistry struct {
	before []Hook
	after  []Hook
	onErr  []ErrorHook
}

// hookStore uses copy-on-write snapshots. Query execution only takes a pointer
// read, while the uncommon registration path performs the copy under a lock.
// This keeps hooks safe for dynamic registration without adding a mutex to the
// database hot path.
type hookStore struct {
	mu      sync.Mutex
	current atomic.Pointer[hookRegistry]
}

func newHookStore() *hookStore {
	s := &hookStore{}
	s.current.Store(&hookRegistry{})
	return s
}

var emptyHookRegistry = &hookRegistry{}

func (s *hookStore) snapshot() *hookRegistry {
	if s == nil {
		return emptyHookRegistry
	}
	current := s.current.Load()
	if current == nil {
		return emptyHookRegistry
	}
	return current
}

// empty reports whether no hook of any kind is registered. It is the fast
// path check for query execution: with no hooks there is nothing to invoke
// and no reason to attach the driver name to the context.
func (s *hookStore) empty() bool {
	r := s.snapshot()
	return len(r.before) == 0 && len(r.after) == 0 && len(r.onErr) == 0
}

func (s *hookStore) addBefore(hooks ...Hook) {
	if len(hooks) == 0 {
		return
	}
	s.mu.Lock()
	old := s.current.Load()
	if old == nil {
		old = &hookRegistry{}
	}
	next := &hookRegistry{
		before: append(append([]Hook(nil), old.before...), hooks...),
		after:  old.after,
		onErr:  old.onErr,
	}
	s.current.Store(next)
	s.mu.Unlock()
}

func (s *hookStore) addAfter(hooks ...Hook) {
	if len(hooks) == 0 {
		return
	}
	s.mu.Lock()
	old := s.current.Load()
	if old == nil {
		old = &hookRegistry{}
	}
	next := &hookRegistry{
		before: old.before,
		after:  append(append([]Hook(nil), old.after...), hooks...),
		onErr:  old.onErr,
	}
	s.current.Store(next)
	s.mu.Unlock()
}

func (s *hookStore) addError(hooks ...ErrorHook) {
	if len(hooks) == 0 {
		return
	}
	s.mu.Lock()
	old := s.current.Load()
	if old == nil {
		old = &hookRegistry{}
	}
	next := &hookRegistry{
		before: old.before,
		after:  old.after,
		onErr:  append(append([]ErrorHook(nil), old.onErr...), hooks...),
	}
	s.current.Store(next)
	s.mu.Unlock()
}

func invokeHook(phase string, hook Hook, ctx context.Context, query string, args ...any) (
	outCtx context.Context,
	outQuery string,
	outArgs []any,
	err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			outCtx, outQuery, outArgs = ctx, query, args
			err = &HookPanicError{Phase: phase, Value: recovered, Stack: debug.Stack()}
		}
	}()
	return hook(ctx, query, args...)
}

func invokeErrorHook(hook ErrorHook, ctx context.Context, cause error, query string, args ...any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &HookPanicError{Phase: "error", Value: recovered, Stack: debug.Stack()}
		}
	}()
	return hook(ctx, cause, query, args...)
}

func closeIfPossible(value any) {
	if value == nil {
		return
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if rv.IsNil() {
			return
		}
	}
	if closer, ok := value.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

type ctxDriverNameKey struct{}

func withDriverName(ctx context.Context, driverName string) context.Context {
	return context.WithValue(ctx, ctxDriverNameKey{}, driverName)
}

func DriverNameFromContext(ctx context.Context) (string, bool) {
	driverName, ok := ctx.Value(ctxDriverNameKey{}).(string)
	if !ok || driverName == "" {
		return "", false
	}
	return driverName, true
}
