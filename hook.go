package squealx

import (
	"context"
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
