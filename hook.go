package squealx

import (
	"context"
)

// Hook is the hook callback signature
type Hook func(ctx context.Context, query string, args ...interface{}) (context.Context, error)

// ErrorHook is the error handling callback signature
type ErrorHook func(ctx context.Context, err error, query string, args ...interface{}) error

type BeforeHook interface {
	Before(ctx context.Context, query string, args ...interface{}) (context.Context, error)
}

type AfterHook interface {
	After(ctx context.Context, query string, args ...interface{}) (context.Context, error)
}

type ErrorerHook interface {
	OnError(ctx context.Context, err error, query string, args ...interface{}) error
}
