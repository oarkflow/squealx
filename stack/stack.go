package stack

import (
	"errors"
	"reflect"
)

type PreHook func(args ...any) error
type PostHook func(results ...any) error

type WrapOption func(*wrapOptions)

type wrapOptions struct {
	preHook   PreHook
	postHook  PostHook
	errorHook func(err error)
}

func WrapFunc[T any](fn T, opts ...WrapOption) T {
	options := &wrapOptions{}
	for _, opt := range opts {
		opt(options)
	}
	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()
	if fnType.Kind() != reflect.Func {
		panic("WrapFunc expects a function")
	}
	wrappedFn := reflect.MakeFunc(fnType, func(args []reflect.Value) (results []reflect.Value) {
		argInterfaces := make([]any, len(args))
		for i, arg := range args {
			argInterfaces[i] = arg.Interface()
		}
		if options.preHook != nil {
			if err := options.preHook(argInterfaces...); err != nil {
				handleErrorWithHook(err, options.errorHook)
				return handleError(fnType, err)
			}
		}
		results = fnValue.Call(args)
		if len(results) > 0 {
			lastResult := results[len(results)-1]
			if lastResult.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !lastResult.IsNil() {
				err := lastResult.Interface().(error)
				handleErrorWithHook(err, options.errorHook)
				return handleError(fnType, err)
			}
		}
		resultInterfaces := make([]any, len(results))
		for i, result := range results {
			resultInterfaces[i] = result.Interface()
		}
		if options.postHook != nil {
			if err := options.postHook(resultInterfaces...); err != nil {
				handleErrorWithHook(err, options.errorHook)
				return handleError(fnType, err)
			}
		}
		return results
	}).Interface()
	return wrappedFn.(T)
}

func handleErrorWithHook(err error, errorHook func(error)) {
	if errorHook != nil {
		errorHook(err)
	}
}

func handleError(fnType reflect.Type, err error) []reflect.Value {
	numOut := fnType.NumOut()
	results := make([]reflect.Value, numOut)
	for i := 0; i < numOut; i++ {
		outType := fnType.Out(i)
		if outType == reflect.TypeOf((*error)(nil)).Elem() {
			results[i] = reflect.ValueOf(err)
		} else {
			results[i] = reflect.Zero(outType)
		}
	}
	return results
}

func add(a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, errors.New("inputs must be non-negative")
	}
	return a + b, nil
}

func WithPreHook(hook PreHook) WrapOption {
	return func(opts *wrapOptions) {
		opts.preHook = hook
	}
}

func WithPostHook(hook PostHook) WrapOption {
	return func(opts *wrapOptions) {
		opts.postHook = hook
	}
}

func WithErrorHook(hook func(err error)) WrapOption {
	return func(opts *wrapOptions) {
		opts.errorHook = hook
	}
}
