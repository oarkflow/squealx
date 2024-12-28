package stack

import (
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
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	wrappedFn := reflect.MakeFunc(fnType, func(args []reflect.Value) (results []reflect.Value) {
		if options.preHook != nil {
			if err := callPreHook(args, options.preHook); err != nil {
				handleErrorWithHook(err, options.errorHook)
				return createErrorResults(fnType, err, errorType)
			}
		}
		results = fnValue.Call(args)
		if isErrorPresent(results, errorType) {
			err := results[len(results)-1].Interface().(error)
			handleErrorWithHook(err, options.errorHook)
			return createErrorResults(fnType, err, errorType)
		}
		if options.postHook != nil {
			if err := callPostHook(results, options.postHook); err != nil {
				handleErrorWithHook(err, options.errorHook)
				return createErrorResults(fnType, err, errorType)
			}
		}
		return results
	}).Interface()
	return wrappedFn.(T)
}

func callPreHook(args []reflect.Value, preHook PreHook) error {
	argInterfaces := make([]any, len(args))
	for i, arg := range args {
		argInterfaces[i] = arg.Interface()
	}
	return preHook(argInterfaces...)
}

func callPostHook(results []reflect.Value, postHook PostHook) error {
	resultInterfaces := make([]any, len(results))
	for i, result := range results {
		resultInterfaces[i] = result.Interface()
	}
	return postHook(resultInterfaces...)
}

func isErrorPresent(results []reflect.Value, errorType reflect.Type) bool {
	if len(results) == 0 {
		return false
	}
	lastResult := results[len(results)-1]
	return lastResult.Type().Implements(errorType) && !lastResult.IsNil()
}

func handleErrorWithHook(err error, errorHook func(error)) {
	if errorHook != nil {
		errorHook(err)
	}
}

func createErrorResults(fnType reflect.Type, err error, errorType reflect.Type) []reflect.Value {
	numOut := fnType.NumOut()
	results := make([]reflect.Value, numOut)
	for i := 0; i < numOut; i++ {
		outType := fnType.Out(i)
		if outType == errorType {
			results[i] = reflect.ValueOf(err)
		} else {
			results[i] = reflect.Zero(outType)
		}
	}
	return results
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
