package hooks

import (
	"context"
	"strings"
	"time"

	"github.com/oarkflow/log"
)

type Notifier func(query string, args []any, latency string)
type ArgRedactor func(args []any) []any

type logStartKey struct{ hook *Hook }

type Hook struct {
	logger       *log.Logger
	logSlowQuery bool
	duration     time.Duration
	notify       Notifier
	redact       ArgRedactor
}

func NewLogger(logger *log.Logger, logSlowQuery bool, dur time.Duration, notify ...Notifier) *Hook {
	hook := &Hook{
		logger:       logger,
		logSlowQuery: logSlowQuery,
		duration:     dur,
		redact:       RedactSensitiveArgs,
	}
	if len(notify) > 0 {
		hook.notify = notify[0]
	}
	return hook
}

// WithRedactor replaces the default shallow redactor. Passing nil disables
// argument logging entirely, which is the safest choice for authentication or
// payment queries.
func (h *Hook) WithRedactor(redactor ArgRedactor) *Hook {
	h.redact = redactor
	return h
}

func (h *Hook) safeArgs(args []any) []any {
	if h.redact == nil {
		return nil
	}
	return h.redact(args)
}

func (h *Hook) Before(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, logStartKey{hook: h}, time.Now()), query, args, nil
}

func (h *Hook) elapsed(ctx context.Context) time.Duration {
	if ctx != nil {
		if started, ok := ctx.Value(logStartKey{hook: h}).(time.Time); ok {
			return time.Since(started)
		}
	}
	return 0
}

func (h *Hook) After(ctx context.Context, query string, args ...any) (context.Context, string, []any, error) {
	since := h.elapsed(ctx)
	latency := since.String()
	safeArgs := h.safeArgs(args)
	if h.logger == nil {
		if h.notify != nil {
			h.notify(query, safeArgs, latency)
		}
		return ctx, query, args, nil
	}
	if h.logSlowQuery {
		if since >= h.duration {
			h.logger.Warn().Str("query", query).Any("arguments", safeArgs).Str("latency", latency).Msg("Slow query")
			if h.notify != nil {
				h.notify(query, safeArgs, latency)
			}
		}
	} else {
		h.logger.Info().Str("query", query).Any("arguments", safeArgs).Str("latency", latency).Msg("Query log")
	}
	return ctx, query, args, nil
}

func (h *Hook) OnError(ctx context.Context, err error, query string, args ...any) error {
	if err == nil {
		return nil
	}
	safeArgs := h.safeArgs(args)
	if h.logger == nil {
		if h.notify != nil {
			h.notify(query, safeArgs, err.Error())
		}
		return err
	}
	h.logger.Error().Err(err).Str("query", query).Any("arguments", safeArgs).Msg("Error on query")
	return err
}

var sensitiveKeys = map[string]struct{}{
	"authorization": {}, "card": {}, "card_number": {}, "cvv": {}, "password": {},
	"secret": {}, "token": {}, "access_token": {}, "refresh_token": {}, "api_key": {},
}

// RedactSensitiveArgs copies only containers that need modification. Positional
// scalar arguments are preserved because their meaning cannot be inferred; use
// WithRedactor(nil) or a custom redactor when positional values are sensitive.
func RedactSensitiveArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, arg := range args {
		switch value := arg.(type) {
		case map[string]any:
			copyMap := make(map[string]any, len(value))
			for key, item := range value {
				if _, sensitive := sensitiveKeys[strings.ToLower(key)]; sensitive {
					copyMap[key] = "[REDACTED]"
				} else {
					copyMap[key] = item
				}
			}
			out[i] = copyMap
		case map[string]string:
			copyMap := make(map[string]string, len(value))
			for key, item := range value {
				if _, sensitive := sensitiveKeys[strings.ToLower(key)]; sensitive {
					copyMap[key] = "[REDACTED]"
				} else {
					copyMap[key] = item
				}
			}
			out[i] = copyMap
		default:
			out[i] = arg
		}
	}
	return out
}
