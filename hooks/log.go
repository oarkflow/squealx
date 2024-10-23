package hooks

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/log"
)

type Notifier func(query string, args []any, latency string)

type Hook struct {
	logger       *log.Logger
	started      int
	logSlowQuery bool
	duration     time.Duration
	notify       Notifier
}

func NewLogger(logger *log.Logger, logSlowQuery bool, dur time.Duration, notify ...Notifier) *Hook {
	hook := &Hook{
		logger:       logger,
		logSlowQuery: logSlowQuery,
		duration:     dur,
	}
	if len(notify) > 0 {
		hook.notify = notify[0]
	}
	return hook
}
func (h *Hook) Before(ctx context.Context, query string, args ...any) (context.Context, error) {
	return context.WithValue(ctx, &h.started, time.Now()), nil
}

func (h *Hook) After(ctx context.Context, query string, args ...any) (context.Context, error) {
	since := time.Since(ctx.Value(&h.started).(time.Time))
	if h.logger == nil {
		if h.notify != nil {
			h.notify(query, args, fmt.Sprintf("%s", since))
		}
		return ctx, nil
	}
	if h.logSlowQuery {
		if since > h.duration {
			h.logger.Warn().
				Str("query", query).
				Any("arguments", args).
				Str("latency", fmt.Sprintf("%s", since)).
				Msg("Slow query")
			if h.notify != nil {
				h.notify(query, args, fmt.Sprintf("%s", since))
			}
		}
	} else {
		h.logger.Info().
			Str("query", query).
			Any("arguments", args).
			Str("latency", fmt.Sprintf("%s", since)).
			Msg("Query log")
	}
	return ctx, nil
}

func (h *Hook) OnError(ctx context.Context, err error, query string, args ...any) error {
	h.logger.Error().
		Err(err).
		Str("query", query).
		Any("arguments", args).
		Msg("Error on query")
	return err
}
