package hooks

import (
	"context"
	"log"
	"os"
	"time"
)

const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
)

type logger interface {
	Printf(string, ...any)
}

type Hook struct {
	log          logger
	started      int
	logSlowQuery bool
	duration     time.Duration
}

func NewLogger(logSlowQuery bool, dur time.Duration) *Hook {
	return &Hook{
		log:          log.New(os.Stderr, "", log.LstdFlags),
		logSlowQuery: logSlowQuery,
		duration:     dur,
	}
}
func (h *Hook) Before(ctx context.Context, query string, args ...any) (context.Context, error) {
	return context.WithValue(ctx, &h.started, time.Now()), nil
}

func (h *Hook) After(ctx context.Context, query string, args ...any) (context.Context, error) {
	since := time.Since(ctx.Value(&h.started).(time.Time))
	if h.logSlowQuery {
		if since > h.duration {
			h.log.Printf(Yellow+"Query: `%s`, Args: `%q`. took: %s"+Reset, query, args, since)
		}
	} else {
		h.log.Printf(Green+"Query: `%s`, Args: `%q`. took: %s"+Reset, query, args, since)
	}
	return ctx, nil
}

func (h *Hook) OnError(ctx context.Context, err error, query string, args ...any) error {
	h.log.Printf(Red+"Error: %v, Query: `%s`, Args: `%q`, Took: %s"+Reset,
		err, query, args, time.Since(ctx.Value(&h.started).(time.Time)))
	return err
}
