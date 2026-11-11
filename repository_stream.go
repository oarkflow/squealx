package squealx

import (
	"context"
	"errors"
	"fmt"
	"iter"
)

// PreloadedCursor streams repository rows while assembling configured
// relations a batch at a time. Values are independent after Next advances;
// relation data for the active batch remains live until that batch is passed.
type PreloadedCursor[T any] struct {
	ctx        context.Context
	repo       *repository[T]
	source     *Cursor[T]
	batchSize  int
	batch      []T
	parents    []map[string]any
	position   int
	pendingErr error
	err        error
	closed     bool
}

// Keep common batch storage between reads, but do not let a caller-selected
// large batch permanently increase a cursor's retained heap.
const maxRetainedPreloadedRows = 500

// Next advances to the next preloaded parent row.
func (c *PreloadedCursor[T]) Next() bool {
	if c == nil || c.closed || c.err != nil {
		return false
	}
	if c.position < len(c.batch) {
		c.position++
		return true
	}
	if c.pendingErr != nil {
		c.err = c.pendingErr
		_ = c.Close()
		return false
	}
	if err := c.loadBatch(); err != nil {
		c.err = err
		_ = c.Close()
		return false
	}
	if len(c.batch) == 0 {
		_ = c.Close()
		return false
	}
	c.position = 1
	return true
}

func (c *PreloadedCursor[T]) loadBatch() error {
	clear(c.batch)
	if cap(c.batch) > maxRetainedPreloadedRows {
		c.batch = nil
	} else {
		c.batch = c.batch[:0]
	}
	if c.source == nil {
		return errors.New("squealx: nil preloaded cursor source")
	}
	if c.batchSize <= 0 {
		return errors.New("squealx: invalid preloaded cursor batch size")
	}
	if err := validatePreloadedCursorPool(c.repo.db, c.repo.preloadRelations); err != nil {
		return err
	}
	parents := c.parents[:0]
	if cap(parents) < c.batchSize {
		parents = make([]map[string]any, 0, c.batchSize)
	}
	defer func() {
		// preloadData and map cursors can keep these maps alive through c.batch.
		// Drop our scratch references after conversion and retain only the slice
		// backing array for the next batch when it remains reasonably sized.
		clear(parents)
		if cap(parents) <= maxRetainedPreloadedRows {
			c.parents = parents[:0]
		} else {
			c.parents = nil
		}
	}()
	for len(parents) < c.batchSize && c.source.Next() {
		parent, err := preloadedCursorValueMap(c.source.Copy())
		if err != nil {
			return fmt.Errorf("preload: conversion to map failed: %w", err)
		}
		parents = append(parents, parent)
	}
	streamErr := c.source.Err()
	if len(parents) == 0 {
		return streamErr
	}
	c.pendingErr = streamErr
	loaded, err := c.repo.preloadData(c.ctx, parents)
	if err != nil {
		return err
	}
	if cap(c.batch) < len(loaded) {
		c.batch = make([]T, 0, len(loaded))
	}
	for _, parent := range loaded {
		value, err := preloadedCursorMapValue[T](parent)
		if err != nil {
			return fmt.Errorf("preload: conversion from map failed: %w", err)
		}
		c.batch = append(c.batch, value)
	}
	return nil
}

func preloadedCursorValueMap[T any](value T) (map[string]any, error) {
	switch row := any(value).(type) {
	case map[string]any:
		return row, nil
	case *map[string]any:
		if row == nil {
			return nil, errors.New("nil map row")
		}
		return *row, nil
	default:
		return toMap(value)
	}
}

func preloadedCursorMapValue[T any](row map[string]any) (T, error) {
	var value T
	switch target := any(&value).(type) {
	case *map[string]any:
		normalizePreloadedMap(row)
		*target = row
		return value, nil
	case **map[string]any:
		normalizePreloadedMap(row)
		copyOfRow := row
		*target = &copyOfRow
		return value, nil
	default:
		return value, fromMap(row, &value)
	}
}

func normalizePreloadedMap(row map[string]any) {
	for key, value := range row {
		switch nested := value.(type) {
		case map[string]any:
			normalizePreloadedMap(nested)
		case []map[string]any:
			values := make([]any, len(nested))
			for i, item := range nested {
				normalizePreloadedMap(item)
				values[i] = item
			}
			row[key] = values
		case []any:
			for _, item := range nested {
				if itemMap, ok := item.(map[string]any); ok {
					normalizePreloadedMap(itemMap)
				}
			}
		}
	}
}

// Value returns the current parent row. It is valid after Next returns true
// and until the next Next call or Close.
func (c *PreloadedCursor[T]) Value() T {
	var zero T
	if c == nil || c.position == 0 || c.position > len(c.batch) {
		return zero
	}
	return c.batch[c.position-1]
}

// Each consumes the cursor and closes it on EOF or callback error. Preloaded
// row values are independent and may be retained after the callback returns.
func (c *PreloadedCursor[T]) Each(callback func(T) error) (returnErr error) {
	if callback == nil {
		return joinCloseError(errors.New("squealx: nil preloaded cursor callback"), c.Close())
	}
	defer func() {
		returnErr = joinCloseError(returnErr, c.Close())
	}()
	for c.Next() {
		if err := callback(c.Value()); err != nil {
			c.err = err
			return err
		}
	}
	return c.Err()
}

// All returns a range-over-func iterator and closes the cursor when iteration
// ends, including when the caller breaks early. Preloaded row values are
// independent and may be retained after each iteration.
func (c *PreloadedCursor[T]) All() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		if c == nil {
			var zero T
			yield(zero, ErrCursorClosed)
			return
		}
		defer c.Close()
		for c.Next() {
			if !yield(c.Value(), nil) {
				return
			}
		}
		if err := c.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

// Err returns the first query, conversion, or preload error.
func (c *PreloadedCursor[T]) Err() error {
	if c == nil {
		return ErrCursorClosed
	}
	if c.err != nil {
		return c.err
	}
	if c.pendingErr != nil {
		return nil
	}
	if c.source != nil {
		return c.source.Err()
	}
	return nil
}

// Close releases the source cursor and buffered relation batch.
func (c *PreloadedCursor[T]) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	clear(c.batch)
	c.batch = nil
	clear(c.parents)
	c.parents = nil
	c.position = 0
	c.ctx = nil
	c.repo = nil
	c.pendingErr = nil
	source := c.source
	c.source = nil
	if source != nil {
		if err := source.Close(); err != nil {
			c.err = joinCloseError(c.err, err)
			return err
		}
	}
	return nil
}
