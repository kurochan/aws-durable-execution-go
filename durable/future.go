package durable

import (
	"context"
	"sync"
	"sync/atomic"
)

// Future represents a durable operation whose result is produced when Await is
// called.
//
// DurableContext methods return futures so the SDK can checkpoint an operation
// before the caller decides to wait for it. A Future starts at most once.
type Future[T any] struct {
	once     sync.Once
	done     chan struct{}
	exec     func(context.Context) (T, error)
	result   T
	err      error
	executed atomic.Bool
}

// NewFuture creates a Future backed by exec.
//
// exec is invoked once, lazily, by the first Await call.
func NewFuture[T any](exec func(context.Context) (T, error)) *Future[T] {
	return &Future[T]{
		done: make(chan struct{}),
		exec: exec,
	}
}

func (f *Future[T]) start(ctx context.Context) {
	f.once.Do(func() {
		f.executed.Store(true)
		go func() {
			defer close(f.done)
			f.result, f.err = f.exec(ctx)
		}()
	})
}

// Await starts the future if needed and waits for its result.
//
// If ctx is canceled before the future completes, Await returns ctx.Err().
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	f.start(ctx)
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-f.done:
		return f.result, f.err
	}
}

// Executed reports whether the future has been started by Await.
func (f *Future[T]) Executed() bool {
	return f.executed.Load()
}

// NewResolvedFuture returns a Future that resolves to value.
func NewResolvedFuture[T any](value T) *Future[T] {
	return NewFuture(func(context.Context) (T, error) { return value, nil })
}

// NewRejectedFuture returns a Future that fails with err.
func NewRejectedFuture[T any](err error) *Future[T] {
	return NewFuture(func(context.Context) (T, error) {
		var zero T
		return zero, err
	})
}

// NewNeverFuture returns a Future that only completes when its context is
// canceled.
//
// It is used during replay paths where a previously succeeded child context
// should not execute operations that were not part of the checkpointed result.
func NewNeverFuture[T any]() *Future[T] {
	return NewFuture(func(ctx context.Context) (T, error) {
		<-ctx.Done()
		var zero T
		return zero, ctx.Err()
	})
}
