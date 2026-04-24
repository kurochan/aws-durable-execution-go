package durable

import (
	"context"
	"sync"
	"sync/atomic"
)

type Future[T any] struct {
	once     sync.Once
	done     chan struct{}
	exec     func(context.Context) (T, error)
	result   T
	err      error
	executed atomic.Bool
}

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

func (f *Future[T]) Executed() bool {
	return f.executed.Load()
}

func NewResolvedFuture[T any](value T) *Future[T] {
	return NewFuture(func(context.Context) (T, error) { return value, nil })
}

func NewRejectedFuture[T any](err error) *Future[T] {
	return NewFuture(func(context.Context) (T, error) {
		var zero T
		return zero, err
	})
}

func NewNeverFuture[T any]() *Future[T] {
	return NewFuture(func(ctx context.Context) (T, error) {
		<-ctx.Done()
		var zero T
		return zero, ctx.Err()
	})
}
