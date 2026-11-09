package isowatchprocessor

import (
	"context"
	"sync"
	"sync/atomic"
)

// worker runs slow work (HTTP calls, database writes) on its own goroutine
// so the pipeline never waits for it. Items go into a fixed size queue.
// When the queue is full, offer returns false and the item is dropped.
type worker[T any] struct {
	items   chan T
	stop    chan struct{}
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	handle  func(context.Context, T)
	once    sync.Once
	started atomic.Bool
}

func newWorker[T any](size int, handle func(context.Context, T)) *worker[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &worker[T]{
		items:  make(chan T, size),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
		handle: handle,
	}
}

func (w *worker[T]) start() {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer close(w.done)
		for {
			select {
			case <-w.stop:
				// Finish what is queued, unless shutdown runs out of time
				// and cancels w.ctx.
				for {
					select {
					case it := <-w.items:
						if w.ctx.Err() == nil {
							w.handle(w.ctx, it)
						}
					default:
						return
					}
				}
			case it := <-w.items:
				w.handle(w.ctx, it)
			}
		}
	}()
}

// offer queues an item without blocking. It returns false if the queue is
// full.
func (w *worker[T]) offer(it T) bool {
	select {
	case w.items <- it:
		return true
	default:
		return false
	}
}

// shutdown stops the worker after the queued items are handled, or when ctx
// ends, whichever comes first. It is safe to call more than once.
func (w *worker[T]) shutdown(ctx context.Context) {
	w.once.Do(func() { close(w.stop) })
	if !w.started.Load() {
		w.cancel()
		return
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		w.cancel()
		<-w.done
	}
	w.cancel()
}
