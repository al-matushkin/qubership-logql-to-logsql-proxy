// Package limits provides the concurrency semaphore used to protect the proxy
// from resource exhaustion under high load.
package limits

import (
	"context"
	"errors"
)

// ErrQueueFull is returned by Acquire when both the active-slot semaphore and
// the waiting queue are at capacity. The caller should respond with HTTP 429.
var ErrQueueFull = errors.New("too many requests queued; try again later")

// Limiter is a context-aware concurrency limiter backed by two buffered
// channels:
//   - active: a semaphore of size maxConcurrent
//   - queue:  a holding area of size maxQueueDepth for requests waiting for a slot
//
// This ensures at most maxConcurrent requests call VictoriaLogs simultaneously,
// while up to maxQueueDepth additional requests may wait. Beyond that, callers
// receive ErrQueueFull without blocking.
type Limiter struct {
	active chan struct{}
	queue  chan struct{}
}

// New creates a Limiter. maxConcurrent must be ≥ 1; maxQueueDepth may be 0
// (in which case requests are rejected immediately when all slots are busy).
func New(maxConcurrent, maxQueueDepth int) *Limiter {
	return &Limiter{
		active: make(chan struct{}, maxConcurrent),
		queue:  make(chan struct{}, maxQueueDepth),
	}
}

// Acquire blocks until a concurrency slot is available or ctx is cancelled.
//
//   - Fast path: if an active slot is free it is taken immediately.
//   - Slow path: the caller joins the queue, then waits for an active slot.
//   - If the queue is also full, ErrQueueFull is returned without blocking.
//   - If ctx is cancelled while waiting in the queue, the queue position is
//     released and ctx.Err() is returned without consuming an active slot.
func (l *Limiter) Acquire(ctx context.Context) error {
	// Fast path: grab an active slot without touching the queue.
	select {
	case l.active <- struct{}{}:
		return nil
	default:
	}

	// Slow path: try to join the waiting queue.
	select {
	case l.queue <- struct{}{}:
	default:
		return ErrQueueFull
	}

	// Wait for an active slot or context cancellation.
	select {
	case <-ctx.Done():
		<-l.queue // leave the queue; no active slot consumed
		return ctx.Err()
	case l.active <- struct{}{}:
		<-l.queue // moved from queue to active
		return nil
	}
}

// Release frees one active slot, allowing a goroutine blocked in Acquire to
// proceed. It must be called exactly once for each successful Acquire, typically
// via defer.
func (l *Limiter) Release() {
	<-l.active
}

// ActiveCount returns the current number of goroutines holding an active slot.
func (l *Limiter) ActiveCount() int { return len(l.active) }

// QueuedCount returns the current number of goroutines waiting in the queue.
func (l *Limiter) QueuedCount() int { return len(l.queue) }
