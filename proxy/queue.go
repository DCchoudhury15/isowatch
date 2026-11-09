package proxy

import "sync"

// queue holds the client messages PostgreSQL has not answered yet. It has
// no size limit: the client decides how far it pipelines, and a limit here
// could stop the proxy from forwarding the Sync that PostgreSQL waits for.
type queue struct {
	mu    sync.Mutex
	items []*entry
	head  int
}

func (q *queue) push(e *entry) {
	q.mu.Lock()
	q.items = append(q.items, e)
	q.mu.Unlock()
}

func (q *queue) peek() *entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.items) {
		return nil
	}
	return q.items[q.head]
}

func (q *queue) pop() *entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.items) {
		return nil
	}
	e := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	switch {
	case q.head == len(q.items):
		q.items, q.head = q.items[:0], 0
	case q.head > 1024 && q.head*2 > len(q.items):
		n := copy(q.items, q.items[q.head:])
		q.items, q.head = q.items[:n], 0
	}
	return e
}
