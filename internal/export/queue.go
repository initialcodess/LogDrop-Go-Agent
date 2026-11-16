package export

import (
	"sync"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
)

// ShutdownBudget is the most shutdown spends sending what is queued.
const ShutdownBudget = 10 * time.Second

// Queue is the bounded memory queue and its worker goroutine, with the Java agent's batching rules: new spans are
// dropped while the queue is full, one batch is sent at a time (a batch waiting for a retry keeps its place, so
// batches keep their order), flush has a timeout, and shutdown spends at most 10 s.
type Queue struct {
	exporter       *Exporter
	maxQueue       int
	batchSize      int
	delay          time.Duration
	mu             sync.Mutex
	spans          []core.SpanData
	wake           chan struct{}
	flushRequested int64
	flushed        int64
	flushedSignal  chan struct{}
	shutdown       bool
	done           chan struct{}
	dropped        int64
}

// NewQueue starts the worker.
func NewQueue(exporter *Exporter, maxQueueSize, maxExportBatchSize int, scheduleDelay time.Duration) *Queue {
	q := &Queue{exporter: exporter, maxQueue: maxQueueSize, batchSize: maxExportBatchSize, delay: scheduleDelay,
		wake: make(chan struct{}, 1), flushedSignal: make(chan struct{}), done: make(chan struct{})}
	go q.run()
	return q
}

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Add queues one span, or drops it when the queue is full or shut down.
func (q *Queue) Add(span core.SpanData) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown {
		return
	}
	if len(q.spans) >= q.maxQueue {
		q.dropped++
		return
	}
	q.spans = append(q.spans, span)
	if len(q.spans) >= q.batchSize {
		q.signal()
	}
}

// Dropped counts spans dropped because the queue was full.
func (q *Queue) Dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// IsShutdown reports a closed queue.
func (q *Queue) IsShutdown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shutdown
}

// Pending counts queued spans.
func (q *Queue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.spans)
}

func (q *Queue) run() {
	defer close(q.done)
	for {
		deadline := time.NewTimer(q.delay)
		for {
			q.mu.Lock()
			idle := !q.shutdown && q.flushRequested == q.flushed && len(q.spans) < q.batchSize
			q.mu.Unlock()
			if !idle {
				break
			}
			select {
			case <-q.wake:
				continue
			case <-deadline.C:
			}
			break
		}
		deadline.Stop()
		q.mu.Lock()
		target := q.flushRequested
		stopping := q.shutdown
		// A flush or shutdown sends everything queued now; otherwise full batches, or one after the delay.
		drain := stopping || target != q.flushed
		q.mu.Unlock()
		q.send(drain)
		q.mu.Lock()
		q.flushed = target
		close(q.flushedSignal)
		q.flushedSignal = make(chan struct{})
		empty := len(q.spans) == 0
		q.mu.Unlock()
		if stopping && empty {
			return
		}
	}
}

func (q *Queue) send(drain bool) {
	first := true
	for {
		q.mu.Lock()
		if len(q.spans) == 0 || (!drain && !first && len(q.spans) < q.batchSize) {
			q.mu.Unlock()
			return
		}
		count := min(q.batchSize, len(q.spans))
		batch := make([]core.SpanData, count)
		copy(batch, q.spans[:count])
		q.spans = append(q.spans[:0:0], q.spans[count:]...)
		q.mu.Unlock()
		first = false
		q.exportSafely(batch)
	}
}

func (q *Queue) exportSafely(batch []core.SpanData) {
	defer func() { _ = recover() }() // the worker must keep running
	q.exporter.Export(batch)
}

// Flush sends the spans queued before the call; false when that did not finish in time.
func (q *Queue) Flush(timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return false
	}
	q.flushRequested++
	target := q.flushRequested
	q.signal()
	for q.flushed < target {
		signal := q.flushedSignal
		q.mu.Unlock()
		select {
		case <-signal:
		case <-deadline.C:
			return false
		}
		q.mu.Lock()
	}
	q.mu.Unlock()
	return true
}

// Shutdown sends what is queued, for at most budget (ShutdownBudget when 0), then closes the exporter.
func (q *Queue) Shutdown(budget time.Duration) {
	if budget <= 0 {
		budget = ShutdownBudget
	}
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return
	}
	q.shutdown = true
	q.signal()
	q.mu.Unlock()
	q.exporter.Closing()
	select {
	case <-q.done:
	case <-time.After(budget):
	}
	q.exporter.Close()
}
