package agent

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// EvictionInterval is how often abandoned spans are looked for while the limit is reached.
const EvictionInterval = time.Second

type spanKey struct {
	trace trace.TraceID
	span  trace.SpanID
}

// Admission is always-on sampling with an explicit overload refusal, like the Java core's ActiveSpanAdmission: never
// percentage or parent-flag-based sampling. With a maximum age, a span that never ends (an instrumentation or
// application leak) stops counting against the limit after that age, so leaks cannot switch tracing off for good. It
// is still exported if it ends later.
type Admission struct {
	limit        int
	maxAge       time.Duration
	onRefusal    func()
	clock        func() time.Duration
	mu           sync.Mutex
	started      map[spanKey]time.Duration
	active       int
	lastEviction *time.Duration
	refused      atomic.Int64
	abandoned    atomic.Int64
}

// NewAdmission admits at most limit open spans.
func NewAdmission(limit int, maxAge time.Duration, onRefusal func(), clock func() time.Duration) *Admission {
	if clock == nil {
		clock = diag.Monotonic
	}
	return &Admission{limit: limit, maxAge: maxAge, onRefusal: onRefusal, clock: clock, started: map[spanKey]time.Duration{}}
}

// Active counts open admitted spans.
func (a *Admission) Active() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active
}

// Refused counts spans not recorded because the limit was reached.
func (a *Admission) Refused() int64 { return a.refused.Load() }

// Abandoned counts spans that stopped counting because they never ended.
func (a *Admission) Abandoned() int64 { return a.abandoned.Load() }

// ShouldSample admits a span while fewer than the limit are open.
func (a *Admission) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	state := trace.SpanContextFromContext(p.ParentContext).TraceState()
	a.mu.Lock()
	admitted := a.active < a.limit || (a.maxAge > 0 && a.evictAbandoned())
	if admitted {
		a.active++
	}
	a.mu.Unlock()
	if !admitted {
		a.refused.Add(1)
		a.onRefusal()
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: state}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
}

// Description names the sampler.
func (a *Admission) Description() string {
	return fmt.Sprintf("LogDropAlwaysOnWithCapacityLimit(%d)", a.limit)
}

// Started records the start of an admitted span.
func (a *Admission) Started(c trace.SpanContext) {
	if a.maxAge <= 0 {
		return
	}
	a.mu.Lock()
	a.started[spanKey{c.TraceID(), c.SpanID()}] = a.clock()
	a.mu.Unlock()
}

// Ended frees the slot of an admitted span (unless it was already counted as abandoned).
func (a *Admission) Ended(c trace.SpanContext) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := spanKey{c.TraceID(), c.SpanID()}
	if a.maxAge <= 0 {
		a.active--
		return
	}
	if _, ok := a.started[key]; ok {
		delete(a.started, key)
		a.active--
	}
}

func (a *Admission) evictAbandoned() bool {
	now := a.clock()
	if a.lastEviction != nil && now-*a.lastEviction < EvictionInterval {
		return false
	}
	a.lastEviction = &now
	released := false
	for key, started := range a.started {
		if now-started < a.maxAge {
			continue
		}
		delete(a.started, key)
		a.active--
		a.abandoned.Add(1)
		released = true
	}
	return released
}
