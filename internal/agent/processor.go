package agent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/export"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Processor is the only span processor of the agent, in the Java agent's order: admission bookkeeping, the kubelet
// probe filter (traces started by User-Agent kube-probe/ are dropped with their whole local trace), dependency naming,
// and the privacy snapshot BEFORE the bounded queue. The SDK's span itself is never queued or exported.
type Processor struct {
	admission *Admission
	limits    core.TracingLimits
	queue     *export.Queue
	mu        sync.Mutex
	// probes maps the trace of a probe request to its local root span, while that root is open.
	probes map[trace.TraceID]trace.SpanID
}

// NewProcessor sends sanitized spans to queue.
func NewProcessor(admission *Admission, limits core.TracingLimits, queue *export.Queue) *Processor {
	return &Processor{admission: admission, limits: limits, queue: queue, probes: map[trace.TraceID]trace.SpanID{}}
}

func userAgent(attributes []attribute.KeyValue) string {
	for _, kv := range attributes {
		if (kv.Key == "user_agent.original" || kv.Key == "http.user_agent") && kv.Value.Type() == attribute.STRING {
			return kv.Value.AsString()
		}
	}
	return ""
}

// isProbeRoot reports the local root server span of a kubelet probe request. The Go HTTP instrumentations set the
// request's attributes when the span starts.
func isProbeRoot(span sdktrace.ReadOnlySpan) bool {
	if span.SpanKind() != trace.SpanKindServer {
		return false
	}
	if parent := span.Parent(); parent.SpanID().IsValid() && !parent.IsRemote() {
		return false
	}
	return strings.HasPrefix(userAgent(span.Attributes()), "kube-probe/")
}

// OnStart records the span for admission and marks probe traces.
func (p *Processor) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	c := span.SpanContext()
	p.admission.Started(c)
	if isProbeRoot(span) {
		p.mu.Lock()
		p.probes[c.TraceID()] = c.SpanID()
		p.mu.Unlock()
	}
}

// OnEnd queues the sanitized copy of the span, unless it belongs to a probe trace.
func (p *Processor) OnEnd(span sdktrace.ReadOnlySpan) {
	c := span.SpanContext()
	defer p.admission.Ended(c)
	p.mu.Lock()
	root, probe := p.probes[c.TraceID()]
	if probe && root == c.SpanID() {
		delete(p.probes, c.TraceID())
	}
	p.mu.Unlock()
	if probe || isProbeRoot(span) {
		return
	}
	values := map[string]attribute.Value{}
	for _, kv := range span.Attributes() {
		values[string(kv.Key)] = kv.Value
	}
	name := ApplyNaming(span.Name(), values)
	attributes := make([]attribute.KeyValue, 0, len(values))
	for key, value := range values {
		attributes = append(attributes, attribute.KeyValue{Key: attribute.Key(key), Value: value})
	}
	p.queue.Add(core.Snapshot(span, p.limits, name, attributes))
}

// ForceFlush sends what is queued.
func (p *Processor) ForceFlush(ctx context.Context) error {
	timeout := time.Minute
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	if !p.queue.Flush(timeout) {
		return context.DeadlineExceeded
	}
	return nil
}

// Shutdown sends what is queued, for at most 10 s, and stops the exporter.
func (p *Processor) Shutdown(context.Context) error {
	p.queue.Shutdown(0)
	return nil
}
