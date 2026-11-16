package logdrop

import (
	"context"
	"fmt"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope of custom spans.
const ScopeName = "github.com/initialcodess/LogDrop-Go-Agent/logdrop"

const maxSpanName = 256

// Kind is the kind of a custom span.
type Kind int

// Span kinds.
const (
	// KindInternal is work inside the service (the default).
	KindInternal Kind = iota
	// KindServer handles a request from another service.
	KindServer
	// KindClient calls another service.
	KindClient
	// KindProducer sends a message.
	KindProducer
	// KindConsumer receives a message.
	KindConsumer
)

// SpanContext identifies a span.
type SpanContext struct {
	TraceID string
	SpanID  string
	Sampled bool
}

// SpanOption sets the kind or the links of a custom span.
type SpanOption func(*spanConfig)

type spanConfig struct {
	kind  Kind
	links []SpanContext
}

// WithKind sets the span's kind.
func WithKind(kind Kind) SpanOption { return func(c *spanConfig) { c.kind = kind } }

// WithLinks points at related spans by their context.
func WithLinks(links ...SpanContext) SpanOption {
	return func(c *spanConfig) { c.links = append(c.links, links...) }
}

// Span is a custom span. Every method is safe on a span that is not recorded (tracing does not run, or the name was
// invalid) and does nothing then.
type Span struct{ span trace.Span }

func otelKind(kind Kind) trace.SpanKind {
	switch kind {
	case KindServer:
		return trace.SpanKindServer
	case KindClient:
		return trace.SpanKindClient
	case KindProducer:
		return trace.SpanKindProducer
	case KindConsumer:
		return trace.SpanKindConsumer
	}
	return trace.SpanKindInternal
}

func otelLinks(links []SpanContext) []trace.Link {
	var out []trace.Link
	for _, l := range links {
		traceID, err1 := trace.TraceIDFromHex(l.TraceID)
		spanID, err2 := trace.SpanIDFromHex(l.SpanID)
		if err1 != nil || err2 != nil {
			continue
		}
		var flags trace.TraceFlags
		if l.Sampled {
			flags = trace.FlagsSampled
		}
		out = append(out, trace.Link{SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: flags, Remote: true})})
	}
	return out
}

// StartSpan starts a span as a child of the span in ctx and returns a context in which it is current: spans started
// with that context, also in other goroutines, are its children. End it with End, for example with defer. A blank
// name, or one longer than 256 characters, gives a span that is not recorded.
func StartSpan(ctx context.Context, name string, options ...SpanOption) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if core.IsBlank(name) || core.UTF16Len(name) > maxSpanName {
		return ctx, &Span{span: trace.SpanFromContext(context.Background())}
	}
	config := spanConfig{}
	for _, option := range options {
		option(&config)
	}
	ctx, span := otel.Tracer(ScopeName, trace.WithInstrumentationVersion(Version)).Start(ctx, name,
		trace.WithSpanKind(otelKind(config.kind)), trace.WithLinks(otelLinks(config.links)...))
	return ctx, &Span{span: span}
}

// InSpan runs work in a new span and ends it when work returns. An error that work returns, or a panic, is recorded
// with its type only and passed on.
func InSpan(ctx context.Context, name string, work func(ctx context.Context, span *Span) error, options ...SpanOption) (err error) {
	ctx, span := StartSpan(ctx, name, options...)
	defer span.End()
	defer func() {
		if recovered := recover(); recovered != nil {
			span.recordType(fmt.Sprintf("%T", recovered))
			panic(recovered)
		}
	}()
	if err = work(ctx, span); err != nil {
		span.RecordError(err)
	}
	return err
}

// Attribute sets a custom attribute. It is sent only when its key is listed in LOGDROP_ALLOWED_ATTRIBUTE_KEYS. value
// is a string, a bool, an integer or a float; other values are ignored.
func (s *Span) Attribute(key string, value any) *Span {
	k := attribute.Key(key)
	switch v := value.(type) {
	case string:
		s.span.SetAttributes(k.String(v))
	case bool:
		s.span.SetAttributes(k.Bool(v))
	case int:
		s.span.SetAttributes(k.Int(v))
	case int8:
		s.span.SetAttributes(k.Int64(int64(v)))
	case int16:
		s.span.SetAttributes(k.Int64(int64(v)))
	case int32:
		s.span.SetAttributes(k.Int64(int64(v)))
	case int64:
		s.span.SetAttributes(k.Int64(v))
	case uint8:
		s.span.SetAttributes(k.Int64(int64(v)))
	case uint16:
		s.span.SetAttributes(k.Int64(int64(v)))
	case uint32:
		s.span.SetAttributes(k.Int64(int64(v)))
	case float32:
		s.span.SetAttributes(k.Float64(float64(v)))
	case float64:
		s.span.SetAttributes(k.Float64(v))
	}
	return s
}

// Event adds an event. Use constant names, without request or user data.
func (s *Span) Event(name string) *Span {
	if !core.IsBlank(name) {
		s.span.AddEvent(name)
	}
	return s
}

// Success marks the span as successful.
func (s *Span) Success() *Span {
	s.span.SetStatus(codes.Ok, "")
	return s
}

// Error marks the span as failed, without an error value.
func (s *Span) Error() *Span {
	s.span.SetStatus(codes.Error, "")
	return s
}

// RecordError marks the span as failed and records the error's type (for example *fs.PathError), never its message.
func (s *Span) RecordError(err error) *Span {
	if err != nil {
		s.recordType(fmt.Sprintf("%T", err))
	}
	return s
}

func (s *Span) recordType(typeName string) {
	s.span.SetStatus(codes.Error, "")
	s.span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", typeName)))
}

// End ends the span; it is sent with the next batch. Calling it again does nothing.
func (s *Span) End() { s.span.End() }

// Context returns the span's trace and span ID.
func (s *Span) Context() SpanContext { return contextOf(s.span.SpanContext()) }

func contextOf(c trace.SpanContext) SpanContext {
	return SpanContext{TraceID: c.TraceID().String(), SpanID: c.SpanID().String(), Sampled: c.IsSampled()}
}

// CurrentSpanContext returns the trace and span ID of the span in ctx, and false when there is none.
func CurrentSpanContext(ctx context.Context) (SpanContext, bool) {
	c := trace.SpanContextFromContext(ctx)
	if !c.IsValid() {
		return SpanContext{}, false
	}
	return contextOf(c), true
}
