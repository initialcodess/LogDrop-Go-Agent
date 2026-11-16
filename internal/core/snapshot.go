package core

import (
	"math"
	"sort"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// DBQueryText carries the value-free SQL template of a SQL span.
const DBQueryText = "db.query.text"

// SafeAttribute is an attribute that may leave the process. Value is a string, an int64, a finite float64 or a bool.
type SafeAttribute struct {
	Key   string
	Value any
}

// SpanEvent is a sanitized span event.
type SpanEvent struct {
	Name         string
	TimeUnixNano int64
	Attributes   []SafeAttribute
}

// SpanLink is a sanitized span link.
type SpanLink struct {
	TraceID    trace.TraceID
	SpanID     trace.SpanID
	TraceState string
	Flags      byte
	IsRemote   bool
	Attributes []SafeAttribute
}

// SpanData is a sanitized copy of a finished span, made BEFORE the batch queue, the same as the Java core's
// PrivacySpanProcessor snapshot. Only copied values are kept; the span itself is not referenced. The status description
// is never kept: it can carry exception text, so only the code leaves the process.
type SpanData struct {
	Name           string
	Kind           trace.SpanKind
	TraceID        trace.TraceID
	SpanID         trace.SpanID
	ParentSpanID   trace.SpanID
	HasParent      bool
	ParentIsRemote bool
	TraceState     string
	Flags          byte
	StartUnixNano  int64
	EndUnixNano    int64
	// StatusCode is the OTLP status code: 0 unset, 1 ok, 2 error.
	StatusCode   int
	Attributes   []SafeAttribute
	Events       []SpanEvent
	Links        []SpanLink
	ScopeName    string
	ScopeVersion string
}

// SafeValue is the value of one attribute that may leave the process, and false when it must not. Like the Java core,
// only strings (bounded), whole numbers, finite floating-point numbers and booleans pass.
func SafeValue(value attribute.Value, limits TracingLimits) (any, bool) {
	switch value.Type() {
	case attribute.STRING:
		return limits.Bounded(value.AsString()), true
	case attribute.BOOL:
		return value.AsBool(), true
	case attribute.INT64:
		return value.AsInt64(), true
	case attribute.FLOAT64:
		f := value.AsFloat64()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		return f, true
	}
	return nil, false
}

func latest(attributes []attribute.KeyValue) map[string]attribute.Value {
	values := make(map[string]attribute.Value, len(attributes))
	for _, kv := range attributes {
		values[string(kv.Key)] = kv.Value
	}
	return values
}

// Sanitize keeps allowlisted attributes up to the limit, keys in ordinal order (which keys survive past the limit is
// not part of the shared contract).
func Sanitize(attributes []attribute.KeyValue, limits TracingLimits) []SafeAttribute {
	return sanitize(latest(attributes), limits)
}

func sanitize(values map[string]attribute.Value, limits TracingLimits) []SafeAttribute {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	safe := []SafeAttribute{}
	for _, key := range keys {
		if len(safe) >= limits.MaxAttributes {
			break
		}
		if !IsExportable(key, limits.AllowedKeys) {
			continue
		}
		if value, ok := SafeValue(values[key], limits); ok {
			safe = append(safe, SafeAttribute{key, value})
		}
	}
	return safe
}

func firstString(values map[string]attribute.Value, keys ...string) string {
	for _, key := range keys {
		if v, ok := values[key]; ok && v.Type() == attribute.STRING && !IsBlank(v.AsString()) {
			return v.AsString()
		}
	}
	return ""
}

// SanitizeSpan is Sanitize plus the value-free SQL template of a SQL span as db.query.text. Raw db.statement and
// db.query.text never pass; only the normalizer output does, and only for SQL systems.
func SanitizeSpan(attributes []attribute.KeyValue, limits TracingLimits) []SafeAttribute {
	values := latest(attributes)
	safe := sanitize(values, limits)
	system := firstString(values, "db.system.name", "db.system")
	if system == "" || !IsSQLSystem(system) || len(safe) >= limits.MaxAttributes {
		return safe
	}
	statement := firstString(values, DBQueryText, "db.statement")
	if statement == "" {
		return safe
	}
	if template, ok := NormalizeSQL(statement, system); ok {
		safe = append(safe, SafeAttribute{DBQueryText, limits.Bounded(template)})
	}
	return safe
}

const scopeMax = 128

func take(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return UTF16Prefix(value, max)
}

// StatusCode is the OTLP status code of a finished span.
func StatusCode(code codes.Code) int {
	switch code {
	case codes.Error:
		return 2
	case codes.Ok:
		return 1
	}
	return 0
}

// Snapshot is a sanitized copy of a finished span. name and attributes are the span's own, or renamed ones (see
// the dependency naming).
func Snapshot(span sdktrace.ReadOnlySpan, limits TracingLimits, name string, attributes []attribute.KeyValue) SpanData {
	events := []SpanEvent{}
	for _, e := range span.Events() {
		if len(events) >= limits.MaxEvents {
			break
		}
		events = append(events, SpanEvent{limits.Bounded(e.Name), e.Time.UnixNano(), Sanitize(e.Attributes, limits)})
	}
	links := []SpanLink{}
	for _, l := range span.Links() {
		if len(links) >= limits.MaxLinks {
			break
		}
		c := l.SpanContext
		links = append(links, SpanLink{c.TraceID(), c.SpanID(), c.TraceState().String(), byte(c.TraceFlags()),
			c.IsRemote(), Sanitize(l.Attributes, limits)})
	}
	context, parent := span.SpanContext(), span.Parent()
	scope := span.InstrumentationScope()
	return SpanData{
		Name:           limits.Bounded(name),
		Kind:           span.SpanKind(),
		TraceID:        context.TraceID(),
		SpanID:         context.SpanID(),
		ParentSpanID:   parent.SpanID(),
		HasParent:      parent.SpanID().IsValid(),
		ParentIsRemote: parent.SpanID().IsValid() && parent.IsRemote(),
		TraceState:     context.TraceState().String(),
		Flags:          byte(context.TraceFlags()),
		StartUnixNano:  span.StartTime().UnixNano(),
		EndUnixNano:    span.EndTime().UnixNano(),
		StatusCode:     StatusCode(span.Status().Code),
		Attributes:     SanitizeSpan(attributes, limits),
		Events:         events,
		Links:          links,
		ScopeName:      take(scope.Name, scopeMax),
		ScopeVersion:   take(scope.Version, scopeMax),
	}
}
