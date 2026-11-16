package export

import (
	"encoding/binary"
	"math"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
)

// The OTLP ExportTraceServiceRequest as protobuf bytes, written by hand like the Python and .NET agents' encoders, so
// the library does not bring protobuf and gRPC into the service's dependencies: only the fields LogDrop reads, in the
// field order of opentelemetry-proto's trace.proto. Span and link flags carry the W3C trace flags in their low 8 bits
// and the remote-parent bits, like the Java and Node.js agents.

const (
	hasIsRemote = 0x100
	isRemote    = 0x200
)

type writer struct{ buf []byte }

func (w *writer) varint(v uint64) { w.buf = binary.AppendUvarint(w.buf, v) }

func (w *writer) tag(field, wire int) { w.varint(uint64(field<<3 | wire)) }

func (w *writer) bytes(field int, value []byte) {
	w.tag(field, 2)
	w.varint(uint64(len(value)))
	w.buf = append(w.buf, value...)
}

func (w *writer) message(field int, nested *writer) { w.bytes(field, nested.buf) }

func (w *writer) str(field int, value string) {
	if value != "" {
		w.bytes(field, []byte(value))
	}
}

func (w *writer) uint(field int, value uint64) {
	if value != 0 {
		w.tag(field, 0)
		w.varint(value)
	}
}

func (w *writer) fixed64(field int, value uint64) {
	if value != 0 {
		w.tag(field, 1)
		w.buf = binary.LittleEndian.AppendUint64(w.buf, value)
	}
}

// double is written even when it is 0: a oneof member has presence.
func (w *writer) double(field int, value float64) {
	w.tag(field, 1)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(value))
}

func (w *writer) fixed32(field int, value uint32) {
	if value != 0 {
		w.tag(field, 5)
		w.buf = binary.LittleEndian.AppendUint32(w.buf, value)
	}
}

func anyValue(value any) *writer {
	w := &writer{}
	switch v := value.(type) {
	case string:
		w.bytes(1, []byte(v))
	case bool:
		w.tag(2, 0)
		if v {
			w.varint(1)
		} else {
			w.varint(0)
		}
	case int64:
		w.tag(3, 0)
		w.varint(uint64(v))
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		w.double(4, v)
	default:
		return nil
	}
	return w
}

func attributes(w *writer, field int, list []core.SafeAttribute) {
	for _, a := range list {
		value := anyValue(a.Value)
		if value == nil {
			continue
		}
		kv := &writer{}
		kv.str(1, a.Key)
		kv.message(2, value)
		w.message(field, kv)
	}
}

func flags(traceFlags byte, remote bool) uint32 {
	f := uint32(traceFlags) | hasIsRemote
	if remote {
		f |= isRemote
	}
	return f
}

func nonNegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func spanMessage(span *core.SpanData) *writer {
	w := &writer{}
	w.bytes(1, span.TraceID[:])
	w.bytes(2, span.SpanID[:])
	w.str(3, span.TraceState)
	if span.HasParent {
		w.bytes(4, span.ParentSpanID[:])
	}
	w.str(5, span.Name)
	w.uint(6, uint64(span.Kind))
	w.fixed64(7, nonNegative(span.StartUnixNano))
	w.fixed64(8, nonNegative(span.EndUnixNano))
	attributes(w, 9, span.Attributes)
	for _, e := range span.Events {
		ew := &writer{}
		ew.fixed64(1, nonNegative(e.TimeUnixNano))
		ew.str(2, e.Name)
		attributes(ew, 3, e.Attributes)
		w.message(11, ew)
	}
	for _, l := range span.Links {
		lw := &writer{}
		lw.bytes(1, l.TraceID[:])
		lw.bytes(2, l.SpanID[:])
		lw.str(3, l.TraceState)
		attributes(lw, 4, l.Attributes)
		lw.fixed32(6, flags(l.Flags, l.IsRemote))
		w.message(13, lw)
	}
	status := &writer{}
	status.uint(3, uint64(span.StatusCode))
	w.message(15, status)
	w.fixed32(16, flags(span.Flags, span.ParentIsRemote))
	return w
}

// Encode returns one request for spans, all with the same resource, grouped by instrumentation scope in first-seen
// order.
func Encode(spans []core.SpanData, resource []core.SafeAttribute) []byte {
	type scope struct {
		name, version string
		spans         []*core.SpanData
	}
	var scopes []*scope
	for i := range spans {
		span := &spans[i]
		var found *scope
		for _, s := range scopes {
			if s.name == span.ScopeName && s.version == span.ScopeVersion {
				found = s
				break
			}
		}
		if found == nil {
			found = &scope{name: span.ScopeName, version: span.ScopeVersion}
			scopes = append(scopes, found)
		}
		found.spans = append(found.spans, span)
	}
	resourceSpans := &writer{}
	resourceWriter := &writer{}
	attributes(resourceWriter, 1, resource)
	resourceSpans.message(1, resourceWriter)
	for _, s := range scopes {
		scopeSpans := &writer{}
		scopeWriter := &writer{}
		scopeWriter.str(1, s.name)
		scopeWriter.str(2, s.version)
		scopeSpans.message(1, scopeWriter)
		for _, span := range s.spans {
			scopeSpans.message(2, spanMessage(span))
		}
		resourceSpans.message(2, scopeSpans)
	}
	request := &writer{}
	request.message(1, resourceSpans)
	return request.buf
}
