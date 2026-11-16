// Package otlptest decodes the OTLP trace requests the agent sends, for tests: only the fields the encoder writes.
package otlptest

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
)

// Span is a decoded span.
type Span struct {
	TraceID, SpanID, ParentSpanID, TraceState, Name string
	Kind                                            int
	Start, End                                      uint64
	Attributes                                      map[string]any
	Events                                          []Event
	Links                                           []Link
	StatusCode                                      int
	Flags                                           uint32
	Scope, ScopeVersion                             string
}

// Event is a decoded span event.
type Event struct {
	Name       string
	Attributes map[string]any
}

// Link is a decoded span link.
type Link struct {
	TraceID, SpanID string
	Attributes      map[string]any
	Flags           uint32
}

// Request is a decoded ExportTraceServiceRequest with one resource.
type Request struct {
	Resource map[string]any
	Spans    []Span
}

type field struct {
	number int
	wire   int
	varint uint64
	bytes  []byte
}

func fields(b []byte) ([]field, error) {
	var out []field
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("bad tag")
		}
		b = b[n:]
		f := field{number: int(tag >> 3), wire: int(tag & 7)}
		switch f.wire {
		case 0:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, fmt.Errorf("bad varint")
			}
			f.varint, b = v, b[n:]
		case 1:
			f.varint, b = binary.LittleEndian.Uint64(b), b[8:]
		case 5:
			f.varint, b = uint64(binary.LittleEndian.Uint32(b)), b[4:]
		case 2:
			l, n := binary.Uvarint(b)
			if n <= 0 || int(l) > len(b)-n {
				return nil, fmt.Errorf("bad length")
			}
			f.bytes, b = b[n:n+int(l)], b[n+int(l):]
		default:
			return nil, fmt.Errorf("wire type %d", f.wire)
		}
		out = append(out, f)
	}
	return out, nil
}

func must(b []byte) []field {
	f, err := fields(b)
	if err != nil {
		panic(err)
	}
	return f
}

func anyValue(b []byte) any {
	for _, f := range must(b) {
		switch f.number {
		case 1:
			return string(f.bytes)
		case 2:
			return f.varint != 0
		case 3:
			return int64(f.varint)
		case 4:
			return math.Float64frombits(f.varint)
		}
	}
	return nil
}

func keyValue(b []byte, into map[string]any) {
	var key string
	var value any
	for _, f := range must(b) {
		if f.number == 1 {
			key = string(f.bytes)
		} else if f.number == 2 {
			value = anyValue(f.bytes)
		}
	}
	into[key] = value
}

// Decode decodes body, gunzipping it first when it is compressed.
func Decode(body []byte) (Request, error) {
	if len(body) > 2 && body[0] == 0x1f && body[1] == 0x8b {
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return Request{}, err
		}
		if body, err = io.ReadAll(r); err != nil {
			return Request{}, err
		}
	}
	request := Request{Resource: map[string]any{}}
	top, err := fields(body)
	if err != nil {
		return request, err
	}
	for _, rs := range top {
		for _, f := range must(rs.bytes) {
			switch f.number {
			case 1:
				for _, a := range must(f.bytes) {
					keyValue(a.bytes, request.Resource)
				}
			case 2:
				var scope, version string
				var spans [][]byte
				for _, s := range must(f.bytes) {
					if s.number == 1 {
						for _, sf := range must(s.bytes) {
							if sf.number == 1 {
								scope = string(sf.bytes)
							} else if sf.number == 2 {
								version = string(sf.bytes)
							}
						}
					} else if s.number == 2 {
						spans = append(spans, s.bytes)
					}
				}
				for _, b := range spans {
					span := decodeSpan(b)
					span.Scope, span.ScopeVersion = scope, version
					request.Spans = append(request.Spans, span)
				}
			}
		}
	}
	return request, nil
}

func decodeSpan(b []byte) Span {
	s := Span{Attributes: map[string]any{}}
	for _, f := range must(b) {
		switch f.number {
		case 1:
			s.TraceID = hex.EncodeToString(f.bytes)
		case 2:
			s.SpanID = hex.EncodeToString(f.bytes)
		case 3:
			s.TraceState = string(f.bytes)
		case 4:
			s.ParentSpanID = hex.EncodeToString(f.bytes)
		case 5:
			s.Name = string(f.bytes)
		case 6:
			s.Kind = int(f.varint)
		case 7:
			s.Start = f.varint
		case 8:
			s.End = f.varint
		case 9:
			keyValue(f.bytes, s.Attributes)
		case 11:
			e := Event{Attributes: map[string]any{}}
			for _, ef := range must(f.bytes) {
				if ef.number == 2 {
					e.Name = string(ef.bytes)
				} else if ef.number == 3 {
					keyValue(ef.bytes, e.Attributes)
				}
			}
			s.Events = append(s.Events, e)
		case 13:
			l := Link{Attributes: map[string]any{}}
			for _, lf := range must(f.bytes) {
				switch lf.number {
				case 1:
					l.TraceID = hex.EncodeToString(lf.bytes)
				case 2:
					l.SpanID = hex.EncodeToString(lf.bytes)
				case 4:
					keyValue(lf.bytes, l.Attributes)
				case 6:
					l.Flags = uint32(lf.varint)
				}
			}
			s.Links = append(s.Links, l)
		case 15:
			for _, sf := range must(f.bytes) {
				if sf.number == 3 {
					s.StatusCode = int(sf.varint)
				}
			}
		case 16:
			s.Flags = uint32(f.varint)
		}
	}
	return s
}
