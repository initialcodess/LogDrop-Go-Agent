package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The shared privacy cases in conformance/privacy, copied from the Java agent (the reference).

func load(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "conformance", "privacy", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatal(err)
	}
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestSQLNormalizer(t *testing.T) {
	var file struct {
		Cases []struct {
			Name     string
			System   *string
			Input    *string
			Expected *string
		}
	}
	load(t, "sql-normalizer.json", &file)
	for _, c := range file.Cases {
		t.Run(c.Name, func(t *testing.T) {
			got, ok := NormalizeSQL(str(c.Input), str(c.System))
			if c.Expected == nil {
				if ok {
					t.Fatalf("expected nothing, got %q", got)
				}
				return
			}
			if !ok || got != *c.Expected {
				t.Fatalf("got %q (%v), want %q", got, ok, *c.Expected)
			}
		})
	}
}

type limitsJSON struct {
	MaxActiveSpans, MaxAttributes, MaxEvents, MaxLinks, MaxStringLength int
}

type rulesJSON struct {
	Attributes []struct {
		Key           string
		Allowed       []string
		Exportable    bool
		CustomAllowed bool
	}
	SQLSystems struct {
		SQL    []*string `json:"sql"`
		NotSQL []*string `json:"notSql"`
	} `json:"sqlSystems"`
	CustomKeys struct{ Valid, Invalid []*string }
	Limits     struct {
		Mvp     limitsJSON
		Invalid []limitsJSON
	}
	Endpoints struct {
		Valid []struct {
			Endpoint string
			Insecure bool
			Traces   string
		}
		Invalid []struct {
			Endpoint *string
			Insecure bool
		}
	}
	Credentials struct {
		Valid, Invalid []*string
		MaxLength      int
	}
	ServiceIdentity struct{ Valid, Invalid [][]*string }
}

func TestRules(t *testing.T) {
	var r rulesJSON
	load(t, "rules.json", &r)
	for _, c := range r.Attributes {
		allowed := set(c.Allowed...)
		if IsExportable(c.Key, allowed) != c.Exportable {
			t.Errorf("exportable %q", c.Key)
		}
		if IsCustomAllowed(c.Key, allowed) != c.CustomAllowed {
			t.Errorf("customAllowed %q", c.Key)
		}
	}
	for _, s := range r.SQLSystems.SQL {
		if !IsSQLSystem(str(s)) {
			t.Errorf("sql system %q", str(s))
		}
	}
	for _, s := range r.SQLSystems.NotSQL {
		if IsSQLSystem(str(s)) {
			t.Errorf("not a sql system %q", str(s))
		}
	}
	for _, k := range r.CustomKeys.Valid {
		if !IsValidCustomKey(str(k)) {
			t.Errorf("valid custom key %q", str(k))
		}
	}
	for _, k := range r.CustomKeys.Invalid {
		if IsValidCustomKey(str(k)) {
			t.Errorf("invalid custom key %q", str(k))
		}
	}
	mvp, err := MvpLimits(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := r.Limits.Mvp
	if mvp.MaxActiveSpans != want.MaxActiveSpans || mvp.MaxAttributes != want.MaxAttributes || mvp.MaxEvents != want.MaxEvents ||
		mvp.MaxLinks != want.MaxLinks || mvp.MaxStringLength != want.MaxStringLength {
		t.Errorf("mvp limits %+v", mvp)
	}
	for _, l := range r.Limits.Invalid {
		if _, err := NewTracingLimits(l.MaxActiveSpans, l.MaxAttributes, l.MaxEvents, l.MaxLinks, l.MaxStringLength, nil); !IsLimitError(err) {
			t.Errorf("invalid limits accepted: %+v", l)
		}
	}
}

func TestEndpointsCredentialsAndIdentity(t *testing.T) {
	var r rulesJSON
	load(t, "rules.json", &r)
	for _, e := range r.Endpoints.Valid {
		got, err := TracesEndpoint(e.Endpoint, e.Insecure)
		if err != nil || got != e.Traces {
			t.Errorf("endpoint %q: got %q, %v; want %q", e.Endpoint, got, err, e.Traces)
		}
	}
	for _, e := range r.Endpoints.Invalid {
		if got, err := TracesEndpoint(str(e.Endpoint), e.Insecure); !IsLimitError(err) {
			t.Errorf("endpoint %q accepted as %q", str(e.Endpoint), got)
		}
	}
	for _, k := range r.Credentials.Valid {
		if !IsValidCredential(str(k)) {
			t.Errorf("valid credential %q", str(k))
		}
	}
	for _, k := range r.Credentials.Invalid {
		if IsValidCredential(str(k)) {
			t.Errorf("invalid credential %q", str(k))
		}
	}
	long := make([]byte, r.Credentials.MaxLength+1)
	for i := range long {
		long[i] = 'a'
	}
	if !IsValidCredential(string(long[:r.Credentials.MaxLength])) || IsValidCredential(string(long)) {
		t.Error("credential length bound")
	}
	identity := func(parts []*string) error {
		dimensions := []string{}
		for _, p := range parts[1:] {
			dimensions = append(dimensions, str(p))
		}
		return RequireServiceIdentity(parts[0], dimensions...)
	}
	for _, i := range r.ServiceIdentity.Valid {
		if err := identity(i); err != nil {
			t.Errorf("valid identity %v: %v", i, err)
		}
	}
	for _, i := range r.ServiceIdentity.Invalid {
		if err := identity(i); !IsLimitError(err) {
			t.Errorf("invalid identity accepted: %q", str(i[0]))
		}
	}
}

type attributeJSON struct {
	Key   string
	Type  string
	Value json.RawMessage
}

func typed(t *testing.T, a attributeJSON) attribute.KeyValue {
	key := attribute.Key(a.Key)
	switch a.Type {
	case "string":
		var s string
		mustUnmarshal(t, a.Value, &s)
		return key.String(s)
	case "boolean":
		var b bool
		mustUnmarshal(t, a.Value, &b)
		return key.Bool(b)
	case "long":
		var l int64
		mustUnmarshal(t, a.Value, &l)
		return key.Int64(l)
	case "double":
		var s string
		if json.Unmarshal(a.Value, &s) == nil {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				t.Fatal(err)
			}
			return key.Float64(f)
		}
		var f float64
		mustUnmarshal(t, a.Value, &f)
		return key.Float64(f)
	case "stringArray":
		var s []string
		mustUnmarshal(t, a.Value, &s)
		return key.StringSlice(s)
	case "longArray":
		var l []int64
		mustUnmarshal(t, a.Value, &l)
		return key.Int64Slice(l)
	}
	t.Fatalf("unknown attribute type %s", a.Type)
	return attribute.KeyValue{}
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatal(err)
	}
}

func typedList(t *testing.T, list []attributeJSON) []attribute.KeyValue {
	out := []attribute.KeyValue{}
	for _, a := range list {
		out = append(out, typed(t, a))
	}
	return out
}

func sameValue(t *testing.T, key string, raw json.RawMessage, actual any) {
	t.Helper()
	var v any
	mustUnmarshal(t, raw, &v)
	switch expected := v.(type) {
	case string:
		if s, ok := actual.(string); !ok || s != expected {
			t.Errorf("%s: got %#v, want %q", key, actual, expected)
		}
	case bool:
		if b, ok := actual.(bool); !ok || b != expected {
			t.Errorf("%s: got %#v, want %v", key, actual, expected)
		}
	case float64:
		text := string(raw)
		if isWhole(text) {
			if l, ok := actual.(int64); !ok || float64(l) != expected {
				t.Errorf("%s: got %#v, want long %v", key, actual, expected)
			}
		} else if f, ok := actual.(float64); !ok || f != expected {
			t.Errorf("%s: got %#v, want double %v", key, actual, expected)
		}
	default:
		t.Errorf("%s: unexpected expected value %s", key, raw)
	}
}

func isWhole(text string) bool {
	for _, c := range text {
		if c == '.' || c == 'e' || c == 'E' {
			return false
		}
	}
	return true
}

func sameAttributes(t *testing.T, expected map[string]json.RawMessage, actual []SafeAttribute) {
	t.Helper()
	got := map[string]any{}
	keys := []string{}
	for _, a := range actual {
		got[a.Key] = a.Value
		keys = append(keys, a.Key)
	}
	wantKeys := []string{}
	for k := range expected {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(keys)
	sort.Strings(wantKeys)
	if len(keys) != len(wantKeys) {
		t.Fatalf("attributes: got %v, want %v", keys, wantKeys)
	}
	for i := range keys {
		if keys[i] != wantKeys[i] {
			t.Fatalf("attributes: got %v, want %v", keys, wantKeys)
		}
	}
	for k, raw := range expected {
		sameValue(t, k, raw, got[k])
	}
}

func TestSpanSanitizer(t *testing.T) {
	var file struct {
		Cases []struct {
			Name    string
			Limits  json.RawMessage
			Allowed []string
			Span    struct {
				Name       string
				Attributes []attributeJSON
				Status     *struct{ Code, Description string }
				Events     []struct {
					Name       string
					Attributes []attributeJSON
				}
				Links []struct{ Attributes []attributeJSON }
			}
			Expected struct {
				Name           string
				Attributes     map[string]json.RawMessage
				AttributeCount *int
				AttributesFrom map[string]json.RawMessage
				Status         struct{ Code, Description string }
				Events         []struct {
					Name       string
					Attributes map[string]json.RawMessage
				}
				Links []struct{ Attributes map[string]json.RawMessage }
			}
		}
	}
	load(t, "span-sanitizer.json", &file)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := provider.Tracer("conformance")
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		SpanID:  trace.SpanID{0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22}, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	for _, c := range file.Cases {
		t.Run(c.Name, func(t *testing.T) {
			var limits TracingLimits
			var err error
			var name string
			if json.Unmarshal(c.Limits, &name) == nil {
				limits, err = MvpLimits(c.Allowed)
			} else {
				var l limitsJSON
				mustUnmarshal(t, c.Limits, &l)
				limits, err = NewTracingLimits(l.MaxActiveSpans, l.MaxAttributes, l.MaxEvents, l.MaxLinks, l.MaxStringLength, c.Allowed)
			}
			if err != nil {
				t.Fatal(err)
			}
			links := []trace.Link{}
			for _, l := range c.Span.Links {
				links = append(links, trace.Link{SpanContext: remote, Attributes: typedList(t, l.Attributes)})
			}
			_, span := tracer.Start(context.Background(), c.Span.Name, trace.WithLinks(links...))
			span.SetAttributes(typedList(t, c.Span.Attributes)...)
			for _, e := range c.Span.Events {
				span.AddEvent(e.Name, trace.WithAttributes(typedList(t, e.Attributes)...))
			}
			if c.Span.Status != nil {
				code := map[string]codes.Code{"ERROR": codes.Error, "OK": codes.Ok}[c.Span.Status.Code]
				span.SetStatus(code, c.Span.Status.Description)
			}
			span.End()
			ended := recorder.Ended()
			finished := ended[len(ended)-1]
			data := Snapshot(finished, limits, finished.Name(), finished.Attributes())
			if data.Name != c.Expected.Name {
				t.Errorf("name %q, want %q", data.Name, c.Expected.Name)
			}
			if c.Expected.Attributes != nil {
				sameAttributes(t, c.Expected.Attributes, data.Attributes)
			} else {
				if len(data.Attributes) != *c.Expected.AttributeCount {
					t.Errorf("attribute count %d, want %d", len(data.Attributes), *c.Expected.AttributeCount)
				}
				for _, a := range data.Attributes {
					sameValue(t, a.Key, c.Expected.AttributesFrom[a.Key], a.Value)
				}
			}
			status := map[int]string{2: "ERROR", 1: "OK", 0: "UNSET"}[data.StatusCode]
			if status != c.Expected.Status.Code || c.Expected.Status.Description != "" {
				t.Errorf("status %s, want %s", status, c.Expected.Status.Code)
			}
			if len(data.Events) != len(c.Expected.Events) {
				t.Fatalf("events %d, want %d", len(data.Events), len(c.Expected.Events))
			}
			for i, e := range c.Expected.Events {
				if data.Events[i].Name != e.Name {
					t.Errorf("event %q, want %q", data.Events[i].Name, e.Name)
				}
				sameAttributes(t, e.Attributes, data.Events[i].Attributes)
			}
			if len(data.Links) != len(c.Expected.Links) {
				t.Fatalf("links %d, want %d", len(data.Links), len(c.Expected.Links))
			}
			for i, l := range c.Expected.Links {
				sameAttributes(t, l.Attributes, data.Links[i].Attributes)
			}
		})
	}

}
