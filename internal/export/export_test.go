package export

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/otlptest"
	"go.opentelemetry.io/otel/trace"
)

const now = int64(1_800_000_000_000_000_000)

func span(name string, start int64) core.SpanData {
	return core.SpanData{Name: name, Kind: trace.SpanKindServer, TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2},
		StartUnixNano: start, EndUnixNano: start + 1000, ScopeName: "scope",
		Attributes: []core.SafeAttribute{{Key: "http.route", Value: "/orders"}}}
}

func TestSplitKeepsRequestsInsideLogDropLimits(t *testing.T) {
	var spans []core.SpanData
	for i := 0; i < 300; i++ {
		spans = append(spans, span("a", now))
	}
	requests, dropped := Split(spans, now)
	if dropped != 0 || len(requests) != 2 || len(requests[0]) != 256 || len(requests[1]) != 44 {
		t.Fatalf("300 spans: %d requests, dropped %d", len(requests), dropped)
	}
	spans = nil
	for i := 0; i < 40; i++ {
		spans = append(spans, span("a", now-int64(i)*minuteNanos))
	}
	spans = append(spans, span("future", now+6*minuteNanos), span("old", now-8*24*60*minuteNanos), span("zero", 0))
	requests, dropped = Split(spans, now)
	if dropped != 3 || len(requests) != 2 || len(requests[0]) != 32 || len(requests[1]) != 8 {
		t.Fatalf("40 minutes: %d requests (%d), dropped %d", len(requests), len(requests[0]), dropped)
	}
	if requests[0][0].StartUnixNano > requests[1][0].StartUnixNano {
		t.Fatal("requests are not in start-minute order")
	}
}

type fakeTransport struct {
	mu       sync.Mutex
	attempts []Attempt
	bodies   [][]byte
	keys     []string
}

func (f *fakeTransport) Send(body []byte, key string) Attempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bodies = append(f.bodies, body)
	f.keys = append(f.keys, key)
	if len(f.attempts) == 0 {
		return Attempt{Status: 200}
	}
	a := f.attempts[0]
	if len(f.attempts) > 1 {
		f.attempts = f.attempts[1:]
	}
	return a
}

func (f *fakeTransport) Close() {}

func (f *fakeTransport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

type lines struct {
	mu   sync.Mutex
	list []string
}

func (l *lines) write(line string) {
	l.mu.Lock()
	l.list = append(l.list, line)
	l.mu.Unlock()
}

func (l *lines) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.list, "\n")
}

func exporter(t *fakeTransport, out *lines, key *KeySource) *Exporter {
	retry := NewRetrySchedule(nil, func() float64 { return 0.002 }, 0)
	if key == nil {
		key = FixedKey("ldtk_test")
	}
	return NewExporter(t, key, retry, diag.New(out.write, nil), "gzip", 5*time.Second,
		[]core.SafeAttribute{{Key: "service.name", Value: "orders"}}, func() int64 { return now })
}

func TestNoResponseIsResentWithTheSameBytes(t *testing.T) {
	transport := &fakeTransport{attempts: []Attempt{{NoResponse: true, Code: "ECONNREFUSED"}, {NoResponse: true, Code: "ECONNREFUSED"}, {Status: 200}}}
	e := exporter(transport, &lines{}, nil)
	if !e.Export([]core.SpanData{span("GET /orders", now)}) || e.Exported() != 1 || transport.count() != 3 {
		t.Fatalf("exported %d after %d attempts", e.Exported(), transport.count())
	}
	if string(transport.bodies[0]) != string(transport.bodies[2]) {
		t.Fatal("a resent request has other bytes")
	}
	request, err := otlptest.Decode(transport.bodies[0])
	if err != nil || len(request.Spans) != 1 || request.Resource["service.name"] != "orders" {
		t.Fatalf("decoded %+v, %v", request, err)
	}
	s := request.Spans[0]
	if s.Name != "GET /orders" || s.Kind != 2 || s.Attributes["http.route"] != "/orders" || s.Flags&0x100 == 0 || s.Scope != "scope" {
		t.Fatalf("span %+v", s)
	}
}

func TestRetriedAnswersAndFinalAnswers(t *testing.T) {
	transport := &fakeTransport{attempts: []Attempt{{Status: 503}, {Status: 429}, {Status: 202}}}
	e := exporter(transport, &lines{}, nil)
	if !e.Export([]core.SpanData{span("a", now)}) || transport.count() != 3 {
		t.Fatalf("503, 429, 202: %d attempts", transport.count())
	}
	out := &lines{}
	transport = &fakeTransport{attempts: []Attempt{{Status: 400}}}
	e = exporter(transport, out, nil)
	if e.Export([]core.SpanData{span("a", now)}) || e.Rejected() != 1 || transport.count() != 1 {
		t.Fatalf("400: rejected %d after %d attempts", e.Rejected(), transport.count())
	}
	if !strings.Contains(out.joined(), "HTTP 400") {
		t.Fatalf("no 400 warning: %s", out.joined())
	}
	out = &lines{}
	transport = &fakeTransport{attempts: []Attempt{{Status: 401}}}
	e = exporter(transport, out, nil)
	if e.Export([]core.SpanData{span("a", now)}) || e.Failed() != 1 || transport.count() != 1 {
		t.Fatalf("401: failed %d after %d attempts", e.Failed(), transport.count())
	}
	if !strings.Contains(out.joined(), "LogDrop rejected the ingest key (HTTP 401).") || strings.Contains(out.joined(), "ldtk_test") {
		t.Fatalf("401 warning: %s", out.joined())
	}
}

func TestTimeoutAfterWriteIsResentOnceOnly(t *testing.T) {
	transport := &fakeTransport{attempts: []Attempt{{NoResponse: true, AfterWrite: true, Code: "LOGDROP_TIMEOUT"}}}
	out := &lines{}
	e := exporter(transport, out, nil)
	if e.Export([]core.SpanData{span("a", now)}) || transport.count() != 2 {
		t.Fatalf("timeouts after write: %d attempts", transport.count())
	}
	if !strings.Contains(out.joined(), "LogDrop is not reachable. Trace export failed.") {
		t.Fatalf("warning: %s", out.joined())
	}
}

func TestSpansOutsideTheClockWindowAreRejectedBeforeSending(t *testing.T) {
	transport := &fakeTransport{}
	e := exporter(transport, &lines{}, nil)
	if e.Export([]core.SpanData{span("future", now+10*minuteNanos)}) || e.Rejected() != 1 || transport.count() != 0 {
		t.Fatalf("rejected %d, attempts %d", e.Rejected(), transport.count())
	}
}

func TestKeyFileIsReReadAndKeptWhenInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("ldtk_first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var clock time.Duration
	out := &lines{}
	keys := FileKey(path, diag.New(out.write, nil), func() time.Duration { return clock })
	if keys.Current() != "ldtk_first" {
		t.Fatal("first key")
	}
	_ = os.WriteFile(path, []byte("ldtk_second"), 0o600)
	clock += 5 * time.Second
	if keys.Current() != "ldtk_first" {
		t.Fatal("re-read before 10 s")
	}
	clock += 5 * time.Second
	if keys.Current() != "ldtk_second" {
		t.Fatal("not re-read after 10 s")
	}
	_ = os.WriteFile(path, []byte("not a key"), 0o600)
	keys.Rejected()
	clock += 2 * time.Second
	if keys.Current() != "ldtk_second" || !strings.Contains(out.joined(), "the last valid key is still used") {
		t.Fatalf("invalid file: %s", out.joined())
	}
	keys.Override("ldtk_override")
	clock += 11 * time.Second
	if keys.Current() != "ldtk_override" {
		t.Fatal("override replaced while the file did not change")
	}
}

func TestQueueBatchesFlushesAndDropsWhenFull(t *testing.T) {
	transport := &fakeTransport{}
	e := exporter(transport, &lines{}, nil)
	q := NewQueue(e, 4, 2, time.Hour)
	for i := 0; i < 6; i++ {
		q.Add(span("a", now))
	}
	if !q.Flush(5 * time.Second) {
		t.Fatal("flush timed out")
	}
	if e.Exported()+q.Dropped() != 6 || e.Exported() < 4 {
		t.Fatalf("exported %d, dropped %d", e.Exported(), q.Dropped())
	}
	q.Shutdown(time.Second)
	if q.Flush(time.Second) {
		t.Fatal("flush after shutdown")
	}
	q.Add(span("late", now))
	if q.Pending() != 0 {
		t.Fatal("span queued after shutdown")
	}
}

func TestLinkFlagsCarryRemoteBits(t *testing.T) {
	s := span("a", now)
	s.ParentIsRemote, s.HasParent, s.ParentSpanID, s.Flags = true, true, trace.SpanID{9}, 1
	s.Links = []core.SpanLink{{TraceID: trace.TraceID{3}, SpanID: trace.SpanID{4}, Flags: 1, IsRemote: true}}
	s.Attributes = append(s.Attributes, core.SafeAttribute{Key: "n", Value: int64(-5)}, core.SafeAttribute{Key: "f", Value: 0.0},
		core.SafeAttribute{Key: "b", Value: false})
	request, err := otlptest.Decode(Encode([]core.SpanData{s}, nil))
	if err != nil {
		t.Fatal(err)
	}
	got := request.Spans[0]
	if got.Flags != 0x301 || got.Links[0].Flags != 0x301 || got.ParentSpanID == "" {
		t.Fatalf("flags %x, link %x", got.Flags, got.Links[0].Flags)
	}
	if got.Attributes["n"] != int64(-5) || got.Attributes["f"] != 0.0 || got.Attributes["b"] != false {
		t.Fatalf("attributes %+v", got.Attributes)
	}
}
