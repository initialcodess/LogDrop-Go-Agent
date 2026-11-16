package logdrop_test

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/otlptest"
	"github.com/initialcodess/LogDrop-Go-Agent/logdrop"
)

type gateway struct {
	mu       sync.Mutex
	spans    []otlptest.Span
	resource map[string]any
	keys     []string
	agents   []string
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	request, err := otlptest.Decode(body)
	if err != nil || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Encoding") != "gzip" {
		w.WriteHeader(400)
		return
	}
	g.mu.Lock()
	g.spans = append(g.spans, request.Spans...)
	g.resource = request.Resource
	g.keys = append(g.keys, r.Header.Get("x-logdrop-ingest-key"))
	g.agents = append(g.agents, r.Header.Get("User-Agent"))
	g.mu.Unlock()
	w.WriteHeader(200)
}

func (g *gateway) find(name string) (otlptest.Span, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, s := range g.spans {
		if s.Name == name {
			return s, true
		}
	}
	return otlptest.Span{}, false
}

func TestTracingEndToEnd(t *testing.T) {
	g := &gateway{}
	ingest := httptest.NewTLSServer(g)
	defer ingest.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ingest.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOGDROP_ENDPOINT", ingest.URL)
	t.Setenv("LOGDROP_CA_FILE", ca)
	t.Setenv("LOGDROP_INGEST_KEY", "ldtk_first")
	t.Setenv("LOGDROP_SERVICE", "orders")
	t.Setenv("LOGDROP_RELEASE", "1.4.2")
	t.Setenv("LOGDROP_ALLOWED_ATTRIBUTE_KEYS", "order.type")
	t.Setenv("LOGDROP_SCHEDULE_DELAY", "50ms")
	shutdown := logdrop.Start()
	if !logdrop.IsAgentAttached() {
		t.Fatal("not attached")
	}

	downstreamMux := http.NewServeMux()
	downstreamMux.HandleFunc("GET /prices/{sku}", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "12") })
	downstream := httptest.NewServer(logdrop.Handler(downstreamMux))
	defer downstream.Close()

	client := logdrop.HTTPClient()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		err := logdrop.InSpan(r.Context(), "checkout.price", func(ctx context.Context, span *logdrop.Span) error {
			span.Attribute("order.type", "delivery").Attribute("customer.email", "a@b.c").Event("price.requested")
			request, _ := http.NewRequestWithContext(ctx, "GET", downstream.URL+"/prices/A1?token=private", nil)
			response, err := client.Do(request)
			if err != nil {
				return err
			}
			response.Body.Close()
			return &fs.PathError{Op: "open", Path: "/private/path", Err: errors.New("private message")}
		})
		if err == nil {
			t.Error("the error was not passed on")
		}
		w.WriteHeader(200)
	})
	server := httptest.NewServer(logdrop.Handler(mux))
	defer server.Close()
	response, err := http.Get(server.URL + "/orders/7?card=4111")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if !logdrop.Flush(5 * time.Second) {
		t.Fatalf("flush failed: %+v", logdrop.ExportStatus())
	}

	orders, ok := g.find("GET /orders/{id}")
	if !ok || orders.Attributes["http.route"] != "/orders/{id}" || orders.Kind != 2 {
		t.Fatalf("server span %+v", orders)
	}
	custom, ok := g.find("checkout.price")
	if !ok || custom.ParentSpanID != orders.SpanID || custom.Attributes["order.type"] != "delivery" ||
		custom.Attributes["customer.email"] != nil || custom.StatusCode != 2 || custom.Scope != logdrop.ScopeName {
		t.Fatalf("custom span %+v", custom)
	}
	if len(custom.Events) != 2 || custom.Events[1].Name != "exception" ||
		custom.Events[1].Attributes["exception.type"] != "*fs.PathError" || len(custom.Events[1].Attributes) != 1 {
		t.Fatalf("custom span events %+v", custom.Events)
	}
	call, ok := g.find("GET")
	if !ok || call.ParentSpanID != custom.SpanID || call.Kind != 3 {
		t.Fatalf("client span %+v", call)
	}
	prices, ok := g.find("GET /prices/{sku}")
	if !ok || prices.TraceID != orders.TraceID || prices.ParentSpanID != call.SpanID {
		t.Fatalf("downstream span %+v (trace %s)", prices, orders.TraceID)
	}
	g.mu.Lock()
	for _, s := range g.spans {
		for key, value := range s.Attributes {
			if text, ok := value.(string); ok && (strings.Contains(text, "4111") || strings.Contains(text, "private")) {
				t.Errorf("%s exports %s=%q", s.Name, key, text)
			}
		}
	}
	resource, firstKey, agent := g.resource, g.keys[0], g.agents[0]
	g.mu.Unlock()
	if resource["service.name"] != "orders" || resource["service.version"] != "1.4.2" || resource["telemetry.sdk.language"] != "go" ||
		resource["telemetry.distro.name"] != "logdrop-go-agent" {
		t.Fatalf("resource %+v", resource)
	}
	if firstKey != "ldtk_first" || agent != "logdrop-go-agent/"+logdrop.Version {
		t.Fatalf("headers %q %q", firstKey, agent)
	}

	if err := logdrop.UpdateIngestCredential("not a key"); !errors.Is(err, logdrop.ErrInvalidCredential) {
		t.Fatalf("invalid key: %v", err)
	}
	if err := logdrop.UpdateIngestCredential("ldtk_second"); err != nil {
		t.Fatal(err)
	}
	_, span := logdrop.StartSpan(context.Background(), "after.update")
	span.End()
	if !logdrop.Flush(5*time.Second) || logdrop.Flush(0) {
		t.Fatal("flush")
	}
	g.mu.Lock()
	lastKey := g.keys[len(g.keys)-1]
	g.mu.Unlock()
	if lastKey != "ldtk_second" {
		t.Fatalf("key after update %q", lastKey)
	}
	if s := logdrop.ExportStatus(); !s.AgentAttached || s.ExportedSpans < 5 || s.FailedSpans != 0 || s.ActiveSpans != 0 {
		t.Fatalf("status %+v", s)
	}
	_, blank := logdrop.StartSpan(context.Background(), " ")
	blank.Attribute("order.type", "x").End()
	shutdown()
	if s := logdrop.ExportStatus(); !s.Closed || logdrop.Flush(time.Second) {
		t.Fatalf("after shutdown %+v", s)
	}
	if err := logdrop.UpdateIngestCredential("ldtk_third"); !errors.Is(err, logdrop.ErrClosed) {
		t.Fatalf("update after shutdown: %v", err)
	}
}
