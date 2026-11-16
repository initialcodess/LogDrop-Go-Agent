package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/export"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/otlptest"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func baseEnv() map[string]string {
	return map[string]string{
		"LOGDROP_ENDPOINT":   "https://otel.example.com",
		"LOGDROP_INGEST_KEY": "ldtk_test",
		"LOGDROP_SERVICE":    "orders",
	}
}

func TestSettingsDefaultsAndProblems(t *testing.T) {
	s := ParseSettings(baseEnv())
	if !s.Valid() || s.TracesEndpoint != "https://otel.example.com/v1/traces" || s.EndpointHost != "otel.example.com" ||
		s.Environment != "production" || s.MaxQueueSize != 2048 || s.MaxExportBatchSize != 128 || s.Compression != "gzip" ||
		s.Limits.MaxAttributes != 24 {
		t.Fatalf("defaults: %+v", *s)
	}
	env := baseEnv()
	env["LOGDROP_INGEST_KEY_FILE"] = "/secret/path"
	env["LOGDROP_MAX_QUEUE_SIZE"] = "lots"
	env["LOGDROP_SCHEDULE_DELAY"] = "1m"
	env["LOGDROP_SERVICE"] = "ord\x07ers"
	env["LOGDROP_MAX_EXPORT_BATCH_SIZE"] = "300"
	env["LOGDROP_ALLOWED_ATTRIBUTE_KEYS"] = "order.type, 1bad"
	s = ParseSettings(env)
	joined := strings.Join(s.Problems, " ")
	for _, want := range []string{"not both", "LOGDROP_MAX_QUEUE_SIZE must be a whole number", "LOGDROP_SCHEDULE_DELAY must be a duration",
		"at most 128 characters", "at most 256", "LOGDROP_ALLOWED_ATTRIBUTE_KEYS"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in %q", want, joined)
		}
	}
	if strings.Contains(joined, "/secret/path") || strings.Contains(joined, "ldtk_test") || strings.Contains(joined, "lots") {
		t.Errorf("a problem shows a value: %q", joined)
	}
	if DisabledReason(map[string]string{"LOGDROP_ENDPOINT": " "}) != "LOGDROP_ENDPOINT is not set." ||
		DisabledReason(map[string]string{"LOGDROP_ENDPOINT": "https://x", "LOGDROP_ENABLED": "FALSE"}) == "" {
		t.Error("disabled reasons")
	}
}

func TestKubernetesIdentityAndInstanceID(t *testing.T) {
	env := baseEnv()
	env["LOGDROP_K8S_POD_UID"] = "uid-1"
	env["LOGDROP_K8S_CONTAINER_NAME"] = "app"
	env["LOGDROP_K8S_WORKLOAD_KIND"] = "Deployment"
	env["LOGDROP_K8S_WORKLOAD_NAME"] = "orders"
	s := ParseSettings(env)
	if s.Kubernetes["k8s.deployment.name"] != "orders" || !s.InKubernetes() {
		t.Fatalf("kubernetes %+v", s.Kubernetes)
	}
	// java.util.UUID.nameUUIDFromBytes("uid-1/app"), the Java agent's instance ID for this container.
	if id := InstanceID(s); id != "fd5ab36e-168d-3348-abb3-e455e93c663f" {
		t.Fatalf("instance %s", id)
	}
	resource := map[string]any{}
	for _, a := range BuildResource(s, map[string]string{"host.name": "image-host", "telemetry.sdk.language": "go"}) {
		resource[a.Key] = a.Value
	}
	if _, ok := resource["host.name"]; ok || resource["telemetry.distro.name"] != Distribution || resource["service.name"] != "orders" {
		t.Fatalf("resource %+v", resource)
	}
	if ContainerID("0::/kubepods/pod1/"+strings.Repeat("a", 64)+".scope\n", "") != strings.Repeat("a", 64) {
		t.Fatal("container id")
	}
}

func values(kv ...attribute.KeyValue) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, a := range kv {
		m[string(a.Key)] = a.Value
	}
	return m
}

func TestDependencyNaming(t *testing.T) {
	v := values(attribute.String("db.system.name", "redis"), attribute.String("server.address", "redis:6379"), attribute.Int("server.port", 6379))
	if name := ApplyNaming("incr", v); name != "INCR" || v["db.operation.name"].AsString() != "INCR" || v["server.address"].AsString() != "redis" {
		t.Fatalf("redis: %s %+v", name, v)
	}
	v = values(attribute.String("db.system", "redis"))
	if name := ApplyNaming("set private-key", v); name != "set private-key" {
		t.Fatalf("a second word after a plain command could be a key: %s", name)
	}
	v = values(attribute.String("http.request.method", "GET"), attribute.String("network.protocol.version", "1.1"))
	ApplyNaming("GET", v)
	if _, ok := v["network.protocol.version"]; ok {
		t.Fatal("HTTP/1.1 version kept")
	}
	v = values(attribute.String("rpc.system.name", "grpc"), attribute.String("rpc.method", "shop.Orders/Get"),
		attribute.String("rpc.response.status_code", "NOT_FOUND"))
	ApplyNaming("shop.Orders/Get", v)
	if v["rpc.system"].AsString() != "grpc" || v["rpc.service"].AsString() != "shop.Orders" || v["rpc.method"].AsString() != "Get" ||
		v["rpc.grpc.status_code"].AsInt64() != 5 {
		t.Fatalf("grpc keys %+v", v)
	}
	v = values(attribute.String("messaging.system", "rabbitmq"))
	ApplyNaming("orders publish", v)
	if v["peer.service"].AsString() != "RabbitMQ" {
		t.Fatal("rabbitmq peer")
	}
}

type capture struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (c *capture) Send(body []byte, _ string) export.Attempt {
	c.mu.Lock()
	c.bodies = append(c.bodies, body)
	c.mu.Unlock()
	return export.Attempt{Status: 200}
}

func (c *capture) Close() {}

func (c *capture) spans(t *testing.T) []otlptest.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []otlptest.Span
	for _, b := range c.bodies {
		r, err := otlptest.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r.Spans...)
	}
	return out
}

func TestPipelineDropsProbesAndSanitizes(t *testing.T) {
	env := baseEnv()
	env["LOGDROP_ALLOWED_ATTRIBUTE_KEYS"] = "order.type"
	sent := &capture{}
	a, err := New(ParseSettings(env), diag.New(func(string) {}, nil), sent)
	if err != nil {
		t.Fatal(err)
	}
	tracer := a.Provider.Tracer("test")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, child := tracer.Start(r.Context(), "SELECT orders")
		child.SetAttributes(attribute.String("db.system.name", "postgresql"),
			attribute.String("db.query.text", "SELECT total FROM orders WHERE id = 42 AND owner = 'ayse'"),
			attribute.String("url.full", "https://x/?token=secret"), attribute.String("order.type", "delivery"))
		child.End()
		w.WriteHeader(204)
	})
	handler := otelhttp.NewHandler(mux, "server", otelhttp.WithTracerProvider(a.Provider),
		otelhttp.WithPropagators(propagation.TraceContext{}))
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, agent := range []string{"kube-probe/1.31", "curl/8"} {
		request, _ := http.NewRequest("GET", server.URL+"/orders/7?card=4111", nil)
		request.Header.Set("User-Agent", agent)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	if !a.Flush(5 * time.Second) {
		t.Fatal("flush")
	}
	spans := sent.spans(t)
	if len(spans) != 2 {
		t.Fatalf("%d spans, want the curl request's server and child span only", len(spans))
	}
	var serverSpan, child otlptest.Span
	for _, s := range spans {
		if s.Kind == int(trace.SpanKindServer) {
			serverSpan = s
		} else {
			child = s
		}
	}
	if child.Attributes["db.query.text"] != "SELECT total FROM orders WHERE id = ? AND owner = ?" ||
		child.Attributes["order.type"] != "delivery" || child.Attributes["url.full"] != nil || child.ParentSpanID != serverSpan.SpanID {
		t.Fatalf("child %+v", child.Attributes)
	}
	for key := range serverSpan.Attributes {
		if key == "url.query" || key == "url.full" || key == "user_agent.original" || key == "http.target" {
			t.Fatalf("server span exports %s", key)
		}
	}
	if s := a.Status(); s.Exported != 2 || s.Active != 0 {
		t.Fatalf("status %+v", s)
	}
	_ = a.Provider.Shutdown(context.Background())
	if a.Flush(time.Second) || a.UpdateIngestCredential("ldtk_new") != Closed {
		t.Fatal("after shutdown")
	}
}

func TestAdmissionRefusesOverTheLimitAndForgetsAbandonedSpans(t *testing.T) {
	var clock time.Duration
	refusals := 0
	a := NewAdmission(1, time.Minute, func() { refusals++ }, func() time.Duration { return clock })
	first := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}})
	if a.ShouldSample(samplingParameters()).Decision != 2 {
		t.Fatal("first span refused")
	}
	a.Started(first)
	if a.ShouldSample(samplingParameters()).Decision != 0 || refusals != 1 || a.Refused() != 1 {
		t.Fatal("second span admitted")
	}
	clock += 2 * time.Minute
	if a.ShouldSample(samplingParameters()).Decision != 2 || a.Abandoned() != 1 {
		t.Fatal("abandoned span still counts")
	}
}

func samplingParameters() sdktrace.SamplingParameters {
	return sdktrace.SamplingParameters{ParentContext: context.Background()}
}
