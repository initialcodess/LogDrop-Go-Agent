# Trace Go services with LogDrop (draft quickstart)

> **Draft for the panel guide.** Nothing is published yet: the module is tagged with the first release. Every value in
> angle brackets comes from the panel. The structure follows the other languages' quickstarts.

LogDrop traces Go services with the LogDrop Go package. A Go binary cannot load an agent at run time, so the service
adds the package, starts it in `main` and wraps the libraries it uses, one line each. On Kubernetes, one pod
annotation then gives it its settings; elsewhere you set them as environment variables. You get distributed traces,
service and endpoint health (APM) and the dependency map in the LogDrop panel, also across Java, Node.js, Python, .NET
and Go services in one trace.

The package is OpenTelemetry Go with LogDrop's pipeline, which removes the same data as the other LogDrop agents before
anything leaves the service.

## Before you start

- A LogDrop project with tracing enabled and a **traces ingest key** (panel: project settings, ingest keys).
- **Go 1.26 or newer.**
- On Kubernetes: Kubernetes 1.28 or newer and Helm 3.8 or newer.

## 1. Add the package and start it

```bash
go get github.com/initialcodess/LogDrop-Go-Agent@v0.1.0-alpha2
```

```go
import "github.com/initialcodess/LogDrop-Go-Agent/logdrop"

func main() {
	defer logdrop.Start()()                      // reads LOGDROP_*; sends what is left when main returns
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", getOrder)
	http.ListenAndServe(":8080", logdrop.Handler(mux))
}
```

Without the settings the service runs untraced, with one line on standard error; no call of the package fails it.

## 2. Wrap the libraries you use (one line each)

| Library | Line |
|---|---|
| `net/http` server (ServeMux) | `logdrop.Handler(mux)` |
| chi | `router.Use(logdrop.Middleware(logdrop.RouteFrom(func(r *http.Request) string { return chi.RouteContext(r.Context()).RoutePattern() })))` |
| Gin | `router.Use(otelgin.Middleware("orders"))` (`go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin`) |
| Echo v4 / v5 | `e.Use(echootel.NewMiddleware("orders"))` (`github.com/labstack/echo-otel/v4` or `/v5`) |
| gorilla/mux | `router.Use(otelmux.Middleware("orders"))` (`go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux`) |
| Outgoing HTTP | `client := logdrop.HTTPClient()` or `Transport: logdrop.Transport(nil)` |
| gRPC | server `grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))`, client `grpc.WithStatsHandler(otelgrpc.NewClientHandler())` |
| `database/sql` | `logdropsql.Open("pgx", dsn)` instead of `sql.Open` |
| pgx | `config.ConnConfig.Tracer = otelpgx.NewTracer()` (`github.com/exaring/otelpgx`; queries inside a request or custom span) |
| go-redis v9 | `logdropredis.Instrument(rdb)` |
| Kafka (segmentio/kafka-go) | `logdropkafka.WriteMessages(ctx, writer, msgs...)` and `logdropkafka.Process(ctx, msg, handle)` |
| RabbitMQ (amqp091-go) | `logdropamqp.Publish(ctx, ch, exchange, key, false, false, msg)` and `logdropamqp.Process(ctx, delivery, handle)` |

Pass the request's `context.Context` on to the calls you make, so they join the request's trace.

## 3. Kubernetes: one annotation gives the settings

Store the key in the namespace and install the LogDrop chart with tracing on (the same as for the other languages,
chart `0.1.1-alpha13` or newer), then:

```yaml
spec:
  template:
    metadata:
      labels:
        logdrop.io/trace: go
      annotations:
        logdrop.io/trace: go
        logdrop.io/service: orders            # optional; default: app.kubernetes.io/name, then the workload name
```

The injector mounts the key file and sets the `LOGDROP_*` settings and the pod's identity; no init container, no agent
image. `kubectl -n <namespace> get pod <pod> -o jsonpath='{.metadata.annotations.logdrop\.io/trace-status}'` shows
`injected`, and the service log shows:

```
[logdrop-agent] INFO LogDrop Go agent 0.1.0-alpha2 is tracing service "orders" (environment "production") to otel.logdrop.io.
```

## 3b. Outside Kubernetes

```bash
export LOGDROP_ENDPOINT=https://otel.logdrop.io
export LOGDROP_INGEST_KEY_FILE=/etc/logdrop/trace-ingest-key      # a file with the key (preferred)
export LOGDROP_SERVICE=orders
export LOGDROP_ENVIRONMENT=production
./orders
```

## Custom spans

```go
ctx, span := logdrop.StartSpan(ctx, "checkout.price")
defer span.End()
span.Attribute("order.type", "delivery")      // custom keys need LOGDROP_ALLOWED_ATTRIBUTE_KEYS=order.type

err := logdrop.InSpan(ctx, "checkout.pay", func(ctx context.Context, span *logdrop.Span) error {
	return payments.Pay(ctx, order)           // an error is recorded with its type only
})
```

`logdrop.Flush(timeout)`, `ExportStatus()` and `UpdateIngestCredential(key)` are available too.

## What LogDrop records

The same as the other LogDrop agents: route templates, methods, status codes, error types (`*fs.PathError`), database
systems and value-free SQL templates, Redis command names, messaging systems and destinations, service, release,
environment and the pod and cluster identity. Never request or response headers, cookies, query strings, SQL values,
Redis keys or values, message bodies, error messages or stack traces. Kubernetes liveness and readiness probes are not
traced. The package sends traces only.

## Troubleshooting

| You see | Do this |
|---|---|
| `LogDrop tracing is disabled: LOGDROP_ENDPOINT is not set.` | On Kubernetes: the pod lacks the annotation, or the chart is older than `0.1.1-alpha13` |
| `LogDrop tracing is disabled: the application sets its own OpenTelemetry tracer provider.` | The service sets up OpenTelemetry itself; remove that, or do not use the LogDrop package |
| `LogDrop ingest key is not available` | The Secret `logdrop-trace-ingest-key` is missing in that namespace |
| `LogDrop rejected the ingest key (HTTP 401)` | The key is revoked, expired or not a traces key |
| `LogDrop is not reachable` | Egress to the traces endpoint is blocked (network policy, proxy, DNS) |
| A library's calls are missing | That library is not wrapped (step 2), or the call does not get the request's `ctx` |
