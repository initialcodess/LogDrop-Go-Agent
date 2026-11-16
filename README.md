# LogDrop Go tracing

Tracing for Go services: the `logdrop` package starts OpenTelemetry Go with LogDrop's pipeline, whose privacy layer
behaves exactly like the LogDrop Java, Node.js, Python and .NET agents (the shared cases in `conformance/privacy` come
from the Java agent, the reference). A Go binary cannot load an agent at run time, so the service adds a few lines:
`logdrop.Start` and one line per traced library. Customer guide: [docs/quickstart.md](docs/quickstart.md).

| Path | What it is |
|---|---|
| `logdrop` | `Start`, `Flush`, `ExportStatus`, `UpdateIngestCredential`, custom spans, `net/http` server and client |
| `logdropsql` | `database/sql` (`logdropsql.Open` instead of `sql.Open`) |
| `logdropredis` | go-redis v9 (separate module) |
| `logdropkafka` | segmentio/kafka-go producer and consumer spans (separate module) |
| `logdropamqp` | RabbitMQ amqp091-go publish and process spans (separate module) |
| `internal` | The privacy and export rules, the sampler, processor, queue, retry schedule and OTLP encoder |
| `integration` | Every documented integration against real servers (`scripts/integration.sh`); never released |
| `demo` | A `net/http` demo, the Go counterpart of the other agents' demos |
| `proof` | A disposable kind cluster with the chart's trace injector (settings only for Go), the demo and Redis |
| `conformance` | The privacy cases shared with the other agents |

Gin, Echo, gorilla/mux, gRPC and pgx use their own OpenTelemetry instrumentation packages (see the quickstart); the
LogDrop pipeline filters what they record the same way.

## Develop

```sh
go test -race ./...                                   # unit, conformance and end-to-end tests (Go 1.26 or newer)
python3 scripts/conformance.py check                  # shared privacy cases match the Java agent
scripts/integration.sh                                # PostgreSQL, Redis, Kafka, RabbitMQ in Docker
proof/run.sh ../LogDrop-Devops <output dir>           # kind, the chart's injector, the demo
```

The `go.work` workspace ties the modules together for development; a release is a git tag per module
(`v0.1.0-alpha1`, `logdropredis/v0.1.0-alpha1`, ...). Nothing here is published or deployed by these commands.
