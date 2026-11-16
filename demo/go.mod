module github.com/initialcodess/LogDrop-Go-Agent/demo

go 1.26.0

// Never released: the demo builds the LogDrop package of this repository.
replace (
	github.com/initialcodess/LogDrop-Go-Agent => ../
	github.com/initialcodess/LogDrop-Go-Agent/logdropredis => ../logdropredis
)

require (
	github.com/initialcodess/LogDrop-Go-Agent v0.0.0-00010101000000-000000000000
	github.com/initialcodess/LogDrop-Go-Agent/logdropredis v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.13.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/XSAM/otelsql v0.44.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dlclark/regexp2 v1.12.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.23.0 // indirect
	github.com/redis/go-redis/extra/redisotel/v9 v9.23.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)
