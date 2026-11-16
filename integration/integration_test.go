// Package integration checks every library integration the LogDrop Go documentation names against real servers, with
// the logdrop pipeline in front of a fake gateway. Run it with scripts/integration.sh (it starts PostgreSQL, Redis,
// Kafka and RabbitMQ in Docker); without LOGDROP_IT=1 it is skipped.
package integration

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/mux"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/otlptest"
	"github.com/initialcodess/LogDrop-Go-Agent/logdrop"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropamqp"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropkafka"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropredis"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropsql"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	echootelv4 "github.com/labstack/echo-otel/v4"
	echootelv5 "github.com/labstack/echo-otel/v5"
	"github.com/labstack/echo/v4"
	echov5 "github.com/labstack/echo/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Values that must never leave the service.
var private = []string{"4111", "secret-owner", "private-key", "private-body", "card=", "s3cr3t"}

type gateway struct {
	mu    sync.Mutex
	spans []otlptest.Span
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	request, err := otlptest.Decode(body)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	g.mu.Lock()
	g.spans = append(g.spans, request.Spans...)
	g.mu.Unlock()
}

func (g *gateway) take() []otlptest.Span {
	g.mu.Lock()
	defer g.mu.Unlock()
	spans := g.spans
	g.spans = nil
	return spans
}

func flush(t *testing.T, g *gateway) []otlptest.Span {
	t.Helper()
	if !logdrop.Flush(10 * time.Second) {
		t.Fatalf("flush failed: %+v", logdrop.ExportStatus())
	}
	spans := g.take()
	for _, s := range spans {
		for key, value := range s.Attributes {
			if text, ok := value.(string); ok {
				for _, p := range private {
					if strings.Contains(text, p) {
						t.Errorf("%q exports %s=%q", s.Name, key, text)
					}
				}
			}
		}
	}
	return spans
}

func find(t *testing.T, spans []otlptest.Span, kind int, match func(otlptest.Span) bool) otlptest.Span {
	t.Helper()
	for _, s := range spans {
		if s.Kind == kind && match(s) {
			return s
		}
	}
	for _, s := range spans {
		t.Logf("span %q kind %d scope %s attributes %v", s.Name, s.Kind, s.Scope, s.Attributes)
	}
	t.Fatalf("no matching span of kind %d", kind)
	return otlptest.Span{}
}

func get(t *testing.T, url string) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
}

func route(r string) func(otlptest.Span) bool {
	return func(s otlptest.Span) bool { return s.Attributes["http.route"] == r }
}

func TestIntegrations(t *testing.T) {
	if os.Getenv("LOGDROP_IT") != "1" {
		t.Skip("run scripts/integration.sh")
	}
	g := &gateway{}
	ingest := httptest.NewServer(g)
	defer ingest.Close()
	t.Setenv("LOGDROP_ENDPOINT", ingest.URL)
	t.Setenv("LOGDROP_ALLOW_INSECURE_TRANSPORT", "true")
	t.Setenv("LOGDROP_INGEST_KEY", "ldtk_integration")
	t.Setenv("LOGDROP_SERVICE", "integration")
	t.Setenv("LOGDROP_SCHEDULE_DELAY", "50ms")
	defer logdrop.Start()()
	ctx := context.Background()

	t.Run("gin", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		router := gin.New()
		router.Use(otelgin.Middleware("integration"))
		router.GET("/orders/:id", func(c *gin.Context) { c.Status(200) })
		server := httptest.NewServer(router)
		defer server.Close()
		get(t, server.URL+"/orders/7?card=4111")
		find(t, flush(t, g), 2, route("/orders/:id"))
	})

	t.Run("echo", func(t *testing.T) {
		e := echo.New()
		e.Use(echootelv4.NewMiddleware("integration"))
		e.GET("/orders/:id", func(c echo.Context) error { return c.NoContent(200) })
		server := httptest.NewServer(e)
		defer server.Close()
		get(t, server.URL+"/orders/7?card=4111")
		find(t, flush(t, g), 2, route("/orders/:id"))
	})

	t.Run("echo v5", func(t *testing.T) {
		e := echov5.New()
		e.Use(echootelv5.NewMiddleware("integration"))
		e.GET("/orders/:id", func(c *echov5.Context) error { return c.NoContent(200) })
		server := httptest.NewServer(e)
		defer server.Close()
		get(t, server.URL+"/orders/7?card=4111")
		find(t, flush(t, g), 2, route("/orders/:id"))
	})

	t.Run("gorilla", func(t *testing.T) {
		router := mux.NewRouter()
		router.Use(otelmux.Middleware("integration"))
		router.HandleFunc("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")
		server := httptest.NewServer(router)
		defer server.Close()
		get(t, server.URL+"/orders/7?card=4111")
		find(t, flush(t, g), 2, route("/orders/{id}"))
	})

	t.Run("chi", func(t *testing.T) {
		router := chi.NewRouter()
		router.Use(logdrop.Middleware(logdrop.RouteFrom(func(r *http.Request) string {
			return chi.RouteContext(r.Context()).RoutePattern()
		})))
		router.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {})
		server := httptest.NewServer(router)
		defer server.Close()
		get(t, server.URL+"/orders/7?card=4111")
		s := find(t, flush(t, g), 2, route("/orders/{id}"))
		if s.Name != "GET /orders/{id}" {
			t.Errorf("chi span %q", s.Name)
		}
	})

	t.Run("grpc", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
		healthpb.RegisterHealthServer(server, health.NewServer())
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatal(err)
		}
		spans := flush(t, g)
		client := find(t, spans, 3, func(s otlptest.Span) bool { return s.Attributes["rpc.system"] == "grpc" })
		serverSpan := find(t, spans, 2, func(s otlptest.Span) bool { return s.Attributes["rpc.system"] == "grpc" })
		if serverSpan.TraceID != client.TraceID {
			t.Error("gRPC server span is not in the client's trace")
		}
	})

	postgres := os.Getenv("LOGDROP_IT_POSTGRES")
	t.Run("pgx", func(t *testing.T) {
		config, err := pgxpool.ParseConfig(postgres)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.Tracer = otelpgx.NewTracer()
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		// pgx's tracer records queries inside a span, such as a request's.
		requestCtx, request := logdrop.StartSpan(ctx, "request")
		var n int
		err = pool.QueryRow(requestCtx, "SELECT count(*) FROM pg_class WHERE relname = 'secret-owner' OR relpages > 4111").Scan(&n)
		request.End()
		if err != nil {
			t.Fatal(err)
		}
		s := find(t, flush(t, g), 3, func(s otlptest.Span) bool {
			return strings.HasPrefix(stringOf(s.Attributes["db.query.text"]), "SELECT count(*)")
		})
		if s.Attributes["db.query.text"] != "SELECT count(*) FROM pg_class WHERE relname = ? OR relpages > ?" {
			t.Errorf("pgx statement %v", s.Attributes)
		}
	})

	t.Run("database/sql", func(t *testing.T) {
		db, err := logdropsql.Open("pgx", postgres)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_class WHERE relname = 'secret-owner'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		s := find(t, flush(t, g), 3, func(s otlptest.Span) bool { return s.Name == "SELECT" })
		if s.Attributes["db.system.name"] != "postgresql" || s.Attributes["server.address"] != "127.0.0.1" ||
			s.Attributes["db.query.text"] != "SELECT count(*) FROM pg_class WHERE relname = ?" {
			t.Errorf("database/sql span %v", s.Attributes)
		}
	})

	t.Run("redis", func(t *testing.T) {
		rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("LOGDROP_IT_REDIS")})
		if err := logdropredis.Instrument(rdb); err != nil {
			t.Fatal(err)
		}
		defer rdb.Close()
		if err := rdb.Set(ctx, "private-key", "private-body", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		spans := flush(t, g)
		s := find(t, spans, 3, func(s otlptest.Span) bool { return s.Attributes["db.operation.name"] == "SET" })
		if s.Name != "SET" || s.Attributes["db.system.name"] == nil && s.Attributes["db.system"] == nil {
			t.Errorf("redis span %q %v", s.Name, s.Attributes)
		}
		for _, span := range spans {
			if span.Name != "SET" {
				t.Errorf("connection set-up span %q", span.Name)
			}
		}
	})

	t.Run("kafka", func(t *testing.T) {
		broker := os.Getenv("LOGDROP_IT_KAFKA")
		var err error
		for i := 0; i < 30; i++ {
			var conn *kafka.Conn
			if conn, err = kafka.Dial("tcp", broker); err == nil {
				err = conn.CreateTopics(kafka.TopicConfig{Topic: "it-orders", NumPartitions: 1, ReplicationFactor: 1})
				conn.Close()
				if err == nil || strings.Contains(err.Error(), "already exists") {
					err = nil
					break
				}
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			t.Fatal(err)
		}
		writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "it-orders", BatchTimeout: 10 * time.Millisecond}
		defer writer.Close()
		sendCtx, checkout := logdrop.StartSpan(ctx, "checkout")
		err = logdropkafka.WriteMessages(sendCtx, writer, kafka.Message{Key: []byte("private-key"), Value: []byte("private-body")})
		checkout.End()
		if err != nil {
			t.Fatal(err)
		}
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: "it-orders", GroupID: "it", MaxWait: 100 * time.Millisecond})
		defer reader.Close()
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		message, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatal(err)
		}
		_ = logdropkafka.Process(ctx, message, func(context.Context) error { return nil })
		spans := flush(t, g)
		producer := find(t, spans, 4, func(s otlptest.Span) bool { return s.Name == "it-orders publish" })
		consumer := find(t, spans, 5, func(s otlptest.Span) bool { return s.Name == "it-orders process" })
		if consumer.TraceID != producer.TraceID || consumer.ParentSpanID != producer.SpanID ||
			producer.Attributes["messaging.destination.name"] != "it-orders" {
			t.Errorf("kafka trace: producer %v consumer %v", producer, consumer)
		}
	})

	t.Run("rabbitmq", func(t *testing.T) {
		var conn *amqp.Connection
		var err error
		for i := 0; i < 60; i++ {
			if conn, err = amqp.Dial(os.Getenv("LOGDROP_IT_RABBITMQ")); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		channel, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		if err := channel.ExchangeDeclare("it-orders", "direct", false, true, false, false, nil); err != nil {
			t.Fatal(err)
		}
		queue, err := channel.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := channel.QueueBind(queue.Name, "private-key", "it-orders", false, nil); err != nil {
			t.Fatal(err)
		}
		deliveries, err := channel.Consume(queue.Name, "", true, true, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := logdropamqp.Publish(ctx, channel, "it-orders", "private-key", false, false, amqp.Publishing{Body: []byte("private-body")}); err != nil {
			t.Fatal(err)
		}
		select {
		case delivery := <-deliveries:
			_ = logdropamqp.Process(ctx, delivery, func(context.Context) error { return nil })
		case <-time.After(10 * time.Second):
			t.Fatal("no delivery")
		}
		spans := flush(t, g)
		producer := find(t, spans, 4, func(s otlptest.Span) bool { return s.Name == "it-orders publish" })
		consumer := find(t, spans, 5, func(s otlptest.Span) bool { return s.Name == "it-orders process" })
		if consumer.ParentSpanID != producer.SpanID || producer.Attributes["peer.service"] != "RabbitMQ" {
			t.Errorf("rabbitmq trace: producer %v consumer %v", producer.Attributes, consumer.ParentSpanID)
		}
	})
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}
