// LogDrop Go demo: a net/http service traced with the LogDrop Go package, the Go counterpart of the other agents'
// demos. logdrop.Start reads the LOGDROP_* settings (on Kubernetes the trace injector sets them); without them the
// demo runs untraced. Every integration is optional:
//
//	PORT (28120), REDIS_URL (host:port, REDIS_PASSWORD), DATABASE_URL (postgres://...), DEMO_SPRING_URL (a Java demo,
//	for one trace across both), DEMO_SCHEDULED_DELAY and DEMO_TRAFFIC_INTERVAL (seconds, 0 = off).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/logdrop"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropredis"
	"github.com/initialcodess/LogDrop-Go-Agent/logdropsql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func seconds(name string) time.Duration {
	n, _ := strconv.Atoi(setting(name, "0"))
	return time.Duration(n) * time.Second
}

type demo struct {
	port   string
	redis  *redis.Client
	db     *sql.DB
	client *http.Client
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// openDatabase creates and seeds a small orders table; every query passes its values as parameters.
func openDatabase(ctx context.Context, url string) (*sql.DB, error) {
	db, err := logdropsql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo_orders (
		id SERIAL PRIMARY KEY, customer TEXT NOT NULL, total NUMERIC(10, 2) NOT NULL, created_at TIMESTAMPTZ DEFAULT now())`); err != nil {
		return nil, err
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM demo_orders").Scan(&count); err != nil {
		return nil, err
	}
	if count == 0 {
		if _, err := db.ExecContext(ctx, "INSERT INTO demo_orders (customer, total) VALUES ($1, $2), ($3, $4)",
			"ayse", 42.5, "mehmet", 17); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (d *demo) recentOrders(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, total FROM demo_orders ORDER BY id DESC LIMIT $1", min(max(limit, 1), 50))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orders []map[string]any
	for rows.Next() {
		var id int
		var total float64
		if err := rows.Scan(&id, &total); err != nil {
			return nil, err
		}
		orders = append(orders, map[string]any{"id": id, "total": total})
	}
	return orders, rows.Err()
}

// failure is returned by /trace/fail; only its type (*main.failure) is exported, never its message.
type failure struct{ message string }

func (f *failure) Error() string { return f.message }

func (d *demo) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { reply(w, 200, map[string]string{"status": "up"}) })

	// Custom spans with an event and a child, a Redis round trip and, when configured, the Java demo downstream.
	mux.HandleFunc("GET /trace/demo", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := logdrop.StartSpan(r.Context(), "demo.sdk.operation")
		defer span.End()
		span.Attribute("demo.kind", "go").Event("operation.started")
		_ = logdrop.InSpan(ctx, "demo.sdk.child", func(ctx context.Context, child *logdrop.Span) error {
			if d.redis != nil {
				d.redis.Set(ctx, "logdrop:go-demo:last", time.Now().Unix(), 0)
			}
			child.Event("child.completed")
			return nil
		})
		var downstream any
		if spring := os.Getenv("DEMO_SPRING_URL"); spring != "" {
			request, _ := http.NewRequestWithContext(ctx, "GET", spring+"/trace/demo", nil)
			if response, err := d.client.Do(request); err == nil {
				response.Body.Close()
				downstream = response.StatusCode
			}
		}
		span.Success()
		reply(w, 200, map[string]any{"traceId": span.Context().TraceID, "downstream": downstream})
	})

	mux.HandleFunc("GET /trace/redis", func(w http.ResponseWriter, r *http.Request) {
		if d.redis == nil {
			reply(w, 503, map[string]string{"error": "REDIS_URL is not set"})
			return
		}
		count, err := d.redis.Incr(r.Context(), "logdrop:go-demo:counter").Result()
		if err != nil {
			reply(w, 502, map[string]string{"error": "redis"})
			return
		}
		reply(w, 200, map[string]int64{"count": count})
	})

	mux.HandleFunc("GET /trace/database", func(w http.ResponseWriter, r *http.Request) {
		if d.db == nil {
			reply(w, 503, map[string]string{"error": "DATABASE_URL is not set"})
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit == 0 {
			limit = 5
		}
		orders, err := d.recentOrders(r.Context(), limit)
		if err != nil {
			reply(w, 502, map[string]string{"error": "database"})
			return
		}
		reply(w, 200, map[string]any{"orders": orders})
	})

	mux.HandleFunc("GET /trace/status/{code}", func(w http.ResponseWriter, r *http.Request) {
		code, err := strconv.Atoi(r.PathValue("code"))
		if err != nil || code < 200 || code > 599 {
			code = 400
		}
		reply(w, code, map[string]int{"code": code})
	})

	mux.HandleFunc("GET /trace/fail", func(w http.ResponseWriter, r *http.Request) {
		err := logdrop.InSpan(r.Context(), "demo.failing.step", func(context.Context, *logdrop.Span) error {
			return &failure{"intentional failure (its message is never exported)"}
		})
		reply(w, 500, map[string]string{"error": fmt.Sprintf("%T", err)})
	})

	mux.HandleFunc("GET /trace/downstream", func(w http.ResponseWriter, r *http.Request) {
		target := "/trace/status/200"
		if r.URL.Query().Get("fail") == "true" {
			target = "/trace/status/503"
		}
		request, _ := http.NewRequestWithContext(r.Context(), "GET", "http://127.0.0.1:"+d.port+target, nil)
		response, err := d.client.Do(request)
		if err != nil {
			reply(w, 502, map[string]string{"error": "downstream"})
			return
		}
		response.Body.Close()
		reply(w, response.StatusCode, map[string]int{"downstream": response.StatusCode})
	})
	return logdrop.Handler(mux)
}

// scheduled runs work outside any request, like the plain Java demo.
func (d *demo) scheduled(ctx context.Context, every time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		_ = logdrop.InSpan(ctx, "demo.scheduled.work", func(ctx context.Context, span *logdrop.Span) error {
			if d.db != nil {
				if orders, err := d.recentOrders(ctx, 1); err == nil {
					span.Attribute("demo.orders", len(orders))
				}
			}
			if d.redis != nil {
				d.redis.Get(ctx, "logdrop:go-demo:last")
			}
			return nil
		})
	}
}

// traffic calls one route per tick, in turn. Routes whose dependency is not configured are left out, so their 503
// answers do not show up as errors in APM; the intentional errors stay.
func (d *demo) traffic(ctx context.Context, every time.Duration) {
	routes := []string{"/trace/demo"}
	if d.redis != nil {
		routes = append(routes, "/trace/redis")
	}
	if d.db != nil {
		routes = append(routes, "/trace/database")
	}
	routes = append(routes, "/trace/status/404", "/trace/fail", "/trace/downstream?fail=true")
	for tick := 0; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		request, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:"+d.port+routes[tick%len(routes)], nil)
		if response, err := d.client.Do(request); err == nil {
			response.Body.Close()
		}
	}
}

func main() {
	defer logdrop.Start()()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	d := &demo{port: setting("PORT", "28120"), client: logdrop.HTTPClient()}
	d.client.Timeout = 10 * time.Second
	if address := os.Getenv("REDIS_URL"); address != "" {
		d.redis = redis.NewClient(&redis.Options{Addr: address, Password: os.Getenv("REDIS_PASSWORD")})
		if err := logdropredis.Instrument(d.redis); err != nil {
			fmt.Fprintln(os.Stderr, "redis tracing:", err)
		}
	}
	if url := os.Getenv("DATABASE_URL"); url != "" {
		db, err := openDatabase(ctx, url)
		if err != nil {
			fmt.Fprintln(os.Stderr, "database:", err)
			os.Exit(1)
		}
		d.db = db
	}
	server := &http.Server{Addr: ":" + d.port, Handler: d.routes(), ReadHeaderTimeout: 10 * time.Second}
	if every := seconds("DEMO_SCHEDULED_DELAY"); every > 0 {
		go d.scheduled(ctx, every)
	}
	if every := seconds("DEMO_TRAFFIC_INTERVAL"); every > 0 {
		go d.traffic(ctx, every)
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Println("LogDrop Go demo listening on :" + d.port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
	}
}
