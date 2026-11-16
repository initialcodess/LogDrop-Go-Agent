package logdrop

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// HandlerOption configures Handler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	route func(*http.Request) string
}

// RouteFrom gives Handler or Middleware the route template of a request for routers other than net/http's ServeMux,
// read after the router has handled the request (see Middleware for chi).
func RouteFrom(route func(*http.Request) string) HandlerOption {
	return func(c *handlerConfig) { c.route = route }
}

// patternRoute is the path part of a ServeMux pattern ("GET example.com/orders/{id}" is "/orders/{id}").
func patternRoute(pattern string) string {
	if _, rest, found := strings.Cut(pattern, " "); found {
		pattern = strings.TrimLeft(rest, " \t")
	}
	if i := strings.Index(pattern, "/"); i > 0 {
		pattern = pattern[i:]
	}
	return pattern
}

// Handler traces the requests handler serves: one server span per request, named by the method and the route
// template ("GET /orders/{id}") from net/http's ServeMux or RouteFrom, and continued from the caller's trace.
func Handler(handler http.Handler, options ...HandlerOption) http.Handler {
	config := handlerConfig{}
	for _, option := range options {
		option(&config)
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		route := ""
		if config.route != nil {
			route = config.route(r)
		} else if r.Pattern != "" {
			route = patternRoute(r.Pattern)
		}
		if route != "" {
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
	})
	// otelhttp names the span again after the request with the same formatter, once ServeMux has set the pattern.
	return otelhttp.NewHandler(inner, "http.server", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		if r.Pattern != "" {
			return r.Method + " " + patternRoute(r.Pattern)
		}
		return r.Method
	}))
}

// Middleware is Handler as router middleware, for routers that know the route only inside their own handling, such
// as chi:
//
//	router.Use(logdrop.Middleware(logdrop.RouteFrom(func(r *http.Request) string {
//		return chi.RouteContext(r.Context()).RoutePattern()
//	})))
func Middleware(options ...HandlerOption) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return Handler(next, options...) }
}

// Transport traces the requests sent through base (http.DefaultTransport when nil) and passes the trace on to the
// services called.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base,
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method }))
}

// HTTPClient returns a client whose requests are traced.
func HTTPClient() *http.Client { return &http.Client{Transport: Transport(nil)} }
