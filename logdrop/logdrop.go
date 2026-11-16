// Package logdrop traces a Go service for LogDrop. Call Start once at the beginning of main and defer the function
// it returns; it reads the LOGDROP_* settings (on Kubernetes the LogDrop trace injector sets them), sets
// OpenTelemetry's global tracer provider and W3C propagators, and sends the spans of every OpenTelemetry Go
// instrumentation in the process to LogDrop, after removing everything that could carry personal or secret data.
//
//	func main() {
//		defer logdrop.Start()()
//		http.ListenAndServe(":8080", logdrop.Handler(mux))
//	}
//
// Without LOGDROP_ENDPOINT, with invalid settings or with LOGDROP_ENABLED=false the service runs untraced, with one
// line on standard error that says why; no call of this package ever fails the service.
package logdrop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/agent"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Version is the LogDrop Go agent's version.
const Version = agent.Version

var (
	mu          sync.Mutex
	started     bool
	current     *agent.Agent
	diagnostics = diag.New(nil, nil)
)

// Start starts tracing once and returns Shutdown; later calls only return Shutdown. Call it at the beginning of main:
//
//	defer logdrop.Start()()
func Start() func() {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return Shutdown
	}
	started = true
	defer func() {
		if recover() != nil {
			current = nil
			diagnostics.Error("LogDrop tracing is disabled: the agent could not start.")
		}
	}()
	env := agent.Environment()
	if reason := agent.DisabledReason(env); reason != "" {
		diagnostics.Info("LogDrop tracing is disabled: " + reason)
		return Shutdown
	}
	settings := agent.ParseSettings(env)
	if !settings.Valid() {
		diagnostics.Error("LogDrop tracing is disabled: " + strings.Join(settings.Problems, " "))
		return Shutdown
	}
	if own := fmt.Sprintf("%T", otel.GetTracerProvider()); own != "*global.tracerProvider" {
		diagnostics.Error("LogDrop tracing is disabled: the application sets its own OpenTelemetry tracer provider.")
		return Shutdown
	}
	a, err := agent.New(settings, diagnostics, nil)
	if err != nil {
		diagnostics.Error("LogDrop tracing is disabled: the agent could not start.")
		return Shutdown
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { diagnostics.Debug("OpenTelemetry: " + err.Error()) }))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(a.Provider)
	current = a
	diagnostics.Info(fmt.Sprintf("LogDrop Go agent %s is tracing service %q (environment %q) to %s.",
		Version, settings.Service, settings.Environment, settings.EndpointHost))
	return Shutdown
}

func running() *agent.Agent {
	mu.Lock()
	defer mu.Unlock()
	return current
}

// Shutdown sends the spans that are still queued, for at most 10 seconds, and stops tracing. Spans that end later
// are not sent.
func Shutdown() {
	if a := running(); a != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = a.Provider.Shutdown(ctx)
	}
}

// IsAgentAttached reports whether tracing runs in this process.
func IsAgentAttached() bool { return running() != nil }

// Flush sends the spans that ended before the call, for example before a batch job exits. It returns false when it
// did not finish in time, an export failed, or tracing does not run. The timeout is more than 0 and at most 60
// seconds; another timeout returns false.
func Flush(timeout time.Duration) bool {
	if timeout <= 0 || timeout > time.Minute {
		return false
	}
	a := running()
	return a != nil && a.Flush(timeout)
}

// Status holds the export counters of ExportStatus.
type Status struct {
	// AgentAttached is false when tracing does not run; the counters are then 0.
	AgentAttached bool
	// ExportedSpans were accepted by LogDrop.
	ExportedSpans int64
	// FailedSpans could not be sent.
	FailedSpans int64
	// RejectedSpans were refused as invalid, or dropped before sending because LogDrop would refuse them.
	RejectedSpans int64
	// PendingSpans wait to be sent.
	PendingSpans int64
	// RefusedSpans were not recorded because the active span limit was reached.
	RefusedSpans int64
	// ActiveSpans are open now.
	ActiveSpans int64
	// Closed is true after Shutdown.
	Closed bool
}

// ExportStatus returns the export counters.
func ExportStatus() Status {
	a := running()
	if a == nil {
		return Status{}
	}
	s := a.Status()
	return Status{AgentAttached: true, ExportedSpans: s.Exported, FailedSpans: s.Failed, RejectedSpans: s.Rejected,
		PendingSpans: s.Pending, RefusedSpans: s.Refused, ActiveSpans: s.Active, Closed: s.Closed}
}

// Errors of UpdateIngestCredential.
var (
	ErrNotAttached       = errors.New("logdrop: tracing does not run in this process")
	ErrInvalidCredential = errors.New("logdrop: not a valid ingest key")
	ErrClosed            = errors.New("logdrop: tracing is shut down")
)

// UpdateIngestCredential uses key for later requests, without a restart, until the key file changes again.
func UpdateIngestCredential(key string) error {
	a := running()
	if a == nil {
		return ErrNotAttached
	}
	switch a.UpdateIngestCredential(key) {
	case agent.Invalid:
		return ErrInvalidCredential
	case agent.Closed:
		return ErrClosed
	}
	return nil
}
