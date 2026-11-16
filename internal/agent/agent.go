package agent

import (
	"fmt"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/export"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// AbandonedSpan is how long an open span counts against the active span limit.
const AbandonedSpan = 15 * time.Minute

// Agent is a running LogDrop pipeline.
type Agent struct {
	Settings  *Settings
	Provider  *sdktrace.TracerProvider
	admission *Admission
	queue     *export.Queue
	exporter  *export.Exporter
	keys      *export.KeySource
}

// Status holds the export counters.
type Status struct {
	Exported, Failed, Rejected, Pending, Refused, Active, Abandoned int64
	Closed                                                          bool
}

// New builds the pipeline from settings, or returns the reason it stays off. transport replaces the HTTP sender in
// tests.
func New(settings *Settings, diagnostics *diag.Diagnostics, transport export.Transport) (*Agent, error) {
	diagnostics.SetDebug(settings.Debug)
	var keys *export.KeySource
	if settings.KeyFile != "" {
		keys = export.FileKey(settings.KeyFile, diagnostics, nil)
	} else {
		keys = export.FixedKey(settings.InlineKey)
	}
	timeout := time.Duration(settings.ExportTimeoutMillis) * time.Millisecond
	if transport == nil {
		sender, err := export.NewHTTPSender(settings.TracesEndpoint, settings.Compression, timeout, settings.CAFile,
			Distribution+"/"+Version)
		if err != nil {
			return nil, fmt.Errorf("transport: %w", err)
		}
		transport = sender
	}
	resourceAttributes := BuildResource(settings, Detect())
	exporter := export.NewExporter(transport, keys, export.NewRetrySchedule(nil, nil, 0), diagnostics,
		settings.Compression, timeout, resourceAttributes, nil)
	queue := export.NewQueue(exporter, settings.MaxQueueSize, settings.MaxExportBatchSize,
		time.Duration(settings.ScheduleDelayMillis)*time.Millisecond)
	admission := NewAdmission(settings.Limits.MaxActiveSpans, AbandonedSpan, func() {
		diagnostics.Warn("admission", "LogDrop active span limit reached. New spans are being refused.")
	}, nil)
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(admission),
		sdktrace.WithSpanProcessor(NewProcessor(admission, settings.Limits, queue)),
		// LogDrop's identity is the exporter's own resource; the SDK's detectors and OTEL_RESOURCE_ATTRIBUTES are left out.
		sdktrace.WithResource(resource.Empty()),
	)
	return &Agent{Settings: settings, Provider: provider, admission: admission, queue: queue, exporter: exporter, keys: keys}, nil
}

// Flush sends the spans that ended before the call; true when all of them were accepted in time.
func (a *Agent) Flush(timeout time.Duration) bool {
	if a.queue.IsShutdown() {
		return false
	}
	failures, rejections := a.exporter.Failed(), a.exporter.Rejected()
	done := a.queue.Flush(timeout)
	return done && a.exporter.Failed() == failures && a.exporter.Rejected() == rejections &&
		a.exporter.Pending() == 0 && a.queue.Pending() == 0
}

// Status returns the export counters.
func (a *Agent) Status() Status {
	return Status{
		Exported:  a.exporter.Exported(),
		Failed:    a.exporter.Failed(),
		Rejected:  a.exporter.Rejected(),
		Pending:   a.exporter.Pending() + int64(a.queue.Pending()),
		Refused:   a.admission.Refused(),
		Active:    int64(a.admission.Active()),
		Abandoned: a.admission.Abandoned(),
		Closed:    a.queue.IsShutdown(),
	}
}

// Update codes, the same as the other agents.
const (
	Updated = 0
	Invalid = 1
	Closed  = 3
)

// UpdateIngestCredential uses credential for later requests until the key file changes again.
func (a *Agent) UpdateIngestCredential(credential string) int {
	if a.queue.IsShutdown() {
		return Closed
	}
	if !core.IsValidCredential(credential) {
		return Invalid
	}
	a.keys.Override(credential)
	return Updated
}
