package export

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
)

type outcome int

const (
	exported outcome = iota
	rejected
	failed
)

// Exporter is the only exporter of the agent, in the Java agent's order: requests kept inside LogDrop's admission
// limits (split by start minute, spans outside the clock window dropped and counted as rejected), one request at a
// time, each serialized once and resent with the same bytes and the current ingest key (RetrySchedule). It counts
// exported, failed, rejected (HTTP 400, never retried, or dropped before export) and pending spans for flush and
// export status.
type Exporter struct {
	transport   Transport
	keys        *KeySource
	retry       *RetrySchedule
	diagnostics *diag.Diagnostics
	gzip        bool
	timeout     time.Duration
	now         func() int64
	resource    []core.SafeAttribute
	exported    atomic.Int64
	failed      atomic.Int64
	rejected    atomic.Int64
	pending     atomic.Int64
	closed      atomic.Bool
}

// NewExporter sends through transport with the keys and the resource of this process.
func NewExporter(transport Transport, keys *KeySource, retry *RetrySchedule, diagnostics *diag.Diagnostics,
	compression string, timeout time.Duration, resource []core.SafeAttribute, now func() int64) *Exporter {
	if now == nil {
		now = func() int64 { return time.Now().UnixNano() }
	}
	return &Exporter{transport: transport, keys: keys, retry: retry, diagnostics: diagnostics,
		gzip: compression == "gzip", timeout: timeout, now: now, resource: resource}
}

// Exported counts accepted spans.
func (e *Exporter) Exported() int64 { return e.exported.Load() }

// Failed counts spans that could not be sent.
func (e *Exporter) Failed() int64 { return e.failed.Load() }

// Rejected counts spans LogDrop refused as invalid, or dropped because it would.
func (e *Exporter) Rejected() int64 { return e.rejected.Load() }

// Pending counts spans in a request being sent.
func (e *Exporter) Pending() int64 { return e.pending.Load() }

func (e *Exporter) count(o outcome, n int) {
	switch o {
	case exported:
		e.exported.Add(int64(n))
	case rejected:
		e.rejected.Add(int64(n))
	default:
		e.failed.Add(int64(n))
	}
}

// Export sends one batch from the queue's worker; true when every span was accepted.
func (e *Exporter) Export(spans []core.SpanData) bool {
	if e.closed.Load() {
		e.count(failed, len(spans))
		return false
	}
	if e.keys.Current() == "" {
		e.diagnostics.Warn("key-missing", "LogDrop ingest key is not available. Trace export skipped.")
		e.count(failed, len(spans))
		return false
	}
	requests, dropped := Split(spans, e.now())
	e.count(rejected, dropped)
	// The requests of one batch share one retry window; one whose turn comes after it fails unsent.
	var window *time.Duration
	succeeded := dropped == 0
	for _, request := range requests {
		e.pending.Add(int64(len(request)))
		o := e.sendSafely(request, &window)
		e.pending.Add(-int64(len(request)))
		e.count(o, len(request))
		succeeded = succeeded && o == exported
	}
	return succeeded
}

func (e *Exporter) sendSafely(spans []core.SpanData, window **time.Duration) (o outcome) {
	defer func() {
		if recover() != nil {
			o = failed
		}
	}()
	return e.send(spans, window)
}

func (e *Exporter) body(spans []core.SpanData) []byte {
	serialized := Encode(spans, e.resource)
	if !e.gzip {
		return serialized
	}
	var buffer bytes.Buffer
	w := gzip.NewWriter(&buffer)
	_, _ = w.Write(serialized)
	_ = w.Close()
	return buffer.Bytes()
}

func (e *Exporter) send(spans []core.SpanData, window **time.Duration) outcome {
	body := e.body(spans)
	retries, answered := 0, 0
	var answeredSince *time.Duration
	resentAfterWrite := false
	var last *Attempt
	for {
		if *window != nil && e.retry.WindowOver(**window) {
			break
		}
		key := e.keys.Current()
		if key == "" {
			e.diagnostics.Warn("key-missing", "LogDrop ingest key is not available. Trace export skipped.")
			break
		}
		attemptStart := e.retry.Now()
		if *window == nil {
			started := attemptStart
			*window = &started
		}
		attempt := e.transport.Send(body, key)
		last = &attempt
		if attempt.Answered() {
			if RetriedAnswer(attempt.Status) {
				answered++
				if answeredSince == nil {
					since := attemptStart
					answeredSince = &since
				}
				if delay, ok := e.retry.AnswerDelay(*answeredSince, e.timeout, **window, answered); ok && e.retry.Wait(delay, false) {
					continue
				}
			}
			break
		}
		if attempt.NoResponse && !e.retry.Stopped() {
			// A timeout after the whole request was written may still be processed: resent once only.
			timedOutAfterWrite := attempt.Code == "LOGDROP_TIMEOUT" && attempt.AfterWrite
			if !(timedOutAfterWrite && resentAfterWrite) {
				if delay, ok := e.retry.Delay(**window, retries); ok {
					if timedOutAfterWrite {
						resentAfterWrite = true
					}
					retries++
					if e.retry.Wait(delay, true) {
						continue
					}
				}
			}
		}
		break
	}
	return e.result(last)
}

func (e *Exporter) result(last *Attempt) outcome {
	if last == nil {
		return failed
	}
	if !last.Answered() {
		// Only once the request has failed for good, after its retries.
		e.diagnostics.Warn("unreachable", "LogDrop is not reachable. Trace export failed.")
		e.diagnostics.Debug("Trace export failed: " + last.Code + ".")
		return failed
	}
	status := last.Status
	if status >= 200 && status < 300 {
		return exported
	}
	if status == 401 || status == 403 {
		e.keys.Rejected()
	}
	switch status {
	case 401:
		e.diagnostics.Warn("http-401", "LogDrop rejected the ingest key (HTTP 401). Check that the key is active and not expired.")
	case 403:
		e.diagnostics.Warn("http-403", "LogDrop refused traces for this project (HTTP 403). Check that tracing is enabled for the project.")
	case 400:
		e.diagnostics.Warn("http-400", "LogDrop refused a trace batch as invalid (HTTP 400). Its spans were dropped, not retried.")
		return rejected
	case 429:
		e.diagnostics.Warn("http-429", "LogDrop rate-limited trace export (HTTP 429).")
	default:
		e.diagnostics.Warn(fmt.Sprintf("http-%d", status), fmt.Sprintf("LogDrop rejected trace export (HTTP %d).", status))
	}
	return failed
}

// Closing starts shutdown: a request waiting after a failure goes now (the queue flushes next).
func (e *Exporter) Closing() { e.retry.Closing() }

// Close stops retries and closes the transport.
func (e *Exporter) Close() {
	e.closed.Store(true)
	e.retry.Stop()
	e.transport.Close()
}

// String never shows the key.
func (e *Exporter) String() string { return "Exporter(redacted)" }
