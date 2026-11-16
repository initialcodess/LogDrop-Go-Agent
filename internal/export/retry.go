package export

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
)

// Retry timing, the same as the Java core's ConnectionRetry and the Node.js, Python and .NET agents.
const (
	RetryWindow             = 20 * time.Second
	InitialBackoff          = 500 * time.Millisecond
	MaxBackoff              = 4 * time.Second
	ClosingWindow           = 8 * time.Second
	ClosingMaxBackoff       = time.Second
	AnswerInitialBackoff    = time.Second
	AnswerMaxBackoff        = 5 * time.Second
	AnswerBackoffMultiplier = 1.5
	AnswerMaxAttempts       = 5
)

// RetriedAnswer reports the answers that are sent again: 429, 502, 503 and 504.
func RetriedAnswer(status int) bool {
	return status == 429 || status == 502 || status == 503 || status == 504
}

// RetrySchedule decides when an export request is sent again, always with the same request bytes:
//
//   - No response (connection refused or reset, a timeout before the status line, an unknown host, no route, a TLS
//     handshake cut off by the network): resent after 0.5 s, 1 s, 2 s and then every 4 s (each ±20 %), the last time
//     when 20 s have passed since the batch's first attempt. A timeout after the request was written is resent once.
//   - 429, 502, 503 or 504: resent after 1 s, then x1.5 up to 5 s (±20 %, at most 5 attempts) while the next attempt
//     starts inside the export timeout, counted from the attempt that got the first such answer.
//
// Any other answer is final, and so is a failure that waiting cannot fix (certificate or protocol). At shutdown
// (closing) requests waiting after a failure go at once and then at most 1 s apart, for at most 8 s.
type RetrySchedule struct {
	clock         func() time.Duration
	jitter        func() float64
	window        time.Duration
	mu            sync.Mutex
	closing       bool
	closingAt     time.Duration
	stopped       bool
	closingSignal chan struct{}
	stoppedSignal chan struct{}
}

// NewRetrySchedule uses the monotonic clock and ±20 % jitter unless given others.
func NewRetrySchedule(clock func() time.Duration, jitter func() float64, window time.Duration) *RetrySchedule {
	if clock == nil {
		clock = diag.Monotonic
	}
	if jitter == nil {
		jitter = func() float64 { return 0.8 + rand.Float64()*0.4 }
	}
	if window <= 0 {
		window = RetryWindow
	}
	return &RetrySchedule{clock: clock, jitter: jitter, window: window,
		closingSignal: make(chan struct{}), stoppedSignal: make(chan struct{})}
}

// Now is the schedule's clock.
func (r *RetrySchedule) Now() time.Duration { return r.clock() }

// Stopped reports that the exporter is closed.
func (r *RetrySchedule) Stopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

// WindowOver reports that the retry window of a batch has passed.
func (r *RetrySchedule) WindowOver(windowStart time.Duration) bool {
	return r.Now() >= windowStart+r.window
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func fromMillis(ms float64) time.Duration { return time.Duration(ms * float64(time.Millisecond)) }

// Delay is the wait before retry number retries (0 for the first) of a request without a response; false when there
// is none.
func (r *RetrySchedule) Delay(windowStart time.Duration, retries int) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return 0, false
	}
	limit := MaxBackoff
	if r.closing {
		limit = ClosingMaxBackoff
	}
	backoff := math.Min(millis(InitialBackoff)*math.Pow(2, float64(min(retries, 20))), millis(limit))
	delay := fromMillis(backoff * r.jitter())
	deadline := windowStart + r.window
	if r.closing && r.closingAt < deadline {
		deadline = r.closingAt
	}
	left := deadline - r.Now()
	// The last attempt goes when the window ends, unless that is closer than the shortest backoff.
	if delay <= left {
		return delay, true
	}
	shortest := min(InitialBackoff, limit)
	if left < shortest {
		return 0, false
	}
	return left, true
}

// AnswerDelay is the wait after the answered-th 429/502/503/504; false when the answer is final.
func (r *RetrySchedule) AnswerDelay(answeredSince, timeout, windowStart time.Duration, answered int) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || answered >= AnswerMaxAttempts {
		return 0, false
	}
	backoff := math.Min(millis(AnswerInitialBackoff)*math.Pow(AnswerBackoffMultiplier, float64(answered-1)), millis(AnswerMaxBackoff))
	delay := fromMillis(backoff * r.jitter())
	upcoming := r.Now() + delay
	if upcoming-answeredSince >= timeout || upcoming > windowStart+r.window || (r.closing && upcoming > r.closingAt) {
		return 0, false
	}
	return delay, true
}

// Wait waits delay. True to send again, false when the exporter stopped while waiting. A wait after a failure ends at
// once when shutdown starts.
func (r *RetrySchedule) Wait(delay time.Duration, afterFailure bool) bool {
	var closing <-chan struct{}
	if afterFailure {
		closing = r.closingSignal
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.stoppedSignal:
		return false
	case <-closing:
		return !r.Stopped()
	case <-timer.C:
		return !r.Stopped()
	}
}

// Closing starts shutdown: requests waiting after a failure go now, then at most 1 s apart, for at most 8 s.
func (r *RetrySchedule) Closing() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return
	}
	r.closingAt = r.Now() + ClosingWindow
	r.closing = true
	close(r.closingSignal)
}

// Stop closes the exporter: what still waits ends now with its last outcome; nothing is resent any more.
func (r *RetrySchedule) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	close(r.stoppedSignal)
}
