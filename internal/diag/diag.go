// Package diag writes the agent's one-line messages on standard error. Warnings repeat at most once a minute per
// key; configured values and keys are never printed.
package diag

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Interval is how often one warning may repeat.
const Interval = time.Minute

var start = time.Now()

// Monotonic is the time since the process loaded the agent, on the monotonic clock.
func Monotonic() time.Duration { return time.Since(start) }

// Diagnostics writes the agent's messages.
type Diagnostics struct {
	write func(string)
	clock func() time.Duration
	mu    sync.Mutex
	last  map[string]time.Duration
	debug atomic.Bool
}

// New returns diagnostics that write to w (standard error when nil) with the given clock (Monotonic when nil).
func New(w func(string), clock func() time.Duration) *Diagnostics {
	if w == nil {
		w = func(line string) { fmt.Fprintln(os.Stderr, line) }
	}
	if clock == nil {
		clock = Monotonic
	}
	return &Diagnostics{write: w, clock: clock, last: map[string]time.Duration{}}
}

// SetDebug turns the debug lines on or off.
func (d *Diagnostics) SetDebug(on bool) { d.debug.Store(on) }

// Info writes an information line.
func (d *Diagnostics) Info(message string) { d.write("[logdrop-agent] INFO " + message) }

// Error writes an error line.
func (d *Diagnostics) Error(message string) { d.write("[logdrop-agent] ERROR " + message) }

// Warn writes a warning, at most once a minute per key.
func (d *Diagnostics) Warn(key, message string) {
	now := d.clock()
	d.mu.Lock()
	if previous, ok := d.last[key]; ok && now-previous < Interval {
		d.mu.Unlock()
		return
	}
	d.last[key] = now
	d.mu.Unlock()
	d.write("[logdrop-agent] WARN " + message)
}

// Debug writes agent internals with LOGDROP_DEBUG=true, never span contents or keys.
func (d *Diagnostics) Debug(message string) {
	if d.debug.Load() {
		d.write("[logdrop-agent] DEBUG " + message)
	}
}
