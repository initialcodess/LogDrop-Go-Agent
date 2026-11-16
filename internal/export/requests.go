package export

import (
	"sort"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
)

// LogDrop's trace admission limits, which refuse a whole request with 400: at most 256 spans and at most 32 distinct
// span start minutes per request, and no span starting more than 5 minutes ahead or more than 7 days back. The same
// rules as the Java core's AdmissionBoundedExporter.
const (
	MaxSpans       = 256
	MaxMinutes     = 32
	minuteNanos    = int64(60_000_000_000)
	maxFutureNanos = 5 * minuteNanos
	// A little inside the 7-day bound, so queueing and retries cannot carry a span past it on the way.
	maxAgeNanos = 7*24*60*minuteNanos - 10*minuteNanos
)

// Split returns the requests one batch is sent as, in start-minute order, and how many spans were left out.
func Split(spans []core.SpanData, nowUnixNano int64) ([][]core.SpanData, int) {
	byMinute := map[int64][]core.SpanData{}
	kept := 0
	for _, span := range spans {
		start := span.StartUnixNano
		if start <= 0 || start-nowUnixNano > maxFutureNanos || nowUnixNano-start > maxAgeNanos {
			continue
		}
		minute := start / minuteNanos
		byMinute[minute] = append(byMinute[minute], span)
		kept++
	}
	dropped := len(spans) - kept
	if dropped == 0 && kept <= MaxSpans && len(byMinute) <= MaxMinutes {
		if kept == 0 {
			return nil, dropped
		}
		return [][]core.SpanData{spans}, dropped
	}
	minutes := make([]int64, 0, len(byMinute))
	for m := range byMinute {
		minutes = append(minutes, m)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i] < minutes[j] })
	var requests [][]core.SpanData
	var current []core.SpanData
	count := 0
	for _, minute := range minutes {
		if count == MaxMinutes {
			requests = append(requests, current)
			current, count = nil, 0
		}
		count++
		for _, span := range byMinute[minute] {
			if len(current) == MaxSpans {
				requests = append(requests, current)
				current, count = nil, 1
			}
			current = append(current, span)
		}
	}
	if len(current) > 0 {
		requests = append(requests, current)
	}
	return requests, dropped
}
