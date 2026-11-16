package core

import (
	"errors"
	"regexp"
)

// LimitError reports a LogDrop limit, endpoint or identity value outside its rules (IllegalArgumentException in Java).
type LimitError struct{ Message string }

func (e *LimitError) Error() string { return e.Message }

// IsLimitError reports whether err is a LimitError.
func IsLimitError(err error) bool {
	var limit *LimitError
	return errors.As(err, &limit)
}

func require(condition bool, message string) error {
	if !condition {
		return &LimitError{message}
	}
	return nil
}

var customKey = regexp.MustCompile(`\A[a-zA-Z][a-zA-Z0-9_.\-]{0,127}\z`)

// TracingLimits are the recording limits, the same as the Java core's TracingLimits. Excess data is dropped, never
// sampled.
type TracingLimits struct {
	MaxActiveSpans  int
	MaxAttributes   int
	MaxEvents       int
	MaxLinks        int
	MaxStringLength int
	AllowedKeys     map[string]struct{}
}

// NewTracingLimits checks every limit against its range.
func NewTracingLimits(maxActiveSpans, maxAttributes, maxEvents, maxLinks, maxStringLength int, allowed []string) (TracingLimits, error) {
	checks := []error{
		require(inRange(maxActiveSpans, 1, 100_000), "Invalid LogDrop active span limit"),
		require(inRange(maxAttributes, 1, 128), "Invalid LogDrop attribute limit"),
		require(inRange(maxEvents, 1, 128), "Invalid LogDrop event limit"),
		require(inRange(maxLinks, 1, 128), "Invalid LogDrop link limit"),
		require(inRange(maxStringLength, 1, 4096), "Invalid LogDrop string length limit"),
	}
	for _, err := range checks {
		if err != nil {
			return TracingLimits{}, err
		}
	}
	keys := set(allowed...)
	if err := require(len(keys) <= 128, "At most 128 LogDrop custom attribute keys are allowed"); err != nil {
		return TracingLimits{}, err
	}
	for key := range keys {
		if err := require(IsValidCustomKey(key), "Invalid LogDrop custom attribute key"); err != nil {
			return TracingLimits{}, err
		}
	}
	return TracingLimits{maxActiveSpans, maxAttributes, maxEvents, maxLinks, maxStringLength, keys}, nil
}

// MvpLimits is the MVP profile accepted on 2026-09-08, the same as the Java agent.
func MvpLimits(allowed []string) (TracingLimits, error) {
	return NewTracingLimits(1024, 24, 16, 16, 256, allowed)
}

// IsValidCustomKey reports a valid custom attribute key.
func IsValidCustomKey(key string) bool { return customKey.MatchString(key) }

// Bounded cuts a string to the limit in UTF-16 units, like Java's String#substring.
func (l TracingLimits) Bounded(value string) string { return UTF16Prefix(value, l.MaxStringLength) }

func inRange(value, min, max int) bool { return value >= min && value <= max }
