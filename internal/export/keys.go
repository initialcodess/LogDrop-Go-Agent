// Package export sends sanitized spans to LogDrop with the Java agent's rules: a bounded queue, requests kept inside
// LogDrop's admission limits, a hand-written OTLP/HTTP protobuf encoder, and the shared retry schedule.
package export

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"github.com/initialcodess/LogDrop-Go-Agent/internal/diag"
)

// Key file timing, like the Java agent.
const (
	KeyRefresh      = 10 * time.Second
	KeyMinForced    = time.Second
	KeyFileMaxBytes = 4096
)

// KeySource is the ingest key for every export request, like the Java agent. A key file (a mounted Kubernetes
// Secret) is re-read every 10 s and, at most once a second, right after the gateway rejects the key, so a rotated key
// is used without a restart. A missing or invalid file keeps the last valid key; the key is never logged.
type KeySource struct {
	path        string
	clock       func() time.Duration
	diagnostics *diag.Diagnostics
	mu          sync.Mutex
	key         string
	fileKey     string
	readAt      time.Duration
	read        bool
	stale       bool
}

// FixedKey is a key from LOGDROP_INGEST_KEY.
func FixedKey(key string) *KeySource { return &KeySource{key: key, clock: diag.Monotonic} }

// FileKey is a key read from LOGDROP_INGEST_KEY_FILE.
func FileKey(path string, diagnostics *diag.Diagnostics, clock func() time.Duration) *KeySource {
	if clock == nil {
		clock = diag.Monotonic
	}
	return &KeySource{path: path, clock: clock, diagnostics: diagnostics}
}

// Current is the valid key to send now, or "" when none is available.
func (k *KeySource) Current() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.path != "" {
		k.refreshIfDue()
	}
	return k.key
}

// Rejected marks the key as refused by the gateway: the file is read again before the next request.
func (k *KeySource) Rejected() {
	k.mu.Lock()
	k.stale = true
	k.mu.Unlock()
}

// Override uses a key set through the API until the key file changes again.
func (k *KeySource) Override(credential string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.path != "" && !k.read {
		// Record the file's current key first, so only a later change of the file replaces this one.
		k.fileKey = k.readFile()
		k.readAt = k.clock()
		k.read = true
	}
	k.key = credential
}

func (k *KeySource) refreshIfDue() {
	age := k.clock() - k.readAt
	if k.read && age < KeyRefresh && !(k.stale && age >= KeyMinForced) {
		return
	}
	found := k.readFile()
	k.readAt = k.clock()
	k.read = true
	k.stale = false
	switch {
	case found != "":
		if found != k.fileKey {
			k.key = found
		}
		k.fileKey = found
	case k.key == "":
		if k.diagnostics != nil {
			k.diagnostics.Warn("key-file-missing", "LogDrop ingest key file is missing or invalid; traces are not sent until it is readable.")
		}
	default:
		if k.diagnostics != nil {
			k.diagnostics.Warn("key-file-stale", "LogDrop ingest key file is missing or invalid; the last valid key is still used.")
		}
	}
}

func (k *KeySource) readFile() string {
	file, err := os.Open(k.path)
	if err != nil {
		return ""
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, KeyFileMaxBytes+1))
	if err != nil || len(raw) > KeyFileMaxBytes {
		return ""
	}
	value := core.Trim(string(raw))
	if !core.IsValidCredential(value) {
		return ""
	}
	return value
}

// String never shows the key.
func (k *KeySource) String() string { return "KeySource(redacted)" }
