package core

import "strings"

// Privacy allowlist, the same as the Java core's AttributeRules. A key is exported only when it is a standard
// low-cardinality key or explicitly allowed, and never when it can carry secrets, headers, raw URLs, SQL text or
// exception text. The collector remains the enforcement boundary. Raw SQL keys stay blocked here; the privacy
// snapshot adds only the SQL normalizer's template of a SQL span as db.query.text.

var secretWords = []string{
	"authorization", "cookie", "token", "secret", "password", "credential", "api_key", "apikey", "api-key",
}

var forbiddenPrefixes = []string{"http.request.header", "http.response.header", "db.query.parameter"}

var forbidden = set(
	"db.statement", "db.query.text", "url.full", "url.query", "http.url", "http.target",
	"exception.message", "exception.stacktrace", "logdrop.project.id",
)

// standard holds the keys the APM, dependency map and trace summaries read: templates, system identifiers, bounded
// codes.
var standard = set(
	"http.route", "http.request.method", "http.method", "http.response.status_code", "http.status_code",
	"url.template", "server.address", "server.port", "net.peer.name", "net.peer.port", "peer.service",
	"network.protocol.name", "network.protocol.version",
	"db.system", "db.system.name", "db.namespace", "db.name", "db.operation", "db.operation.name",
	"db.query.summary", "db.collection.name",
	"rpc.system", "rpc.service", "rpc.method", "rpc.grpc.status_code",
	"messaging.system", "messaging.operation", "messaging.operation.name", "messaging.operation.type",
	"messaging.destination.name", "messaging.destination.template", "messaging.destination.temporary",
	"messaging.temp_destination",
	"graphql.operation.name", "graphql.operation.type",
	"exception.type", "error.type",
)

func set(values ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(values))
	for _, v := range values {
		m[v] = struct{}{}
	}
	return m
}

// IsBlocked reports keys that are never exported, even when a customer allowlists them.
func IsBlocked(key string) bool {
	lower := strings.ToLower(key)
	for _, word := range secretWords {
		if strings.Contains(lower, word) {
			return true
		}
	}
	for _, prefix := range forbiddenPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	_, found := forbidden[lower]
	return found
}

// IsStandard reports the standard keys.
func IsStandard(key string) bool {
	_, found := standard[key]
	return found
}

// IsExportable reports automatic and custom span attributes that may leave the process.
func IsExportable(key string, allowed map[string]struct{}) bool {
	if IsBlocked(key) {
		return false
	}
	if IsStandard(key) {
		return true
	}
	_, found := allowed[key]
	return found
}

// IsCustomAllowed reports custom API attributes: only explicitly allowed keys, never blocked ones.
func IsCustomAllowed(key string, allowed map[string]struct{}) bool {
	if IsBlocked(key) {
		return false
	}
	_, found := allowed[key]
	return found
}
