package core

import (
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// IngestKeyHeader carries the traces ingest key on every export request.
const IngestKeyHeader = "x-logdrop-ingest-key"

// MVP export profile, the same as the Java agent (accepted 2026-09-08).
const (
	MvpMaxQueueSize          = 2048
	MvpMaxExportBatchSize    = 128
	MvpScheduleDelayMillis   = 1000
	MvpExportTimeoutMillis   = 5000
	MvpShutdownTimeoutMillis = 10000
)

const endpointMessage = "LogDrop endpoint must be an HTTPS URL without credentials, query, or fragment; HTTP requires explicit opt-in"

var (
	uriPattern = regexp.MustCompile(`(?s)\A([A-Za-z][A-Za-z0-9+.\-]*)://([^/?#]*)([^?#]*)(\?[^#]*)?(#.*)?\z`)
	label      = regexp.MustCompile(`\A[A-Za-z0-9](?:[A-Za-z0-9\-]*[A-Za-z0-9])?\z`)
	// java.net.URI path characters: unreserved, sub-delims, ':', '@', '/' and %XX escapes.
	pathChars = regexp.MustCompile(`\A(?:[A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*\z`)
	digits    = regexp.MustCompile(`\A[0-9]*\z`)
	octet     = regexp.MustCompile(`\A[0-9]{1,3}\z`)
	ipv6      = regexp.MustCompile(`\A[0-9A-Fa-f:.]+\z`)
)

type endpoint struct {
	scheme, host, path        string
	port                      int64
	userInfo, query, fragment bool
}

func isIPv4(host string) bool {
	parts := strings.Split(host, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if !octet.MatchString(p) {
			return false
		}
		if n, _ := strconv.Atoi(p); n > 255 {
			return false
		}
	}
	return true
}

// isHostname is java.net.URI's server-based authority: a domain name whose rightmost label starts with a letter.
func isHostname(host string) bool {
	name := strings.TrimSuffix(host, ".")
	if name == "" {
		return false
	}
	labels := strings.Split(name, ".")
	for _, l := range labels {
		if !label.MatchString(l) {
			return false
		}
	}
	first := labels[len(labels)-1][0]
	return (first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z')
}

// parse returns the parts java.net.URI would report, or nil where it has no server host.
func parse(value string) *endpoint {
	m := uriPattern.FindStringSubmatchIndex(value)
	if m == nil {
		return nil
	}
	group := func(i int) (string, bool) {
		if m[2*i] < 0 {
			return "", false
		}
		return value[m[2*i]:m[2*i+1]], true
	}
	scheme, _ := group(1)
	authority, _ := group(2)
	rawPath, _ := group(3)
	_, hasQuery := group(4)
	_, hasFragment := group(5)
	if !pathChars.MatchString(rawPath) {
		return nil
	}
	if strings.Contains(authority, "@") {
		return &endpoint{scheme: scheme, port: -1, userInfo: true}
	}
	var host, portText string
	if strings.HasPrefix(authority, "[") {
		end := strings.Index(authority, "]")
		if end < 0 || !(ipv6.MatchString(authority[1:end]) && strings.Contains(authority[1:end], ":")) {
			return nil
		}
		host = authority[:end+1]
		if rest := authority[end+1:]; rest != "" {
			if !strings.HasPrefix(rest, ":") {
				return nil
			}
			portText = rest[1:]
		}
	} else {
		colon := strings.LastIndex(authority, ":")
		host = authority
		if colon >= 0 {
			host, portText = authority[:colon], authority[colon+1:]
		}
		if !isIPv4(host) && !isHostname(host) {
			return nil
		}
	}
	if !digits.MatchString(portText) {
		return nil
	}
	port := int64(-1)
	if portText != "" {
		parsed, err := strconv.ParseInt(portText, 10, 64)
		if err != nil || parsed > math.MaxInt32 || len(portText) > 10 {
			return nil
		}
		port = parsed
	}
	path, err := url.PathUnescape(rawPath)
	if err != nil || !utf8.ValidString(path) {
		return nil
	}
	return &endpoint{scheme: scheme, host: host, port: port, query: hasQuery, fragment: hasFragment, path: path}
}

// TracesEndpoint is the OTLP/HTTP traces URL for an HTTPS gateway base URL (or the explicit /v1/traces URL).
func TracesEndpoint(value string, allowInsecureTransport bool) (string, error) {
	uri := parse(value)
	ok := uri != nil && !uri.userInfo && !uri.query && !uri.fragment &&
		(uri.scheme == "https" || (allowInsecureTransport && uri.scheme == "http"))
	if err := require(ok, endpointMessage); err != nil {
		return "", err
	}
	if err := require(uri.port == -1 || (uri.port >= 1 && uri.port <= 65535), "Invalid LogDrop endpoint port"); err != nil {
		return "", err
	}
	if err := require(uri.path == "" || uri.path == "/" || uri.path == "/v1/traces",
		"LogDrop endpoint path must be empty or /v1/traces"); err != nil {
		return "", err
	}
	trimmed := strings.TrimRight(value, "/")
	if uri.path == "/v1/traces" {
		return trimmed, nil
	}
	return trimmed + "/v1/traces", nil
}

// IsValidCredential reports a key of 1 to 512 visible ASCII characters.
func IsValidCredential(credential string) bool {
	if credential == "" || UTF16Len(credential) > 512 {
		return false
	}
	for _, r := range credential {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

// RequireServiceIdentity checks the service name and the other identity values.
func RequireServiceIdentity(serviceName *string, dimensions ...string) error {
	ok := serviceName != nil && len(Trim(*serviceName)) > 0 && UTF16Len(*serviceName) <= 128
	if err := require(ok, "LogDrop service name is required and bounded to 128 characters"); err != nil {
		return err
	}
	for _, d := range dimensions {
		if err := require(UTF16Len(d) <= 128 && !HasControl(d), "Invalid LogDrop resource identity"); err != nil {
			return err
		}
	}
	return require(!HasControl(*serviceName), "Invalid LogDrop service name")
}

// RequireQueue checks the export queue limits.
func RequireQueue(maxQueueSize, maxExportBatchSize int, exportTimeoutMillis, scheduleDelayMillis, shutdownTimeoutMillis float64) error {
	checks := []error{
		require(maxQueueSize >= 1 && maxQueueSize <= 65536 && maxExportBatchSize >= 1 && maxExportBatchSize <= maxQueueSize,
			"Invalid LogDrop export queue limits"),
		require(within(exportTimeoutMillis, 100, 30_000), "Invalid LogDrop export timeout"),
		require(within(scheduleDelayMillis, 10, 5_000), "Invalid LogDrop schedule delay"),
		require(within(shutdownTimeoutMillis, 100, 60_000), "Invalid LogDrop shutdown timeout"),
	}
	for _, err := range checks {
		if err != nil {
			return err
		}
	}
	return nil
}

func within(value, min, max float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= min && value <= max
}
