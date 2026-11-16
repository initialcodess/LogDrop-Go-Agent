// Package agent wires the LogDrop pipeline into the OpenTelemetry Go SDK: settings, the admission sampler, the span
// processor (probe filter, dependency naming, privacy snapshot), the resource identity and the API bridge.
package agent

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
)

// Setting bounds, the same as the Java agent.
const (
	MaxCABytes       = 1024 * 1024
	MaxResourceValue = 256
	// MaxGatewayBatch is the most spans the gateway accepts per request (trace delivery receipt bound).
	MaxGatewayBatch = 256
)

var (
	durationPattern = regexp.MustCompile(`\A([0-9]{1,6})(ms|s)\z`)
	integerPattern  = regexp.MustCompile(`\A[+\-]?[0-9]+\z`)
	workloadKinds   = map[string]bool{"deployment": true, "statefulset": true, "daemonset": true, "job": true,
		"cronjob": true, "replicaset": true}
)

// Settings are the LOGDROP_* settings, with the Java agent's names, defaults and checks. Problems name the setting,
// never its value.
type Settings struct {
	TracesEndpoint      string
	EndpointHost        string
	InlineKey           string
	KeyFile             string
	Service             string
	ServiceNamespace    string
	Environment         string
	Release             string
	InstanceID          string
	Limits              core.TracingLimits
	MaxQueueSize        int
	MaxExportBatchSize  int
	ScheduleDelayMillis int
	ExportTimeoutMillis int
	Compression         string
	CAFile              string
	Debug               bool
	Kubernetes          map[string]string
	Problems            []string
}

// Environment reads the process environment.
func Environment() map[string]string {
	env := map[string]string{}
	for _, entry := range os.Environ() {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	return env
}

func trimmed(value string) string { return core.Trim(value) }

func lower(value string) string { return strings.ToLower(core.Trim(value)) }

// DisabledReason is "" when tracing is enabled; otherwise a reason that never contains a configured value.
func DisabledReason(env map[string]string) string {
	if lower(env["LOGDROP_ENABLED"]) == "false" {
		return "LOGDROP_ENABLED is false."
	}
	if trimmed(env["LOGDROP_ENDPOINT"]) == "" {
		return "LOGDROP_ENDPOINT is not set."
	}
	return ""
}

// InKubernetes reports a pod identity from the trace injector.
func (s *Settings) InKubernetes() bool {
	_, ok := s.Kubernetes["k8s.pod.uid"]
	return ok
}

// Valid reports settings without problems.
func (s *Settings) Valid() bool { return len(s.Problems) == 0 }

// String never shows the settings' values.
func (s *Settings) String() string { return "Settings(redacted)" }

// ParseSettings reads the LOGDROP_* settings from env.
func ParseSettings(env map[string]string) *Settings {
	s := &Settings{Kubernetes: map[string]string{}}
	var found []string
	insecure := lower(env["LOGDROP_ALLOW_INSECURE_TRANSPORT"]) == "true"
	if endpoint, err := core.TracesEndpoint(trimmed(env["LOGDROP_ENDPOINT"]), insecure); err == nil {
		s.TracesEndpoint = endpoint
		if parsed, err := url.Parse(endpoint); err == nil {
			s.EndpointHost = strings.Trim(parsed.Hostname(), "[]")
		}
	} else {
		message := "LOGDROP_ENDPOINT must be an HTTPS URL without credentials, query or fragment"
		if !insecure {
			message += " (HTTP needs LOGDROP_ALLOW_INSECURE_TRANSPORT=true)"
		}
		found = append(found, message+".")
	}

	key, keyFile := trimmed(env["LOGDROP_INGEST_KEY"]), trimmed(env["LOGDROP_INGEST_KEY_FILE"])
	switch {
	case key != "" && keyFile != "":
		found = append(found, "Set either LOGDROP_INGEST_KEY or LOGDROP_INGEST_KEY_FILE, not both.")
	case key == "" && keyFile == "":
		found = append(found, "LOGDROP_INGEST_KEY_FILE or LOGDROP_INGEST_KEY is required.")
	case key != "" && !core.IsValidCredential(key):
		found = append(found, "LOGDROP_INGEST_KEY is not a valid ingest key.")
	}
	if keyFile == "" {
		s.InlineKey = key
	}
	s.KeyFile = keyFile

	s.Service = trimmed(env["LOGDROP_SERVICE"])
	s.ServiceNamespace = trimmed(env["LOGDROP_SERVICE_NAMESPACE"])
	s.Environment = trimmed(env["LOGDROP_ENVIRONMENT"])
	if s.Environment == "" {
		s.Environment = "production"
	}
	s.Release = trimmed(env["LOGDROP_RELEASE"])
	s.InstanceID = trimmed(env["LOGDROP_SERVICE_INSTANCE_ID"])
	if s.Service == "" {
		found = append(found, "LOGDROP_SERVICE is required.")
	} else if err := core.RequireServiceIdentity(&s.Service, s.Environment, s.ServiceNamespace, s.Release, s.InstanceID); err != nil {
		found = append(found, "LOGDROP_SERVICE, LOGDROP_SERVICE_NAMESPACE, LOGDROP_ENVIRONMENT, LOGDROP_RELEASE and "+
			"LOGDROP_SERVICE_INSTANCE_ID must be at most 128 characters without control characters.")
	}

	// Defaults are the MVP profile; overrides follow the validated ranges.
	mvp, _ := core.MvpLimits(nil)
	limits, err := core.NewTracingLimits(
		readInteger(env, "LOGDROP_MAX_ACTIVE_SPANS", mvp.MaxActiveSpans, &found),
		readInteger(env, "LOGDROP_MAX_ATTRIBUTES", mvp.MaxAttributes, &found),
		readInteger(env, "LOGDROP_MAX_EVENTS", mvp.MaxEvents, &found),
		readInteger(env, "LOGDROP_MAX_LINKS", mvp.MaxLinks, &found),
		readInteger(env, "LOGDROP_MAX_STRING_LENGTH", mvp.MaxStringLength, &found),
		list(env["LOGDROP_ALLOWED_ATTRIBUTE_KEYS"]))
	if err != nil {
		found = append(found, "LOGDROP_ALLOWED_ATTRIBUTE_KEYS (at most 128 keys) or a LOGDROP_MAX_* limit is outside its range.")
		limits = mvp
	}
	s.Limits = limits

	s.MaxQueueSize = readInteger(env, "LOGDROP_MAX_QUEUE_SIZE", core.MvpMaxQueueSize, &found)
	s.MaxExportBatchSize = readInteger(env, "LOGDROP_MAX_EXPORT_BATCH_SIZE", core.MvpMaxExportBatchSize, &found)
	s.ScheduleDelayMillis = readDuration(env, "LOGDROP_SCHEDULE_DELAY", core.MvpScheduleDelayMillis, &found)
	s.ExportTimeoutMillis = readDuration(env, "LOGDROP_EXPORT_TIMEOUT", core.MvpExportTimeoutMillis, &found)
	if core.RequireQueue(s.MaxQueueSize, s.MaxExportBatchSize, float64(s.ExportTimeoutMillis), float64(s.ScheduleDelayMillis),
		core.MvpShutdownTimeoutMillis) != nil || s.MaxExportBatchSize > MaxGatewayBatch {
		found = append(found, "LOGDROP_MAX_QUEUE_SIZE, LOGDROP_MAX_EXPORT_BATCH_SIZE (at most 256), LOGDROP_SCHEDULE_DELAY or "+
			"LOGDROP_EXPORT_TIMEOUT is outside its range.")
	}

	compression := lower(env["LOGDROP_COMPRESSION"])
	if compression != "" && compression != "gzip" && compression != "none" {
		found = append(found, "LOGDROP_COMPRESSION must be gzip or none.")
	}
	s.Compression = "gzip"
	if compression == "none" {
		s.Compression = "none"
	}

	if caFile := trimmed(env["LOGDROP_CA_FILE"]); caFile != "" {
		if info, err := os.Stat(caFile); err != nil || info.IsDir() || info.Size() > MaxCABytes {
			found = append(found, "LOGDROP_CA_FILE cannot be read.")
		} else if file, err := os.Open(caFile); err != nil {
			found = append(found, "LOGDROP_CA_FILE cannot be read.")
		} else {
			_ = file.Close()
			s.CAFile = caFile
		}
	}

	s.Debug = lower(env["LOGDROP_DEBUG"]) == "true"

	k8s := s.Kubernetes
	resourceValue(k8s, "k8s.cluster.uid", env["LOGDROP_K8S_CLUSTER_UID"])
	resourceValue(k8s, "k8s.cluster.name", env["LOGDROP_K8S_CLUSTER_NAME"])
	resourceValue(k8s, "k8s.namespace.name", env["LOGDROP_K8S_NAMESPACE"])
	resourceValue(k8s, "k8s.node.name", env["LOGDROP_K8S_NODE_NAME"])
	resourceValue(k8s, "k8s.pod.name", env["LOGDROP_K8S_POD_NAME"])
	resourceValue(k8s, "k8s.pod.uid", env["LOGDROP_K8S_POD_UID"])
	resourceValue(k8s, "k8s.container.name", env["LOGDROP_K8S_CONTAINER_NAME"])
	if kind := lower(env["LOGDROP_K8S_WORKLOAD_KIND"]); workloadKinds[kind] {
		resourceValue(k8s, fmt.Sprintf("k8s.%s.name", kind), env["LOGDROP_K8S_WORKLOAD_NAME"])
	}
	s.Problems = found
	return s
}

func resourceValue(target map[string]string, key, value string) {
	v := trimmed(value)
	if v != "" && core.UTF16Len(v) <= MaxResourceValue && !core.HasControl(v) {
		target[key] = v
	}
}

// readInteger is Integer.parseInt: an optional sign and digits inside the 32-bit range.
func readInteger(env map[string]string, name string, fallback int, problems *[]string) int {
	value := trimmed(env[name])
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(strings.TrimPrefix(value, "+"), 10, 32)
	if !integerPattern.MatchString(value) || err != nil {
		*problems = append(*problems, name+" must be a whole number.")
		return fallback
	}
	return int(parsed)
}

// readDuration reads 200ms or 5s.
func readDuration(env map[string]string, name string, fallbackMillis int, problems *[]string) int {
	value := trimmed(env[name])
	if value == "" {
		return fallbackMillis
	}
	m := durationPattern.FindStringSubmatch(value)
	if m == nil {
		*problems = append(*problems, name+" must be a duration such as 200ms or 5s.")
		return fallbackMillis
	}
	amount, _ := strconv.Atoi(m[1])
	if m[2] == "s" {
		return amount * 1000
	}
	return amount
}

func list(value string) []string {
	var keys []string
	seen := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		key := core.Trim(part)
		if key != "" && !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}
