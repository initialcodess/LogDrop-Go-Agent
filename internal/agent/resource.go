package agent

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	sdk "go.opentelemetry.io/otel/sdk"
)

// The agent's name and version, sent as telemetry.distro.*.
const (
	Version      = "0.1.0-alpha1"
	Distribution = "logdrop-go-agent"
)

// The resource identity comes from LOGDROP_* settings plus an allowlist of detected values. Process command lines,
// executable paths, owners and OTEL_RESOURCE_ATTRIBUTES never leave the process.

var (
	processInstance = randomUUID()
	cgroupID        = regexp.MustCompile(`([0-9a-f]{64})(?:\.scope)?\s*$`)
	mountID         = regexp.MustCompile(`/(?:containers|sandboxes)/([0-9a-f]{64})/`)
	detectedKeys    = []string{"telemetry.sdk.name", "telemetry.sdk.language", "telemetry.sdk.version",
		"process.runtime.name", "process.runtime.version", "os.type", "host.arch", "container.id"}
	// A container image's machine-id or hostname is not a host identity; keep them only outside containers.
	hostOnly = []string{"host.id", "host.name"}
)

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return formatUUID(b[:])
}

func formatUUID(b []byte) string {
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// NameUUID is java.util.UUID.nameUUIDFromBytes: a version 3 (MD5) UUID, so a pod and container get the Java agent's
// instance ID.
func NameUUID(name string) string {
	sum := md5.Sum([]byte(name))
	sum[6] = sum[6]&0x0f | 0x30
	sum[8] = sum[8]&0x3f | 0x80
	return formatUUID(sum[:])
}

func readText(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(raw)
}

// ContainerID reads the container ID from cgroup v1 or, under cgroup v2, from the container's mounts.
func ContainerID(cgroup, mountinfo string) string {
	for _, line := range strings.Split(cgroup, "\n") {
		if m := cgroupID.FindStringSubmatch(line); m != nil {
			return m[1]
		}
	}
	for _, line := range strings.Split(mountinfo, "\n") {
		if !strings.Contains(line, "/hostname") && !strings.Contains(line, "/resolv.conf") {
			continue
		}
		if m := mountID.FindStringSubmatch(line); m != nil {
			return m[1]
		}
	}
	return ""
}

// Detect returns the detected values of this process.
func Detect() map[string]string {
	detected := map[string]string{
		"telemetry.sdk.name":      "opentelemetry",
		"telemetry.sdk.language":  "go",
		"telemetry.sdk.version":   sdk.Version(),
		"process.runtime.name":    "go",
		"process.runtime.version": runtime.Version(),
		"os.type":                 runtime.GOOS,
	}
	if arch := map[string]string{"amd64": "amd64", "arm64": "arm64", "386": "x86", "arm": "arm32"}[runtime.GOARCH]; arch != "" {
		detected["host.arch"] = arch
	}
	if name, err := os.Hostname(); err == nil {
		detected["host.name"] = name
	}
	if runtime.GOOS == "linux" {
		if id := ContainerID(readText("/proc/self/cgroup"), readText("/proc/self/mountinfo")); id != "" {
			detected["container.id"] = id
		}
		machineID := strings.TrimSpace(readText("/etc/machine-id"))
		if machineID == "" {
			machineID = strings.TrimSpace(readText("/var/lib/dbus/machine-id"))
		}
		if machineID != "" {
			detected["host.id"] = machineID
		}
	}
	return detected
}

func bounded(value string) (string, bool) {
	if value == "" || core.UTF16Len(value) > MaxResourceValue || core.HasControl(value) {
		return "", false
	}
	return value, true
}

// InstanceID is LOGDROP_SERVICE_INSTANCE_ID, else stable for one container of one pod, else random per process.
func InstanceID(s *Settings) string {
	if s.InstanceID != "" {
		return s.InstanceID
	}
	podUID, ok := s.Kubernetes["k8s.pod.uid"]
	if !ok {
		return processInstance
	}
	return NameUUID(fmt.Sprintf("%s/%s", podUID, s.Kubernetes["k8s.container.name"]))
}

// BuildResource returns the resource attributes of this process.
func BuildResource(s *Settings, detected map[string]string) []core.SafeAttribute {
	var attributes []core.SafeAttribute
	_, inContainer := bounded(detected["container.id"])
	containerized := s.InKubernetes() || inContainer
	for _, key := range detectedKeys {
		if value, ok := bounded(detected[key]); ok {
			attributes = append(attributes, core.SafeAttribute{Key: key, Value: value})
		}
	}
	if !containerized {
		for _, key := range hostOnly {
			if value, ok := bounded(detected[key]); ok {
				attributes = append(attributes, core.SafeAttribute{Key: key, Value: value})
			}
		}
	}
	for _, key := range sortedKeys(s.Kubernetes) {
		attributes = append(attributes, core.SafeAttribute{Key: key, Value: s.Kubernetes[key]})
	}
	attributes = append(attributes, core.SafeAttribute{Key: "service.name", Value: s.Service},
		core.SafeAttribute{Key: "deployment.environment.name", Value: s.Environment})
	if s.ServiceNamespace != "" {
		attributes = append(attributes, core.SafeAttribute{Key: "service.namespace", Value: s.ServiceNamespace})
	}
	if s.Release != "" {
		attributes = append(attributes, core.SafeAttribute{Key: "service.version", Value: s.Release})
	}
	return append(attributes, core.SafeAttribute{Key: "service.instance.id", Value: InstanceID(s)},
		core.SafeAttribute{Key: "telemetry.distro.name", Value: Distribution},
		core.SafeAttribute{Key: "telemetry.distro.version", Value: Version})
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
