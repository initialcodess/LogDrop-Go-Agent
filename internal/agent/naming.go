package agent

import (
	"regexp"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// Dependency naming keeps the dependency map keys and APM names of the Java agent, so a call reads the same from any
// language: a server span's address is the host only, HTTP/1.x spans carry no protocol version (edges read http;
// HTTP/2 stays http/2), RabbitMQ spans name the broker RabbitMQ, and Redis commands are upper case and are the
// operation (a Redis span named by its command with no operation would read as an unspecified database call).

const (
	protocolVersion = "network.protocol.version"
	peerService     = "peer.service"
)

var (
	redisOperations = []string{"db.operation.name", "db.operation"}
	// One command word, or a container command and its subcommand (CLIENT SETNAME); a second word after any other
	// command could be a key.
	redisCommand = regexp.MustCompile(`\A(?:[A-Za-z][A-Za-z_]*|(?:ACL|CLIENT|CLUSTER|COMMAND|CONFIG|FUNCTION|LATENCY|MEMORY|MODULE|OBJECT|PUBSUB|SCRIPT|SLOWLOG|XGROUP|XINFO|` +
		`acl|client|cluster|command|config|function|latency|memory|module|object|pubsub|script|slowlog|xgroup|xinfo) [A-Za-z][A-Za-z_]*)\z`)
	hostPort = regexp.MustCompile(`\A([^:\[\]]+|\[[0-9A-Fa-f:.]+\]):([0-9]{1,5})\z`)
)

func stringOf(values map[string]attribute.Value, key string) (string, bool) {
	v, ok := values[key]
	if !ok || v.Type() != attribute.STRING {
		return "", false
	}
	return v.AsString(), true
}

// ApplyNaming returns the span name to export and renames the attributes in place.
func ApplyNaming(name string, values map[string]attribute.Value) string {
	_, hasMethod := values["http.request.method"]
	_, hasLegacyMethod := values["http.method"]
	if version, ok := stringOf(values, protocolVersion); ok && (version == "1.1" || version == "1.0") && (hasMethod || hasLegacyMethod) {
		delete(values, protocolVersion)
	}
	if system, ok := stringOf(values, "messaging.system"); ok && system == "rabbitmq" {
		if _, has := values[peerService]; !has {
			values[peerService] = attribute.StringValue("RabbitMQ")
		}
	}
	if address, ok := stringOf(values, "server.address"); ok {
		if port, has := values["server.port"]; has && port.Type() == attribute.INT64 {
			if m := hostPort.FindStringSubmatch(address); m != nil {
				if p, err := strconv.ParseInt(m[2], 10, 64); err == nil && p == port.AsInt64() {
					values["server.address"] = attribute.StringValue(m[1])
				}
			}
		}
	}
	system, ok := stringOf(values, "db.system.name")
	if !ok {
		system, _ = stringOf(values, "db.system")
	}
	if system != "redis" {
		return name
	}
	hasOperation := false
	for _, key := range redisOperations {
		operation, ok := stringOf(values, key)
		if !ok {
			continue
		}
		hasOperation = true
		upper := strings.ToUpper(operation)
		if name == operation {
			name = upper
		}
		values[key] = attribute.StringValue(upper)
	}
	if !hasOperation && redisCommand.MatchString(name) {
		name = strings.ToUpper(name)
		values["db.operation.name"] = attribute.StringValue(name)
	}
	return name
}
