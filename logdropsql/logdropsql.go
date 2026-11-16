// Package logdropsql traces database/sql for LogDrop: use Open instead of sql.Open (or OpenDB instead of
// sql.OpenDB). Every query becomes a span with the database system, the server and a value-free template of the
// statement; the values in the statement never leave the service.
//
//	db, err := logdropsql.Open("pgx", os.Getenv("DATABASE_URL"))
package logdropsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
)

// Option configures Open and OpenDB.
type Option func(*config)

type config struct {
	system string
	host   string
	port   int
	name   string
}

// WithSystem sets the database system (an OpenTelemetry db.system.name value such as postgresql, mysql or
// microsoft.sql_server) when the driver name does not tell it.
func WithSystem(system string) Option { return func(c *config) { c.system = system } }

// WithServer sets the database server's host and port when the data source name does not tell them.
func WithServer(host string, port int) Option { return func(c *config) { c.host, c.port = host, port } }

// systems maps driver names to OpenTelemetry db.system.name values.
var systems = map[string]string{
	"postgres": "postgresql", "pgx": "postgresql", "pgx/v4": "postgresql", "pgx/v5": "postgresql",
	"cloudsqlpostgres": "postgresql", "mysql": "mysql", "sqlite": "sqlite", "sqlite3": "sqlite",
	"sqlserver": "microsoft.sql_server", "mssql": "microsoft.sql_server", "azuresql": "microsoft.sql_server",
	"oracle": "oracle.db", "godror": "oracle.db", "clickhouse": "clickhouse", "snowflake": "snowflake",
}

// SystemOf returns the db.system.name of a driver name, or "other_sql".
func SystemOf(driverName string) string {
	if system, ok := systems[strings.ToLower(driverName)]; ok {
		return system
	}
	return "other_sql"
}

// server reads host, port and database name from a URL ("postgres://u:p@host:5432/db"), key-value ("host=h port=5432
// dbname=db") or MySQL ("u:p@tcp(host:3306)/db") data source name. Credentials are never read.
func server(dsn string) (host string, port int, name string) {
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" && u.Host != "" {
		host = u.Hostname()
		port, _ = strconv.Atoi(u.Port())
		return host, port, strings.TrimPrefix(u.Path, "/")
	}
	if at := strings.LastIndex(dsn, "@tcp("); at >= 0 {
		rest := dsn[at+5:]
		if end := strings.Index(rest, ")"); end >= 0 {
			if h, p, err := net.SplitHostPort(rest[:end]); err == nil {
				host = h
				port, _ = strconv.Atoi(p)
			} else {
				host = rest[:end]
			}
			name = strings.SplitN(strings.TrimPrefix(rest[end+1:], "/"), "?", 2)[0]
		}
		return host, port, name
	}
	for _, part := range strings.Fields(strings.ReplaceAll(dsn, ";", " ")) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "host", "server", "data source":
			host = strings.Split(value, ",")[0]
		case "port":
			port, _ = strconv.Atoi(value)
		case "dbname", "database":
			name = value
		}
	}
	return host, port, name
}

func otelOptions(c config) []otelsql.Option {
	attributes := []attribute.KeyValue{attribute.String("db.system.name", c.system)}
	if c.host != "" {
		attributes = append(attributes, attribute.String("server.address", c.host))
	}
	if c.port > 0 {
		attributes = append(attributes, attribute.Int("server.port", c.port))
	}
	if c.name != "" {
		attributes = append(attributes, attribute.String("db.namespace", c.name))
	}
	return []otelsql.Option{
		otelsql.WithAttributes(attributes...),
		otelsql.WithSpanNameFormatter(spanName),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			OmitConnResetSession: true, OmitConnPrepare: true, OmitRows: true, OmitConnectorConnect: true,
		}),
	}
}

// spanName is the statement's first keyword ("SELECT"), or the database/sql step without a statement.
func spanName(_ context.Context, method otelsql.Method, query string) string {
	if fields := strings.Fields(query); len(fields) > 0 {
		keyword := strings.ToUpper(fields[0])
		if strings.Trim(keyword, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") == "" {
			return keyword
		}
	}
	return string(method)
}

func configure(driverName, dsn string, options []Option) config {
	c := config{system: SystemOf(driverName)}
	c.host, c.port, c.name = server(dsn)
	for _, option := range options {
		option(&c)
	}
	return c
}

// Open opens a database like sql.Open, with tracing.
func Open(driverName, dataSourceName string, options ...Option) (*sql.DB, error) {
	return otelsql.Open(driverName, dataSourceName, otelOptions(configure(driverName, dataSourceName, options))...)
}

// OpenDB opens a database from a connector like sql.OpenDB, with tracing. driverName names the driver for the
// database system ("pgx", "mysql"); use WithServer for the host and port.
func OpenDB(connector driver.Connector, driverName string, options ...Option) *sql.DB {
	return otelsql.OpenDB(connector, otelOptions(configure(driverName, "", options))...)
}
