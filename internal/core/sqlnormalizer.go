package core

import (
	"strings"
	"time"

	"github.com/dlclark/regexp2"
)

// The SQL normalizer turns SQL text into a value-free statement template that can be exported and grouped, the same
// as the Java core's SqlStatementNormalizer: literals become "?", comments are removed, "IN (?, ?, ...)" and multi-row
// VALUES lists collapse to one placeholder group, and whitespace collapses to single spaces. Identifiers, keywords
// and existing bind placeholders are kept.
//
// The OpenTelemetry instrumentations may already mask values; this pass does not trust that. The collector repeats
// the same masking. Only SQL systems are accepted: key-value and document stores can carry user data in keys or
// documents, so their statements are never exported.
//
// Port notes (the Java regex semantics are kept on purpose; conformance/privacy covers each one): Go's own regexp has
// no lookarounds or backreferences, so the patterns run on regexp2 with the .NET agent's spelling of the Java
// patterns. No pattern uses \b, \w, \s or IgnoreCase: word boundaries are explicit ASCII lookarounds, letters are
// [Xx] classes, whitespace is [ \t\n\x0B\f\r], and Java's end-of-input $ is \z. A statement whose matching takes
// longer than the timeout is not exported at all.

const (
	sqlTimeout    = 250 * time.Millisecond
	ws            = `[ \t\n\x0B\f\r]`
	notWordBefore = `(?<![A-Za-z0-9_])`
	notWordAfter  = `(?![A-Za-z0-9_])`
	row           = `\(` + ws + `*\?(?:` + ws + `*,` + ws + `*\?)*` + ws + `*\)`
)

func compile(pattern string, options regexp2.RegexOptions) *regexp2.Regexp {
	re := regexp2.MustCompile(pattern, options)
	re.MatchTimeout = sqlTimeout
	return re
}

var (
	// Scanned left to right in one pass, so a quote inside a comment or "--" inside a string cannot shift what the
	// other alternatives see.
	sqlTokens = compile(
		`'(?:[^']|'')*'`+ // single-quoted string
			`|'[^']*\z`+ // string cut off by length limits
			`|\$([A-Za-z_]*)\$.*?\$\1\$`+ // PostgreSQL dollar-quoted string
			`|/\*.*?\*/`+ // block comment
			`|/\*.*\z`+ // block comment cut off
			`|--[^\r\n]*`, // line comment
		regexp2.Singleline)
	sqlDoubleQuoted = compile(`"(?:[^"]|"")*"|"[^"]*\z`, regexp2.Singleline)
	// MySQL only: elsewhere '#' starts identifiers (SQL Server temp tables) or operators (PostgreSQL).
	sqlHashComment = compile(`#[^\r\n]*`, regexp2.None)
	sqlHex         = compile(notWordBefore+`(?:0[xX][0-9a-fA-F]+|[xX]'[0-9a-fA-F]*')`, regexp2.None)
	// A sign directly before the digits belongs to the literal ("= -12" is "= ?"), but "a-1" keeps its minus.
	sqlNumber = compile(
		`(?<![A-Za-z0-9_$.@:?])(?:-(?=[0-9]))?(?<![A-Za-z0-9_)\]])[0-9]+(?:\.[0-9]+)?(?:[eE][+\-]?[0-9]+)?`+notWordAfter,
		regexp2.None)
	sqlBoolean    = compile(notWordBefore+`(?:[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee])`+notWordAfter, regexp2.None)
	sqlInList     = compile(notWordBefore+`[Ii][Nn]`+ws+`*\(`+ws+`*\?(?:`+ws+`*,`+ws+`*\?)*`+ws+`*\)`, regexp2.None)
	sqlValuesRows = compile(`(`+row+`)(?:`+ws+`*,`+ws+`*`+row+`)+`, regexp2.None)
	sqlWhitespace = compile(ws+`+`, regexp2.None)
)

// OpenTelemetry db.system (legacy) and db.system.name values of SQL databases.
var sqlSystems = set(
	"other_sql", "adabas", "cache", "clickhouse", "cloudscape", "cockroachdb", "db2", "derby", "edb", "firebird",
	"firstsql", "h2", "h2database", "hanadb", "hive", "hsqldb", "ibm.db2", "informix", "ingres", "instantdb",
	"interbase", "intersystems.cache", "mariadb", "maxdb", "microsoft.sql_server", "mssql", "mssqlcompact", "mysql",
	"netezza", "oracle", "oracle.db", "pervasive", "pointbase", "postgresql", "progress", "redshift", "sap.hana",
	"sap.maxdb", "snowflake", "spanner", "sqlite", "sybase", "teradata", "trino", "vertica",
)

// Systems where "..." is a string literal rather than a quoted identifier.
var doubleQuotedStringSystems = set("mysql", "mariadb")

func systemKey(system string) string { return strings.ToLower(Trim(system)) }

// IsSQLSystem reports whether statements of this db.system / db.system.name value may be exported at all.
func IsSQLSystem(system string) bool {
	_, found := sqlSystems[systemKey(system)]
	return found
}

// NormalizeSQL returns the value-free template of statement, and false when nothing usable remains.
func NormalizeSQL(statement, system string) (string, bool) {
	value, err := sqlTokens.ReplaceFunc(statement, func(m regexp2.Match) string {
		token := m.String()
		if strings.HasPrefix(token, "/*") || strings.HasPrefix(token, "--") {
			return " "
		}
		return "?"
	}, -1, -1)
	if err != nil {
		return "", false
	}
	type step struct {
		re   *regexp2.Regexp
		with string
	}
	var steps []step
	if _, found := doubleQuotedStringSystems[systemKey(system)]; found {
		steps = append(steps, step{sqlDoubleQuoted, "?"}, step{sqlHashComment, " "})
	}
	steps = append(steps, step{sqlHex, "?"}, step{sqlNumber, "?"}, step{sqlBoolean, "?"}, step{sqlWhitespace, " "})
	for _, s := range steps {
		if value, err = s.re.Replace(value, s.with, -1, -1); err != nil {
			return "", false
		}
	}
	value = Trim(value)
	if value, err = sqlInList.Replace(value, "IN (?)", -1, -1); err != nil {
		return "", false
	}
	if value, err = sqlValuesRows.Replace(value, "$1", -1, -1); err != nil {
		return "", false
	}
	for strings.HasSuffix(value, ";") {
		value = Trim(value[:len(value)-1])
	}
	return value, value != ""
}
