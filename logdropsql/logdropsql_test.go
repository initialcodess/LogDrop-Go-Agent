package logdropsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strconv"
	"testing"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type fakeDriver struct{}
type fakeConn struct{}
type fakeRows struct{ done bool }

func (fakeDriver) Open(string) (driver.Conn, error)  { return fakeConn{}, nil }
func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{}, nil
}
func (*fakeRows) Columns() []string { return []string{"total"} }
func (*fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(42)
	return nil
}

func TestQueriesBecomeValueFreeSpans(t *testing.T) {
	sql.Register("fakepg", fakeDriver{})
	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	db, err := Open("fakepg", "postgres://app:secret@db.internal:5433/orders?sslmode=disable", WithSystem("postgresql"))
	if err != nil {
		t.Fatal(err)
	}
	var total int
	if err := db.QueryRowContext(context.Background(), "select total from orders where id = 42 and owner = 'ayse'").Scan(&total); err != nil {
		t.Fatal(err)
	}
	spans := recorder.Ended()
	if len(spans) != 1 {
		names := []string{}
		for _, s := range spans {
			names = append(names, s.Name())
		}
		t.Fatalf("spans %v", names)
	}
	limits, _ := core.MvpLimits(nil)
	safe := map[string]any{}
	for _, a := range core.SanitizeSpan(spans[0].Attributes(), limits) {
		safe[a.Key] = a.Value
	}
	if spans[0].Name() != "SELECT" || safe["db.system.name"] != "postgresql" || safe["server.address"] != "db.internal" ||
		safe["server.port"] != int64(5433) || safe["db.namespace"] != "orders" ||
		safe["db.query.text"] != "select total from orders where id = ? and owner = ?" {
		t.Fatalf("span %s %+v", spans[0].Name(), safe)
	}
}

func TestServerFromDataSourceNames(t *testing.T) {
	for dsn, want := range map[string][3]string{
		"postgres://u:p@pg:5432/shop":                        {"pg", "5432", "shop"},
		"host=pg port=6432 dbname=shop user=u password=p":    {"pg", "6432", "shop"},
		"u:p@tcp(mysql:3306)/shop?parseTime=true":            {"mysql", "3306", "shop"},
		"Server=sql,1433;Database=shop;User Id=u;Password=p": {"sql", "", "shop"},
		"file:local.db": {"", "", ""},
	} {
		host, port, name := server(dsn)
		p := ""
		if port > 0 {
			p = strconv.Itoa(port)
		}
		if host != want[0] || p != want[1] || name != want[2] {
			t.Errorf("%s: %s %s %s", dsn, host, p, name)
		}
	}
	if SystemOf("pgx") != "postgresql" || SystemOf("unknown") != "other_sql" {
		t.Error("systems")
	}
}
