package duckdbsql

import (
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestParseDSN(t *testing.T) {
	path, opts, err := ParseDSN("")
	if err != nil || path != ":memory:" || len(opts) != 0 {
		t.Fatalf("empty DSN = (%q, %+v, %v)", path, opts, err)
	}
	path, opts, err = ParseDSN("/tmp/test.duckdb?threads=4&memory_limit=512MB&access_mode=READ_ONLY")
	if err != nil || path != "/tmp/test.duckdb" || len(opts) != 3 {
		t.Fatalf("file DSN = (%q, %+v, %v)", path, opts, err)
	}
	if opts[0] != (duckdb.Option{Name: "threads", Value: "4"}) || opts[2] != (duckdb.Option{Name: "access_mode", Value: "READ_ONLY"}) {
		t.Fatalf("unexpected DSN options: %+v", opts)
	}
	for _, bad := range []string{"bad\x00path", ":memory:?noequals", ":memory:?=val", ":memory:?k=%ZZ"} {
		if _, _, err := ParseDSN(bad); !errors.Is(err, duckdb.ErrInvalidInput) {
			t.Fatalf("ParseDSN(%q) = %v, want ErrInvalidInput", bad, err)
		}
	}
}

func TestValueConversionOptionA(t *testing.T) {
	// Exact microsecond time.Time succeeds as TIMESTAMPTZ.
	tm := time.Date(2026, 5, 6, 7, 8, 9, 123456000, time.UTC)
	dv, err := toDuckDBValue(tm)
	if err != nil {
		t.Fatalf("toDuckDBValue(time): %v", err)
	}
	ts, err := dv.Timestamp()
	if err != nil || ts.Unit != duckdb.TimestampUnitTZ || ts.Value != tm.UnixMicro() {
		t.Fatalf("converted timestamp = %+v (%v)", ts, err)
	}

	// Sub-microsecond time.Time rejected rather than silently truncated.
	tmNanos := time.Date(2026, 5, 6, 7, 8, 9, 123456789, time.UTC)
	if _, err := toDuckDBValue(tmNanos); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("sub-microsecond time.Time = %v, want ErrInvalidInput", err)
	}

	// DefaultValue rejected in SQL parameter binding.
	if _, err := toDuckDBValue(duckdb.DefaultValue()); !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("DefaultValue = %v, want ErrInvalidInput", err)
	}

	// Mixed positional + named arguments rejected.
	_, _, _, err = convertArgs([]driver.NamedValue{
		{Ordinal: 1, Value: int64(1)},
		{Ordinal: 2, Name: "a", Value: int64(2)},
	})
	if !errors.Is(err, duckdb.ErrInvalidInput) {
		t.Fatalf("mixed args = %v, want ErrInvalidInput", err)
	}

	// Option A: scanTime=false returns exact duckdb.Date / duckdb.Timestamp;
	// scanTime=true returns time.Time UTC.
	dateVal := duckdb.DateValue(duckdb.Date{Days: 20454})
	rawDate, err := fromDuckDBValue(dateVal, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rawDate.(duckdb.Date); !ok {
		t.Fatalf("scanTime=false date type = %T, want duckdb.Date", rawDate)
	}
	goDate, err := fromDuckDBValue(dateVal, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := goDate.(time.Time); !ok {
		t.Fatalf("scanTime=true date type = %T, want time.Time", goDate)
	}
}
