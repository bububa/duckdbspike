package duckdb

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestTypeHelpersAndValueAs(t *testing.T) {
	// Date helpers.
	tm := time.Date(2026, 11, 24, 15, 30, 0, 0, time.UTC)
	d, err := DateFromTime(tm)
	if err != nil {
		t.Fatalf("DateFromTime: %v", err)
	}
	if d.String() != "2026-11-24" {
		t.Fatalf("Date.String = %q, want 2026-11-24", d.String())
	}
	dt, err := d.Time()
	if err != nil || dt.Format("2006-01-02") != "2026-11-24" {
		t.Fatalf("Date.Time = %v (%v)", dt, err)
	}
	if (Date{Days: DateInfinity}).String() != "infinity" || (Date{Days: DateNegInfinity}).String() != "-infinity" {
		t.Fatal("Date infinity String mismatch")
	}
	if _, err := (Date{Days: DateInfinity}).Time(); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("DateInfinity.Time = %v, want ErrOutOfRange", err)
	}

	// Timestamp helpers (strict precision check).
	tSec := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	tsS, err := TimestampFromTime(tSec, TimestampUnitSeconds)
	if err != nil || tsS.String() != "2026-05-01T12:00:00Z" {
		t.Fatalf("TimestampFromTime(s) = %v (%v)", tsS, err)
	}
	tSub := time.Date(2026, 5, 1, 12, 0, 0, 123, time.UTC)
	if _, err := TimestampFromTime(tSub, TimestampUnitMicros); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("sub-micros TimestampFromTime = %v, want ErrInvalidInput", err)
	}
	tsN, err := TimestampFromTime(tSub, TimestampUnitNanos)
	if err != nil {
		t.Fatalf("TimestampFromTime(ns): %v", err)
	}
	gotTN, err := tsN.Time()
	if err != nil || !gotTN.Equal(tSub) {
		t.Fatalf("Timestamp.Time(ns) = %v (%v), want %v", gotTN, err, tSub)
	}
	if (Timestamp{Value: TimestampNegInfinity, Unit: TimestampUnitTZ}).String() != "-infinity" {
		t.Fatal("TimestampNegInfinity String mismatch")
	}
	if _, err := (Timestamp{Value: TimestampInfinity, Unit: TimestampUnitMicros}).Time(); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("TimestampInfinity.Time = %v, want ErrOutOfRange", err)
	}

	// Time, TimeNS, TimeTZ, Interval String.
	if got := (Time{Micros: 3661_500_000}).String(); got != "01:01:01.5" {
		t.Fatalf("Time.String = %q", got)
	}
	if got := (TimeNS{Nanos: 3661_000_000_005}).String(); got != "01:01:01.000000005" {
		t.Fatalf("TimeNS.String = %q", got)
	}
	if got := (TimeTZ{Micros: 12 * 3600 * 1_000_000, OffsetSeconds: 8 * 3600}).String(); got != "12:00:00+08:00" {
		t.Fatalf("TimeTZ.String = %q", got)
	}
	if got := (Interval{Months: 2, Days: 3, Micros: 4 * 3600 * 1_000_000}).String(); got != "2 months 3 days 04:00:00" {
		t.Fatalf("Interval.String = %q", got)
	}

	// ParseDecimal & BigRat.
	dec, err := ParseDecimal("-123.45", 10, 4)
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	if dec.String() != "-123.4500" {
		t.Fatalf("dec.String = %q, want -123.4500", dec.String())
	}
	wantRat := big.NewRat(-12345, 100)
	if dec.BigRat().Cmp(wantRat) != 0 {
		t.Fatalf("BigRat = %v, want %v", dec.BigRat(), wantRat)
	}
	if _, err := ParseDecimal("1.234", 5, 2); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("excess scale ParseDecimal = %v, want ErrOutOfRange", err)
	}
	if _, err := ParseDecimal("abc", 5, 2); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid ParseDecimal = %v, want ErrInvalidInput", err)
	}

	// ValueAs[T].
	if got, err := ValueAs[int64](Int64Value(42)); err != nil || got != 42 {
		t.Fatalf("ValueAs[int64] = %d (%v)", got, err)
	}
	if got, err := ValueAs[string](VarcharValue("ok")); err != nil || got != "ok" {
		t.Fatalf("ValueAs[string] = %q (%v)", got, err)
	}
	if _, err := ValueAs[int64](VarcharValue("nope")); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("ValueAs mismatch = %v, want ErrTypeMismatch", err)
	}
}

func TestContextMetadataAndTableNamesMockZeroLeaks(t *testing.T) {
	var (
		cStrings      [][]byte
		liveCtx       int
		liveVals      int
		freedCount    int
		destroyedLTs  int
		liveListElems = map[uintptr]string{}
	)
	allocCStr := func(s string) uintptr {
		b, _ := abi.CString(s)
		cStrings = append(cStrings, b)
		return uintptr(unsafe.Pointer(&b[0]))
	}

	const (
		hValThreads = uintptr(201)
		hValMem     = uintptr(202)
		hValNull    = uintptr(203)
		hValTables  = uintptr(204)
		hValT1      = uintptr(205)
		hValT2      = uintptr(206)
		ltBigint    = uintptr(301)
		ltVarchar   = uintptr(302)
	)
	liveListElems[hValT1] = "orders"
	liveListElems[hValT2] = "users"

	fakeAPI := &abi.API{
		ConnectionGetClientContext: func(conn uintptr, out *uintptr) {
			liveCtx++
			*out = 101
		},
		DestroyClientContext: func(ctx *uintptr) {
			if ctx != nil && *ctx != 0 {
				liveCtx--
				*ctx = 0
			}
		},
		ClientContextGetConnectionID: func(ctx uintptr) uint64 { return 8888 },
		ClientContextGetConfigOption: func(ctx uintptr, name *byte, outScope *int32) uintptr {
			switch abi.GoString(uintptr(unsafe.Pointer(name))) {
			case "threads":
				liveVals++
				*outScope = abi.ConfigOptionScopeGlobal
				return hValThreads
			case "memory_limit":
				liveVals++
				*outScope = abi.ConfigOptionScopeSession
				return hValMem
			case "nullable_opt":
				liveVals++
				*outScope = abi.ConfigOptionScopeSession
				return hValNull
			default:
				*outScope = abi.ConfigOptionScopeInvalid
				return 0
			}
		},
		GetTableNames: func(conn uintptr, query *byte, qualified bool) uintptr {
			if abi.GoString(uintptr(unsafe.Pointer(query))) == "INVALID" {
				return 0
			}
			liveVals++
			return hValTables
		},
		GetValueType: func(val uintptr) uintptr {
			switch val {
			case hValThreads:
				return ltBigint
			case hValMem, hValNull:
				return ltVarchar
			default:
				return 0
			}
		},
		GetTypeID: func(lt uintptr) int32 {
			switch lt {
			case ltBigint:
				return abi.TypeBigint
			case ltVarchar:
				return abi.TypeVarchar
			default:
				return abi.TypeInvalid
			}
		},
		LogicalTypeGetAlias: func(lt uintptr) uintptr { return 0 },
		DestroyLogicalType: func(lt *uintptr) {
			if lt != nil && *lt != 0 {
				destroyedLTs++
				*lt = 0
			}
		},
		IsNullValue: func(val uintptr) bool { return val == hValNull },
		GetInt64:    func(val uintptr) int64 { return 4 },
		GetVarchar: func(val uintptr) uintptr {
			if s, ok := liveListElems[val]; ok {
				return allocCStr(s)
			}
			return allocCStr("256MiB")
		},
		GetListSize: func(val uintptr) uint64 { return 2 },
		GetListChild: func(val uintptr, idx uint64) uintptr {
			liveVals++
			if idx == 0 {
				return hValT1
			}
			return hValT2
		},
		Free: func(ptr uintptr) {
			if ptr != 0 {
				freedCount++
			}
		},
		DestroyValue: func(val *uintptr) {
			if val != nil && *val != 0 {
				liveVals--
				*val = 0
			}
		},
	}

	conn := &Conn{
		api:    fakeAPI,
		handle: 1,
		limits: DefaultDecodeLimits(),
		stmts:  make(map[*Stmt]struct{}),
	}

	id, err := conn.ConnectionID()
	if err != nil || id != 8888 {
		t.Fatalf("ConnectionID = %d (%v), want 8888", id, err)
	}
	threads, scope, ok, err := ConfigOptionAs[int64](conn, "threads")
	if err != nil || !ok || threads != 4 || scope != ConfigOptionScopeGlobal {
		t.Fatalf("ConfigOptionAs(threads) = (%d, %s, %v, %v)", threads, scope, ok, err)
	}
	mem, scope, ok, err := ConfigOptionAs[string](conn, "memory_limit")
	if err != nil || !ok || mem != "256MiB" || scope != ConfigOptionScopeSession {
		t.Fatalf("ConfigOptionAs(memory_limit) = (%q, %s, %v, %v)", mem, scope, ok, err)
	}
	nullOpt, ok, err := conn.ConfigOption("nullable_opt")
	if err != nil || !ok || !nullOpt.Value.IsNull() {
		t.Fatalf("ConfigOption(nullable_opt) = (%+v, %v, %v)", nullOpt, ok, err)
	}
	_, ok, err = conn.ConfigOption("does_not_exist")
	if err != nil || ok {
		t.Fatalf("ConfigOption(missing) = (%v, %v), want (false, nil)", ok, err)
	}
	// Borrowed logical type from GetValueType must NEVER be destroyed!
	if destroyedLTs != 0 {
		t.Fatalf("GetValueType returned borrowed logical type but DestroyLogicalType was called %d times", destroyedLTs)
	}

	tables, err := conn.TableNames(context.Background(), "SELECT * FROM orders JOIN users", false)
	if err != nil || len(tables) != 2 || tables[0] != "orders" || tables[1] != "users" {
		t.Fatalf("TableNames = %v (%v)", tables, err)
	}
	if _, err := conn.TableNames(context.Background(), "INVALID", false); err == nil {
		t.Fatal("expected error for invalid SQL in TableNames")
	}
	if liveCtx != 0 || liveVals != 0 || freedCount != 3 {
		t.Fatalf("leak check failed: liveCtx=%d liveVals=%d freed=%d", liveCtx, liveVals, freedCount)
	}
}
