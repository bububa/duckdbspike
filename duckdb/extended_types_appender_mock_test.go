package duckdb

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestTypesInfinityErrorKindsAndLayout(t *testing.T) {
	if sz := unsafe.Sizeof(Value{}); sz > 48 {
		t.Fatalf("sizeof(Value) = %d bytes, want <= 48", sz)
	}

	// Date & Timestamp infinity sentinels.
	if !(Date{Days: DateInfinity}).IsInfinity() || (Date{Days: DateInfinity}).IsFinite() {
		t.Fatal("DateInfinity failed predicate")
	}
	if !(Date{Days: DateNegInfinity}).IsNegInfinity() || (Date{Days: DateNegInfinity}).IsFinite() {
		t.Fatal("DateNegInfinity failed predicate")
	}
	if !(Date{Days: 0}).IsFinite() {
		t.Fatal("epoch Date should be finite")
	}
	if !(Timestamp{Value: TimestampInfinity, Unit: TimestampUnitMicros}).IsInfinity() {
		t.Fatal("TimestampInfinity failed predicate")
	}
	if !(Timestamp{Value: TimestampNegInfinity, Unit: TimestampUnitTZ}).IsNegInfinity() {
		t.Fatal("TimestampNegInfinity failed predicate")
	}

	// Bit parse, format, copy isolation, validation.
	b, err := NewBitFromString("10110")
	if err != nil || b.BitLen() != 5 || b.String() != "10110" {
		t.Fatalf("NewBitFromString = %+v (%v)", b, err)
	}
	bv, err := BitValue(b)
	if err != nil {
		t.Fatal(err)
	}
	b.Data[1] = 0
	gotB, err := bv.Bit()
	if err != nil || gotB.String() != "10110" {
		t.Fatalf("Bit isolated copy failed: %q (%v)", gotB.String(), err)
	}
	if _, err := NewBitFromString("102"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid bit char = %v, want ErrInvalidInput", err)
	}

	// Bignum ownership & sign handling.
	hugeStr := "-123456789012345678901234567890123456789012345678901234567890"
	bi, _ := new(big.Int).SetString(hugeStr, 10)
	bn, err := BignumFromBigInt(bi)
	if err != nil || !bn.IsNegative() || bn.String() != hugeStr {
		t.Fatalf("BignumFromBigInt = %q (%v)", bn.String(), err)
	}
	bi.SetInt64(0)
	if bn.String() != hugeStr {
		t.Fatal("Bignum aliased source *big.Int")
	}
	bnVal := BignumValue(bn)
	gotBN, err := bnVal.Bignum()
	if err != nil || gotBN.String() != hugeStr {
		t.Fatalf("Bignum Value round-trip = %q (%v)", gotBN.String(), err)
	}

	// UnionType & NewUnionValue validation.
	ut := UnionType(
		StructFieldType{Name: "num", Type: ScalarType(TypeInteger)},
		StructFieldType{Name: "txt", Type: ScalarType(TypeVarchar)},
	)
	uv, err := NewUnionValue(ut, 1, VarcharValue("hello"))
	if err != nil {
		t.Fatal(err)
	}
	gotU, err := uv.Union()
	if err != nil || gotU.Tag != 1 || gotU.TagName != "txt" {
		t.Fatalf("Union round-trip = %+v (%v)", gotU, err)
	}
	if _, err := NewUnionValue(ut, 2, Int32Value(1)); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("oob union tag = %v, want ErrOutOfRange", err)
	}
	if _, err := NewUnionValue(ut, 0, VarcharValue("wrong")); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("mismatched union member = %v, want ErrTypeMismatch", err)
	}

	// ErrorKind classification coverage.
	kindCases := map[int32]ErrorKind{
		abi.ErrorConstraint:           ErrorKindConstraint,
		abi.ErrorCatalog:              ErrorKindCatalog,
		abi.ErrorSyntax:               ErrorKindSyntax,
		abi.ErrorParser:               ErrorKindParser,
		abi.ErrorBinder:               ErrorKindBinder,
		abi.ErrorConversion:           ErrorKindConversion,
		abi.ErrorOutOfRange:           ErrorKindOutOfRange,
		abi.ErrorInvalidInput:         ErrorKindInvalidInput,
		abi.ErrorIO:                   ErrorKindIO,
		abi.ErrorTransaction:          ErrorKindTransaction,
		abi.ErrorConnection:           ErrorKindConnection,
		abi.ErrorInvalidType:          ErrorKindInvalidType,
		abi.ErrorSerialization:        ErrorKindSerialization,
		abi.ErrorNotImplemented:       ErrorKindNotImplemented,
		abi.ErrorHTTP:                 ErrorKindHTTP,
		abi.ErrorMissingExtension:     ErrorKindMissingExtension,
		abi.ErrorInvalidConfiguration: ErrorKindConfiguration,
	}
	for nativeCode, want := range kindCases {
		if got := kindOf(nativeCode); got != want {
			t.Fatalf("kindOf(%d) = %d, want %d", nativeCode, got, want)
		}
	}
}

func TestQueryAppenderMockFailureZeroLeaks(t *testing.T) {
	ctx := context.Background()
	f, lib := newFakeNative(t)
	api := lib.api

	appenders := map[uintptr]string{}
	errDatas := map[uintptr]string{}

	api.CreateLogicalType = func(id int32) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		h := f.allocID()
		f.ltypes[h] = id
		return h
	}
	api.ErrorDataHasError = func(ed uintptr) bool { return errDatas[ed] != "" }
	api.ErrorDataErrorType = func(ed uintptr) int32 { return abi.ErrorBinder }
	api.ErrorDataMessage = func(ed uintptr) uintptr { return f.cstr(errDatas[ed]) }
	api.DestroyErrorData = func(ed *uintptr) {
		if ed == nil || *ed == 0 {
			return
		}
		delete(errDatas, *ed)
		*ed = 0
	}
	api.AppenderCreateQuery = func(conn uintptr, q *byte, colCount uint64, types *uintptr, tbl *byte, colNames *uintptr, out *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		appenders[id] = "binder error in upsert query"
		*out = id
		return abi.StateError
	}
	api.AppenderErrorData = func(app uintptr) uintptr {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.allocID()
		errDatas[id] = appenders[app]
		return id
	}
	api.AppenderDestroy = func(app *uintptr) int32 {
		f.mu.Lock()
		defer f.mu.Unlock()
		if app == nil || *app == 0 {
			return abi.StateSuccess
		}
		delete(appenders, *app)
		*app = 0
		return abi.StateSuccess
	}

	db, _ := lib.Open(ctx, ":memory:")
	conn, _ := db.Connect(ctx)

	// Partial column names rejected before native call.
	if _, err := conn.NewQueryAppender(ctx, "INSERT INTO t SELECT * FROM appended_data", []Column{
		{Name: "id", Type: ScalarType(TypeBigint)},
		{Name: "", Type: ScalarType(TypeVarchar)},
	}, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("partial col names = %v, want ErrInvalidInput", err)
	}

	// Failure in AppenderCreateQuery cleans up logical types, ErrorData, and appender handle.
	_, err := conn.NewQueryAppender(ctx, "INSERT INTO t SELECT * FROM appended_data", []Column{
		{Name: "id", Type: ScalarType(TypeBigint)},
		{Name: "v", Type: ScalarType(TypeVarchar)},
	}, "appended_data")
	var derr *Error
	if !errors.As(err, &derr) || derr.Kind != ErrorKindBinder || derr.Message != "binder error in upsert query" {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(appenders) != 0 || len(errDatas) != 0 {
		t.Fatalf("leaked appenders=%d errDatas=%d", len(appenders), len(errDatas))
	}
	_ = db.Close()
	f.assertZeroLeaks(t)
}
