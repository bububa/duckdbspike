package duckdb

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/big"
	"runtime"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestHugeintUhugeintDecimalOwnershipAndRange(t *testing.T) {
	// Hugeint min / max round-trip via big.Int.
	min128, _ := new(big.Int).SetString("-170141183460469231731687303715884105728", 10)
	max128, _ := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	hMin, err := HugeintFromBigInt(min128)
	if err != nil || hMin.String() != min128.String() {
		t.Fatalf("Hugeint min: %v %v", hMin, err)
	}
	if _, ok := hMin.Int64(); ok {
		t.Fatal("Hugeint min should not fit in int64")
	}
	hMax, err := HugeintFromBigInt(max128)
	if err != nil || hMax.String() != max128.String() {
		t.Fatalf("Hugeint max: %v %v", hMax, err)
	}
	// Mutating returned *big.Int must not affect Hugeint.
	gotBig := hMax.BigInt()
	gotBig.SetInt64(0)
	if hMax.String() != max128.String() {
		t.Fatalf("Hugeint mutated through BigInt(): %s", hMax.String())
	}
	// Out of range rejected.
	if _, err := HugeintFromBigInt(new(big.Int).Add(max128, big.NewInt(1))); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("expected ErrOutOfRange above hugeint max, got %v", err)
	}
	if _, err := HugeintFromBigInt(new(big.Int).Sub(min128, big.NewInt(1))); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("expected ErrOutOfRange below hugeint min, got %v", err)
	}
	// int64 boundaries fit cleanly.
	for _, n := range []int64{math.MinInt64, -1, 0, 1, (1 << 53) + 1, math.MaxInt64} {
		h := HugeintFromInt64(n)
		back, ok := h.Int64()
		if !ok || back != n {
			t.Fatalf("HugeintFromInt64(%d) -> (%d, %v)", n, back, ok)
		}
	}

	// Uhugeint max & negative rejection.
	umax, _ := new(big.Int).SetString("340282366920938463463374607431768211455", 10)
	u, err := UhugeintFromBigInt(umax)
	if err != nil || u.String() != umax.String() {
		t.Fatalf("Uhugeint max: %v %v", u, err)
	}
	if _, ok := u.Uint64(); ok {
		t.Fatal("Uhugeint max should not fit in uint64")
	}
	if _, err := UhugeintFromBigInt(big.NewInt(-1)); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("negative Uhugeint = %v, want ErrOutOfRange", err)
	}
	if _, err := UhugeintFromBigInt(new(big.Int).Add(umax, big.NewInt(1))); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("overflow Uhugeint = %v, want ErrOutOfRange", err)
	}

	// Decimal exact formatting & bounds (width 1..38).
	d38Coef, _ := new(big.Int).SetString("-99999999999999999999999999999999999999", 10)
	d38, err := NewDecimal(38, 10, d38Coef)
	if err != nil {
		t.Fatalf("NewDecimal(38,10): %v", err)
	}
	if got := d38.String(); got != "-9999999999999999999999999999.9999999999" {
		t.Fatalf("d38.String() = %q", got)
	}
	// Small negative value with leading fractional zeros.
	dSmall, err := DecimalFromInt64(6, 4, -5)
	if err != nil || dSmall.String() != "-0.0005" {
		t.Fatalf("dSmall = %q, %v", dSmall.String(), err)
	}
	// Invalid widths, scales, and coefficient overflow rejected.
	if _, err := DecimalFromInt64(0, 0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("width=0: %v", err)
	}
	if _, err := DecimalFromInt64(39, 0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("width=39: %v", err)
	}
	if _, err := DecimalFromInt64(4, 5, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("scale>width: %v", err)
	}
	if _, err := DecimalFromInt64(4, 2, 10000); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("10000 for DECIMAL(4,2): %v", err)
	}
}

func TestValueRangeCheckedConversionsAndIsolation(t *testing.T) {
	// AsInt64 / AsUint64 reject overflow, negatives, floats, decimals, NULL.
	if _, err := Uint64Value(math.MaxUint64).AsInt64(); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("MaxUint64.AsInt64 = %v, want ErrOutOfRange", err)
	}
	if got, err := Uint64Value((1 << 53) + 1).AsInt64(); err != nil || got != (1<<53)+1 {
		t.Fatalf("2^53+1 AsInt64 = %d, %v", got, err)
	}
	if _, err := Int64Value(-1).AsUint64(); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("-1.AsUint64 = %v, want ErrOutOfRange", err)
	}
	if _, err := Float64Value(1.0).AsInt64(); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("Float64.AsInt64 = %v, want ErrTypeMismatch", err)
	}
	if _, err := NullValue().AsInt64(); !errors.Is(err, ErrNullValue) {
		t.Fatalf("Null.AsInt64 = %v, want ErrNullValue", err)
	}

	// Blob ownership: mutating input or output slice cannot alias internal bytes.
	src := []byte{0x00, 0x01, 0xff}
	bv := BlobValue(src)
	src[0] = 0x99
	got, err := bv.Blob()
	if err != nil || !bytes.Equal(got, []byte{0x00, 0x01, 0xff}) {
		t.Fatalf("Blob mutated via source: %x %v", got, err)
	}
	got[1] = 0x88
	got2, _ := bv.Blob()
	if !bytes.Equal(got2, []byte{0x00, 0x01, 0xff}) {
		t.Fatalf("Blob mutated via returned slice: %x", got2)
	}

	// Empty blob vs NULL blob.
	empty := BlobValue(nil)
	if empty.IsNull() {
		t.Fatal("empty BlobValue must not be SQL NULL")
	}
	eb, err := empty.Blob()
	if err != nil || eb == nil || len(eb) != 0 {
		t.Fatalf("empty Blob = %#v, %v", eb, err)
	}
	nullBlob := TypedNullValue(ScalarType(TypeBlob))
	if !nullBlob.IsNull() || nullBlob.Kind() != TypeBlob {
		t.Fatalf("nullBlob = null:%v kind:%v", nullBlob.IsNull(), nullBlob.Kind())
	}
	if _, err := nullBlob.Blob(); !errors.Is(err, ErrNullValue) {
		t.Fatalf("nullBlob.Blob() = %v, want ErrNullValue", err)
	}
}

func TestDecodeOverflowGuardsAndZeroLeaksOnTypeFailure(t *testing.T) {
	ctx := context.Background()

	// Nested STRUCT whose second field is unsupported (UNION) must destroy
	// every allocated child logical type and free child name strings.
	t.Run("nested_unsupported_struct_zero_leaks", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		structID := uintptr(9001)
		child0 := uintptr(9002)
		child1 := uintptr(9003)

		api.ColumnLogicalType = func(r *abi.Result, col uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.ltypes[structID] = abi.TypeStruct
			return structID
		}
		api.StructTypeChildCount = func(lt uintptr) uint64 { return 2 }
		api.StructTypeChildName = func(lt uintptr, i uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.ownedCstr("field")
		}
		api.StructTypeChildType = func(lt uintptr, i uint64) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			if i == 0 {
				f.ltypes[child0] = abi.TypeBigint
				return child0
			}
			f.ltypes[child1] = abi.TypeUnion
			return child1
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.resultColumns = []string{"st"}
		_, err := conn.Query(ctx, "SELECT st")
		if !errors.Is(err, ErrUnsupportedType) {
			t.Fatalf("got err=%v, want ErrUnsupportedType", err)
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})

	// List entry uint64 overflow (Offset=MaxUint64, Length=2) rejected cleanly.
	t.Run("list_entry_overflow_rejected", func(t *testing.T) {
		var pinner runtime.Pinner
		defer pinner.Unpin()
		entry := abi.ListEntry{Offset: math.MaxUint64, Length: 2}
		pinner.Pin(&entry)

		api := &abi.API{
			VectorGetValidity: func(vec uintptr) uintptr { return 0 },
			VectorGetData:     func(vec uintptr) uintptr { return uintptr(unsafe.Pointer(&entry)) },
			ListVectorGetSize: func(vec uintptr) uint64 { return 10 },
		}
		plan := columnPlan{
			lt:   LogicalType{Kind: TypeList, Elem: &LogicalType{Kind: TypeInteger}},
			elem: &columnPlan{lt: ScalarType(TypeInteger)},
		}
		if _, err := decodeVectorValue(api, &plan, 1, 0, DefaultDecodeLimits()); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	// Array row*size overflow rejected cleanly.
	t.Run("array_row_mul_overflow_rejected", func(t *testing.T) {
		api := &abi.API{
			VectorGetValidity: func(vec uintptr) uintptr { return 0 },
		}
		plan := columnPlan{
			lt:   LogicalType{Kind: TypeArray, ArraySize: 4, Elem: &LogicalType{Kind: TypeInteger}},
			elem: &columnPlan{lt: ScalarType(TypeInteger)},
		}
		if _, err := decodeVectorValue(api, &plan, 1, math.MaxUint64/2, DefaultDecodeLimits()); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	// BindValue failure destroys duckdb_value and clears prior bindings.
	t.Run("bind_value_failure_destroys_native_value", func(t *testing.T) {
		f, lib := newFakeNative(t)
		api := lib.api
		api.CreateTimestampNS = func(v abi.TimestampNS) uintptr {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := f.allocID()
			f.values[id] = struct{}{}
			return id
		}
		api.BindValue = func(stmt uintptr, idx uint64, val uintptr) int32 {
			return abi.StateError
		}
		api.DestroyValue = func(val *uintptr) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if val == nil || *val == 0 {
				return
			}
			delete(f.values, *val)
			*val = 0
		}

		db, _ := lib.Open(ctx, ":memory:")
		conn, _ := db.Connect(ctx)
		f.nParams = 1
		ts, _ := TimestampValue(Timestamp{Value: 123, Unit: TimestampUnitNanos})
		if _, err := conn.Query(ctx, "SELECT ?", ts); err == nil {
			t.Fatal("expected bind error")
		}
		_ = db.Close()
		f.assertZeroLeaks(t)
	})
}
