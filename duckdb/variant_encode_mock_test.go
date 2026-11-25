package duckdb

import (
	"errors"
	"math"
	"testing"
)

func TestVariantUvarint32RoundTrip(t *testing.T) {
	vals := []uint32{0, 1, 127, 128, 255, 16383, 16384, 1<<21 - 1, 1 << 21, 1<<28 - 1, math.MaxUint32}
	for _, want := range vals {
		buf := appendUvarint32(nil, want)
		got, n, err := decodeUvarint32(buf)
		if err != nil {
			t.Fatalf("decodeUvarint32(%d) err = %v", want, err)
		}
		if got != want || n != len(buf) {
			t.Fatalf("uvarint(%d): got=(%d, n=%d), want=(%d, n=%d)", want, got, n, want, len(buf))
		}
	}
}

func TestVariantEncoderObjectSortDedupAndLimits(t *testing.T) {
	limits := DefaultDecodeLimits()

	// Unsorted nested objects with shared keys -> keys deduplicated, children sorted by key name.
	innerObj, err := NewStructValue([]StructFieldValue{
		{Name: "z", Type: ScalarType(TypeInteger), Value: Int32Value(1)},
		{Name: "a", Type: ScalarType(TypeInteger), Value: Int32Value(2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := NewStructValue([]StructFieldValue{
		{Name: "z", Type: innerObj.Type(), Value: innerObj},
		{Name: "a", Type: ScalarType(TypeVarchar), Value: VarcharValue("first")},
	})
	if err != nil {
		t.Fatal(err)
	}

	enc := newVariantRowEncoder(limits)
	rootIdx, err := enc.encodeNode(VariantValue(rootObj), 0)
	if err != nil {
		t.Fatalf("encodeNode: %v", err)
	}
	if rootIdx != 0 {
		t.Fatalf("rootIdx = %d, want 0", rootIdx)
	}
	// Shared keys "a" and "z" should only appear once each in keys dictionary.
	if len(enc.keys) != 2 || enc.keys[0] != "a" || enc.keys[1] != "z" {
		t.Fatalf("keys = %v, want [a z]", enc.keys)
	}
	// Root children [0,1] must be in sorted key order: "a" (keyIdx 0), "z" (keyIdx 1).
	if len(enc.children) != 4 {
		t.Fatalf("children len = %d, want 4", len(enc.children))
	}
	if !enc.children[0].hasKey || enc.children[0].keyIdx != 0 || !enc.children[1].hasKey || enc.children[1].keyIdx != 1 {
		t.Fatalf("root children = %+v, want sorted keyIdx 0 then 1", enc.children[:2])
	}
	if !enc.children[2].hasKey || enc.children[2].keyIdx != 0 || !enc.children[3].hasKey || enc.children[3].keyIdx != 1 {
		t.Fatalf("inner children = %+v, want sorted keyIdx 0 then 1", enc.children[2:4])
	}

	// Duplicate object keys must be rejected.
	dupObj := Value{
		kind: uint8(TypeStruct),
		ref: []StructFieldValue{
			{Name: "k", Value: Int32Value(1)},
			{Name: "k", Value: Int32Value(2)},
		},
	}
	if _, err := newVariantRowEncoder(limits).encodeNode(dupObj, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate key err = %v, want ErrInvalidInput", err)
	}

	// Invalid UTF-8 in VARCHAR rejected.
	if _, err := newVariantRowEncoder(limits).encodeNode(VarcharValue(string([]byte{0xff, 0xfe})), 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid UTF-8 err = %v, want ErrInvalidInput", err)
	}

	// Non-VARCHAR MAP key rejected without lossy stringification.
	intMap, err := NewMapValue(ScalarType(TypeInteger), ScalarType(TypeVarchar), []MapEntry{
		{Key: Int32Value(1), Value: VarcharValue("one")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newVariantRowEncoder(limits).encodeNode(intMap, 0); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("non-VARCHAR map key err = %v, want ErrTypeMismatch", err)
	}

	// Depth limit enforced.
	tight := DecodeLimits{MaxNestingDepth: 1, MaxValueBytes: 1024, MaxCollectionLen: 16}
	if _, err := newVariantRowEncoder(tight).encodeNode(rootObj, 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("depth limit err = %v, want ErrLimitExceeded", err)
	}

	// Value size limit enforced.
	smallBytes := DecodeLimits{MaxNestingDepth: 8, MaxValueBytes: 4, MaxCollectionLen: 16}
	if _, err := newVariantRowEncoder(smallBytes).encodeNode(VarcharValue("12345"), 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("MaxValueBytes err = %v, want ErrLimitExceeded", err)
	}
}
