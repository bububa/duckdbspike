package duckdb

import (
	"encoding/binary"
	"errors"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestVariantSchemaValidationZeroLeaks(t *testing.T) {
	runCase := func(name string, childCount uint64, childNames [4]string, dataType int32, wantErr bool) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			var (
				cStrings   [][]byte
				liveLTs    = map[uintptr]int32{}
				freedNames int
				allocNames int
				nextID     = uintptr(1000)
			)
			allocLT := func(tid int32) uintptr {
				nextID++
				liveLTs[nextID] = tid
				return nextID
			}
			allocName := func(s string) uintptr {
				allocNames++
				b, _ := abi.CString(s)
				cStrings = append(cStrings, b)
				return uintptr(unsafe.Pointer(&b[0]))
			}

			rootLT := uintptr(1)
			liveLTs[rootLT] = abi.TypeVariant
			keysLT := uintptr(10)
			childrenLT := uintptr(11)
			valuesLT := uintptr(12)
			dataLT := uintptr(13)
			childStructLT := uintptr(20)
			valStructLT := uintptr(21)

			api := &abi.API{
				GetTypeID: func(lt uintptr) int32 { return liveLTs[lt] },
				StructTypeChildCount: func(lt uintptr) uint64 {
					if lt == rootLT {
						return childCount
					}
					return 2
				},
				StructTypeChildName: func(lt uintptr, i uint64) uintptr {
					switch lt {
					case rootLT:
						return allocName(childNames[i])
					case childStructLT:
						if i == 0 {
							return allocName("keys_index")
						}
						return allocName("values_index")
					case valStructLT:
						if i == 0 {
							return allocName("type_id")
						}
						return allocName("byte_offset")
					default:
						return 0
					}
				},
				StructTypeChildType: func(lt uintptr, i uint64) uintptr {
					switch lt {
					case rootLT:
						switch i {
						case 0:
							liveLTs[keysLT] = abi.TypeList
							return keysLT
						case 1:
							liveLTs[childrenLT] = abi.TypeList
							return childrenLT
						case 2:
							liveLTs[valuesLT] = abi.TypeList
							return valuesLT
						default:
							liveLTs[dataLT] = dataType
							return dataLT
						}
					case childStructLT:
						return allocLT(abi.TypeUInteger)
					case valStructLT:
						if i == 0 {
							return allocLT(abi.TypeUTinyint)
						}
						return allocLT(abi.TypeUInteger)
					default:
						return 0
					}
				},
				ListTypeChildType: func(lt uintptr) uintptr {
					switch lt {
					case keysLT:
						return allocLT(abi.TypeVarchar)
					case childrenLT:
						liveLTs[childStructLT] = abi.TypeStruct
						return childStructLT
					case valuesLT:
						liveLTs[valStructLT] = abi.TypeStruct
						return valStructLT
					default:
						return 0
					}
				},
				DestroyLogicalType: func(lt *uintptr) {
					if lt != nil && *lt != 0 {
						delete(liveLTs, *lt)
						*lt = 0
					}
				},
				Free: func(ptr uintptr) {
					if ptr != 0 {
						freedNames++
					}
				},
			}

			err := validateVariantLogicalType(api, rootLT)
			if wantErr && !errors.Is(err, ErrUnsupportedType) {
				t.Fatalf("err = %v, want ErrUnsupportedType", err)
			}
			if !wantErr && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			// Only rootLT (caller-owned) should remain live; all child LTs and names must be freed.
			if len(liveLTs) != 1 || freedNames != allocNames {
				t.Fatalf("leak detected: liveLTs=%d (want 1) freedNames=%d allocNames=%d", len(liveLTs), freedNames, allocNames)
			}
		})
	}

	runCase("valid_v1_5_6_layout", 4, [4]string{"keys", "children", "values", "data"}, abi.TypeBlob, false)
	runCase("wrong_child_count", 3, [4]string{"keys", "children", "values", "data"}, abi.TypeBlob, true)
	runCase("wrong_child_name", 4, [4]string{"keys", "bad", "values", "data"}, abi.TypeBlob, true)
	runCase("wrong_data_type", 4, [4]string{"keys", "children", "values", "data"}, abi.TypeVarchar, true)
}

func TestVariantDecoderBoundsCycleAndLimitsMock(t *testing.T) {
	// Helper to build a synthetic in-memory VARIANT row decoder directly.
	makeDec := func(types []uint8, offsets []uint32, childVals []uint32, data []byte, limits DecodeLimits) *variantRowDecoder {
		var tPtr, oPtr, cvPtr uintptr
		if len(types) > 0 {
			tPtr = uintptr(unsafe.Pointer(&types[0]))
		}
		if len(offsets) > 0 {
			oPtr = uintptr(unsafe.Pointer(&offsets[0]))
		}
		if len(childVals) > 0 {
			cvPtr = uintptr(unsafe.Pointer(&childVals[0]))
		}
		return &variantRowDecoder{
			limits:          limits,
			childrenEntry:   abi.ListEntry{Offset: 0, Length: uint64(len(childVals))},
			childValsData:   cvPtr,
			valuesEntry:     abi.ListEntry{Offset: 0, Length: uint64(len(types))},
			valTypeData:     tPtr,
			valOffsetData:   oPtr,
			data:            data,
			visiting:        make(map[uint32]struct{}),
			maxDecodedNodes: limits.MaxCollectionLen + 1,
		}
	}

	limits := DefaultDecodeLimits()

	// 1. Out-of-bounds byte_offset.
	d1 := makeDec([]uint8{variantTypeInt32}, []uint32{10}, nil, []byte{1, 2, 3, 4}, limits)
	if _, err := d1.decodeNode(0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("oob byte_offset = %v, want ErrInvalidInput", err)
	}

	// 2. Truncated fixed scalar payload.
	d2 := makeDec([]uint8{variantTypeInt64}, []uint32{0}, nil, []byte{1, 2, 3}, limits)
	if _, err := d2.decodeNode(0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("truncated int64 = %v, want ErrInvalidInput", err)
	}

	// 3. Cycle detection (array at 0 references itself at values_index 0).
	arrayHdr := []byte{1, 0} // count=1, children_idx=0
	d3 := makeDec([]uint8{variantTypeArray}, []uint32{0}, []uint32{0}, arrayHdr, limits)
	if _, err := d3.decodeNode(0, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cyclic array = %v, want ErrInvalidInput", err)
	}

	// 4. MaxNestingDepth enforcement.
	tightDepth := limits
	tightDepth.MaxNestingDepth = 1
	// array(0) -> array(1) -> int32(2) => depth 2 > 1
	d4Data := []byte{1, 0, 1, 1, 7, 0, 0, 0}
	d4 := makeDec(
		[]uint8{variantTypeArray, variantTypeArray, variantTypeInt32},
		[]uint32{0, 2, 4},
		[]uint32{1, 2},
		d4Data,
		tightDepth,
	)
	if _, err := d4.decodeNode(0, 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("depth limit = %v, want ErrLimitExceeded", err)
	}

	// 5. MaxValueBytes enforcement on VARCHAR inside VARIANT.
	tightBytes := limits
	tightBytes.MaxValueBytes = 4
	strData := append([]byte{8}, []byte("12345678")...)
	d5 := makeDec([]uint8{variantTypeVarchar}, []uint32{0}, nil, strData, tightBytes)
	if _, err := d5.decodeNode(0, 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("value bytes limit = %v, want ErrLimitExceeded", err)
	}

	// 6. Unknown VariantLogicalType tag -> ErrUnsupportedType.
	d6 := makeDec([]uint8{250}, []uint32{0}, nil, nil, limits)
	if _, err := d6.decodeNode(0, 0); !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("unknown tag = %v, want ErrUnsupportedType", err)
	}

	// 7. Outer SQL NULL vs inner VARIANT_NULL distinction.
	outerNull := TypedNullValue(ScalarType(TypeVariant))
	if !outerNull.IsNull() {
		t.Fatal("outerNull should have IsNull() == true")
	}
	if _, err := outerNull.Variant(); !errors.Is(err, ErrNullValue) {
		t.Fatalf("outerNull.Variant() = %v, want ErrNullValue", err)
	}
	dNull := makeDec([]uint8{variantTypeNull}, []uint32{0}, nil, nil, limits)
	innerNullVal, err := dNull.decodeNode(0, 0)
	if err != nil {
		t.Fatalf("inner VARIANT_NULL decode: %v", err)
	}
	wrapped := VariantValue(innerNullVal)
	if wrapped.IsNull() {
		t.Fatal("inner VARIANT_NULL wrapper must NOT report outer SQL NULL")
	}
	vr, err := wrapped.Variant()
	if err != nil || !vr.IsNull() || vr.Value.Kind() != TypeSQLNull {
		t.Fatalf("inner VARIANT_NULL = %+v (%v), want Variant.IsNull() == true", vr, err)
	}

	// 8. Exact DECIMAL16/32/64/128 decoding in VARIANT.
	var decBuf []byte
	decBuf = append(decBuf, 4, 2) // width=4, scale=2
	decBuf = binary.LittleEndian.AppendUint16(decBuf, uint16(1234))
	dDec := makeDec([]uint8{variantTypeDecimal}, []uint32{0}, nil, decBuf, limits)
	dv, err := dDec.decodeNode(0, 0)
	if err != nil {
		t.Fatalf("variant decimal decode: %v", err)
	}
	if dec, err := dv.Decimal(); err != nil || dec.String() != "12.34" {
		t.Fatalf("variant decimal = %s (%v), want 12.34", dec.String(), err)
	}
}
