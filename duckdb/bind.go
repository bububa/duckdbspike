package duckdb

import (
	"fmt"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// emptyBindByte provides a non-nil pointer when binding zero-length strings
// or blobs so native code never receives a NULL data pointer.
var emptyBindByte byte

func (s *Stmt) bindLocked(args []Value) error {
	api := s.conn.api
	expected := api.NParams(s.handle)
	if uint64(len(args)) != expected {
		return fmt.Errorf("%w: statement expects %d parameters, got %d", ErrInvalidInput, expected, len(args))
	}
	if expected == 0 {
		return nil
	}
	if api.ClearBindings(s.handle) != abi.StateSuccess {
		return nativeError("clear_bindings", abi.ErrorInvalid, abi.GoString(api.PrepareError(s.handle)))
	}
	for i, arg := range args {
		idx := uint64(i + 1)
		if err := s.bindOneLocked(idx, arg); err != nil {
			_ = api.ClearBindings(s.handle)
			return err
		}
	}
	return nil
}

func (s *Stmt) bindOneLocked(idx uint64, arg Value) error {
	api := s.conn.api
	var state int32
	if arg.IsNull() {
		state = api.BindNull(s.handle, idx)
	} else {
		switch arg.Kind() {
		case TypeBoolean:
			state = api.BindBoolean(s.handle, idx, arg.b)
		case TypeTinyint:
			state = api.BindInt8(s.handle, idx, int8(arg.i64))
		case TypeSmallint:
			state = api.BindInt16(s.handle, idx, int16(arg.i64))
		case TypeInteger:
			state = api.BindInt32(s.handle, idx, int32(arg.i64))
		case TypeBigint:
			state = api.BindInt64(s.handle, idx, arg.i64)
		case TypeUTinyint:
			state = api.BindUint8(s.handle, idx, uint8(arg.u64))
		case TypeUSmallint:
			state = api.BindUint16(s.handle, idx, uint16(arg.u64))
		case TypeUInteger:
			state = api.BindUint32(s.handle, idx, uint32(arg.u64))
		case TypeUBigint:
			state = api.BindUint64(s.handle, idx, arg.u64)
		case TypeHugeint:
			state = api.BindHugeint(s.handle, idx, abi.Hugeint{Lower: arg.huge.Lower, Upper: arg.huge.Upper})
		case TypeUhugeint:
			state = api.BindUhugeint(s.handle, idx, abi.Uhugeint{Lower: arg.uhuge.Lower, Upper: arg.uhuge.Upper})
		case TypeDecimal:
			if err := arg.dec.Validate(); err != nil {
				return err
			}
			state = api.BindDecimal(s.handle, idx, abi.Decimal{
				Width: arg.dec.Width,
				Scale: arg.dec.Scale,
				Value: abi.Hugeint{Lower: arg.dec.Value.Lower, Upper: arg.dec.Value.Upper},
			})
		case TypeFloat:
			state = api.BindFloat(s.handle, idx, arg.f32)
		case TypeDouble:
			state = api.BindDouble(s.handle, idx, arg.f64)
		case TypeVarchar:
			if len(arg.str) == 0 {
				state = api.BindVarcharLength(s.handle, idx, &emptyBindByte, 0)
			} else {
				b := []byte(arg.str)
				state = api.BindVarcharLength(s.handle, idx, &b[0], uint64(len(b)))
				runtime.KeepAlive(b)
			}
		case TypeBlob:
			if len(arg.blob) == 0 {
				state = api.BindBlob(s.handle, idx, &emptyBindByte, 0)
			} else {
				state = api.BindBlob(s.handle, idx, &arg.blob[0], uint64(len(arg.blob)))
				runtime.KeepAlive(arg.blob)
			}
		case TypeDate:
			state = api.BindDate(s.handle, idx, abi.Date{Days: int32(arg.i64)})
		case TypeTime:
			state = api.BindTime(s.handle, idx, abi.Time{Micros: arg.i64})
		case TypeTimestamp:
			state = api.BindTimestamp(s.handle, idx, abi.Timestamp{Micros: arg.ts.Value})
		case TypeTimestampTZ:
			state = api.BindTimestampTZ(s.handle, idx, abi.Timestamp{Micros: arg.ts.Value})
		case TypeInterval:
			state = api.BindInterval(s.handle, idx, abi.Interval{
				Months: arg.iv.Months,
				Days:   arg.iv.Days,
				Micros: arg.iv.Micros,
			})
		case TypeTimestampS:
			v := api.CreateTimestampS(abi.TimestampS{Seconds: arg.ts.Value})
			if v == 0 {
				return nativeError("create_timestamp_s", abi.ErrorInvalid, "")
			}
			state = api.BindValue(s.handle, idx, v)
			api.DestroyValue(&v)
		case TypeTimestampMS:
			v := api.CreateTimestampMS(abi.TimestampMS{Millis: arg.ts.Value})
			if v == 0 {
				return nativeError("create_timestamp_ms", abi.ErrorInvalid, "")
			}
			state = api.BindValue(s.handle, idx, v)
			api.DestroyValue(&v)
		case TypeTimestampNS:
			v := api.CreateTimestampNS(abi.TimestampNS{Nanos: arg.ts.Value})
			if v == 0 {
				return nativeError("create_timestamp_ns", abi.ErrorInvalid, "")
			}
			state = api.BindValue(s.handle, idx, v)
			api.DestroyValue(&v)
		case TypeTimeNS:
			v := api.CreateTimeNS(abi.TimeNS{Nanos: arg.i64})
			if v == 0 {
				return nativeError("create_time_ns", abi.ErrorInvalid, "")
			}
			state = api.BindValue(s.handle, idx, v)
			api.DestroyValue(&v)
		default:
			return fmt.Errorf("%w: parameter %d has unsupported type %s", ErrUnsupportedType, idx, arg.Kind())
		}
	}
	if state != abi.StateSuccess {
		msg := abi.GoString(api.PrepareError(s.handle))
		return nativeError(fmt.Sprintf("bind[%d]", idx), abi.ErrorInvalid, msg)
	}
	return nil
}
