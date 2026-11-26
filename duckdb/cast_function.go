package duckdb

import (
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/bububa/duckdbspike/internal/abi"
)

// CastMode indicates whether DuckDB is executing a normal CAST or a TRY_CAST.
type CastMode int32

const (
	CastModeNormal CastMode = CastMode(abi.CastModeNormal)
	CastModeTry    CastMode = CastMode(abi.CastModeTry)
)

// String returns a human-readable name for the cast mode.
func (m CastMode) String() string {
	switch m {
	case CastModeNormal:
		return "NORMAL"
	case CastModeTry:
		return "TRY"
	default:
		return fmt.Sprintf("CAST_MODE(%d)", int32(m))
	}
}

// CastFunctionSpec defines a custom DuckDB cast from SourceType to TargetType
// registered via Conn.RegisterCastFunction or Tx.RegisterCastFunction.
//
// SQL NULL inputs are automatically propagated to SQL NULL outputs without
// invoking Cast. When Cast returns an error:
//   - In CastModeNormal (CAST), the query fails with that error.
//   - In CastModeTry (TRY_CAST), only the failing row is set to NULL and
//     remaining rows in the batch continue casting.
type CastFunctionSpec struct {
	SourceType       LogicalType
	TargetType       LogicalType
	ImplicitCastCost *int64
	Cast             func(mode CastMode, val Value) (Value, error)
}

type castFuncDef struct {
	api        *abi.API
	limits     DecodeLimits
	spec       CastFunctionSpec
	sourcePlan columnPlan
	targetPlan columnPlan
}

var (
	castFuncDispatchAPI atomic.Pointer[abi.API]
	globalCFDispatcher  = castFuncDispatcher{}
)

type castFuncDispatcher struct{}

func callCastSafe(fn func(CastMode, Value) (Value, error), mode CastMode, in Value) (out Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cast function panic: %v", r)
		}
	}()
	return fn(mode, in)
}

func (castFuncDispatcher) Execute(info uintptr, count uint64, input, output uintptr) bool {
	bootAPI := castFuncDispatchAPI.Load()
	if bootAPI == nil || bootAPI.CastFunctionGetExtraInfo == nil {
		return false
	}
	extraID := bootAPI.CastFunctionGetExtraInfo(info)
	raw, ok := abi.LookupHookHandle(extraID)
	if !ok {
		if bootAPI.CastFunctionSetError != nil {
			cmsg := cErrorString("cast function definition handle not found")
			bootAPI.CastFunctionSetError(info, &cmsg[0])
			runtime.KeepAlive(cmsg)
		}
		return false
	}
	def, ok := raw.(*castFuncDef)
	if !ok || def == nil {
		if bootAPI.CastFunctionSetError != nil {
			cmsg := cErrorString("invalid cast function definition handle")
			bootAPI.CastFunctionSetError(info, &cmsg[0])
			runtime.KeepAlive(cmsg)
		}
		return false
	}
	api := def.api
	mode := CastMode(api.CastFunctionGetCastMode(info))
	allOK := true

	handleErr := func(row uint64, msg string) bool {
		cmsg := cErrorString(msg)
		allOK = false
		if mode == CastModeTry {
			api.CastFunctionSetRowError(info, &cmsg[0], row, output)
			runtime.KeepAlive(cmsg)
			return true
		}
		api.CastFunctionSetError(info, &cmsg[0])
		runtime.KeepAlive(cmsg)
		return false
	}

	var undo []listSizeUndo
	for row := range count {
		inVal, err := decodeVectorValue(api, &def.sourcePlan, input, row, def.limits)
		if err != nil {
			if !handleErr(row, fmt.Sprintf("decode cast input row %d: %v", row, err)) {
				return false
			}
			continue
		}
		if inVal.IsNull() {
			undo = undo[:0]
			if err := writeVectorValue(api, &def.targetPlan, output, row, NullValue(), 0, def.limits, &undo); err != nil {
				if !handleErr(row, fmt.Sprintf("write cast NULL row %d: %v", row, err)) {
					return false
				}
			}
			continue
		}

		outVal, err := callCastSafe(def.spec.Cast, mode, inVal)
		if err != nil {
			if !handleErr(row, err.Error()) {
				return false
			}
			continue
		}

		undo = undo[:0]
		if err := writeVectorValue(api, &def.targetPlan, output, row, outVal, 0, def.limits, &undo); err != nil {
			for _, u := range slices.Backward(undo) {
				_ = api.ListVectorSetSize(u.vec, u.prevSize)
			}
			if !handleErr(row, fmt.Sprintf("write cast output row %d: %v", row, err)) {
				return false
			}
			continue
		}
	}

	return allOK
}

// RegisterCastFunction registers a custom cast from spec.SourceType to
// spec.TargetType on c.
func (c *Conn) RegisterCastFunction(spec CastFunctionSpec) error {
	return c.registerCastFunctionWithTx(nil, spec)
}

// RegisterCastFunction registers a custom cast within tx.
func (tx *Tx) RegisterCastFunction(spec CastFunctionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerCastFunctionWithTx(tx, spec)
}

func (c *Conn) registerCastFunctionWithTx(tx *Tx, spec CastFunctionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if spec.SourceType.Kind == TypeInvalid || spec.SourceType.Kind == TypeAny {
		return fmt.Errorf("%w: cast function requires a concrete SourceType", ErrInvalidInput)
	}
	if spec.TargetType.Kind == TypeInvalid || spec.TargetType.Kind == TypeAny {
		return fmt.Errorf("%w: cast function requires a concrete TargetType", ErrInvalidInput)
	}
	if spec.Cast == nil {
		return fmt.Errorf("%w: cast function requires Cast callback", ErrInvalidInput)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return err
	}
	if err := c.usableLocked(); err != nil {
		return err
	}

	api := c.api
	srcLT, err := createNativeLogicalType(api, spec.SourceType, 0, c.limits)
	if err != nil {
		return fmt.Errorf("cast source type: %w", err)
	}
	defer api.DestroyLogicalType(&srcLT)

	srcPlan, err := buildColumnPlan(api, srcLT, 0, c.limits)
	if err != nil {
		return fmt.Errorf("cast source type: %w", err)
	}

	dstLT, err := createNativeLogicalType(api, spec.TargetType, 0, c.limits)
	if err != nil {
		return fmt.Errorf("cast target type: %w", err)
	}
	defer api.DestroyLogicalType(&dstLT)

	dstPlan, err := buildColumnPlan(api, dstLT, 0, c.limits)
	if err != nil {
		return fmt.Errorf("cast target type: %w", err)
	}

	cf := api.CreateCastFunction()
	if cf == 0 {
		return nativeError("create_cast_function", abi.ErrorOutOfMemory, "")
	}

	api.CastFunctionSetSourceType(cf, srcLT)
	api.CastFunctionSetTargetType(cf, dstLT)
	if spec.ImplicitCastCost != nil {
		api.CastFunctionSetImplicitCastCost(cf, *spec.ImplicitCastCost)
	}

	def := &castFuncDef{
		api:        api,
		limits:     c.limits,
		spec:       spec,
		sourcePlan: srcPlan,
		targetPlan: dstPlan,
	}

	castFuncDispatchAPI.Store(api)
	abi.SetCastFunctionDispatcher(globalCFDispatcher)

	extraID := abi.RegisterHookHandle(def)
	api.CastFunctionSetExtraInfo(cf, extraID, abi.HookDeleteCallback())
	api.CastFunctionSetFunction(cf, abi.CastFunctionExecCallback())

	state := api.RegisterCastFunction(c.handle, cf)
	api.DestroyCastFunction(&cf)
	if state != abi.StateSuccess {
		abi.ReleaseHookHandle(extraID)
		return nativeError("register_cast_function", abi.ErrorInvalid, "failed to register cast function")
	}
	return nil
}
