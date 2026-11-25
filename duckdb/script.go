package duckdb

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

type scriptArgs struct {
	posArgs   []Value
	posOffset int
	namedArgs []NamedArg
	namedByK  map[string]NamedArg
	namedUsed map[string]struct{}
	isNamed   bool
}

func newScriptArgsPos(args []Value) *scriptArgs {
	if len(args) == 0 {
		return &scriptArgs{}
	}
	cp := make([]Value, len(args))
	copy(cp, args)
	return &scriptArgs{posArgs: cp}
}

func newScriptArgsNamed(args []NamedArg) (*scriptArgs, error) {
	if len(args) == 0 {
		return &scriptArgs{isNamed: true}, nil
	}
	byName := make(map[string]NamedArg, len(args))
	cp := make([]NamedArg, len(args))
	for i, a := range args {
		if a.Name == "" {
			return nil, fmt.Errorf("%w: empty parameter name", ErrInvalidInput)
		}
		if _, dup := byName[a.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate parameter name %q", ErrInvalidInput, a.Name)
		}
		byName[a.Name] = a
		cp[i] = a
	}
	return &scriptArgs{
		namedArgs: cp,
		namedByK:  byName,
		namedUsed: make(map[string]struct{}, len(args)),
		isNamed:   true,
	}, nil
}

func (a *scriptArgs) hasAny() bool {
	if a == nil {
		return false
	}
	return len(a.posArgs) > 0 || len(a.namedArgs) > 0
}

// bindStmtLocked binds the appropriate slice/subset of parameters for stmt s.
// When isLast is true, it also verifies that all supplied positional or named
// parameters were consumed across the script.
func (a *scriptArgs) bindStmtLocked(s *Stmt, isLast bool) error {
	api := s.conn.api
	need := api.NParams(s.handle)
	if a == nil {
		if need > 0 {
			return fmt.Errorf("%w: statement expects %d parameters, got 0", ErrInvalidInput, need)
		}
		return s.bindLocked(nil)
	}
	if !a.isNamed {
		rem := len(a.posArgs) - a.posOffset
		if need > uint64(rem) {
			return fmt.Errorf("%w: statement expects %d parameters, only %d remaining", ErrInvalidInput, need, rem)
		}
		slice := a.posArgs[a.posOffset : a.posOffset+int(need)]
		if err := s.bindLocked(slice); err != nil {
			return err
		}
		a.posOffset += int(need)
		if isLast && a.posOffset != len(a.posArgs) {
			return fmt.Errorf("%w: script received %d parameters, consumed %d", ErrInvalidInput, len(a.posArgs), a.posOffset)
		}
		return nil
	}

	// Named parameters.
	if need == 0 {
		if err := s.bindLocked(nil); err != nil {
			return err
		}
		if isLast {
			return a.checkAllNamedUsed()
		}
		return nil
	}
	// If ParameterName is unavailable (legacy mock API), fall back to binding all
	// named arguments directly to this statement.
	if api.ParameterName == nil {
		if err := s.bindNamedLocked(a.namedArgs); err != nil {
			return err
		}
		for _, na := range a.namedArgs {
			a.namedUsed[na.Name] = struct{}{}
		}
		return nil
	}
	subset := make([]NamedArg, 0, int(need))
	seen := make(map[string]struct{}, int(need))
	for i := range need {
		ptr := api.ParameterName(s.handle, i+1)
		name := abi.GoString(ptr)
		if ptr != 0 {
			api.Free(ptr)
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		na, ok := a.namedByK[name]
		if !ok {
			return fmt.Errorf("%w: missing named parameter %q", ErrInvalidInput, name)
		}
		subset = append(subset, na)
	}
	if err := s.bindNamedLocked(subset); err != nil {
		return err
	}
	for _, na := range subset {
		a.namedUsed[na.Name] = struct{}{}
	}
	if isLast {
		return a.checkAllNamedUsed()
	}
	return nil
}

func (a *scriptArgs) checkAllNamedUsed() error {
	if a == nil || !a.isNamed {
		return nil
	}
	for _, na := range a.namedArgs {
		if _, ok := a.namedUsed[na.Name]; !ok {
			return fmt.Errorf("%w: unknown parameter %q", ErrInvalidInput, na.Name)
		}
	}
	return nil
}

type scriptCursor struct {
	extracted uintptr
	nextIdx   uint64
	total     uint64
	args      *scriptArgs
}

func (sc *scriptCursor) destroyLocked(api *abi.API) {
	if sc == nil {
		return
	}
	if sc.extracted != 0 {
		api.DestroyExtracted(&sc.extracted)
	}
}

// isRowReturningStmtLocked classifies whether a prepared statement returns a
// streaming result rowset (SELECT, EXPLAIN, PRAGMA, CALL, RELATION,
// LOGICAL_PLAN) vs a non-row/materialized DDL/DML statement.
func isRowReturningStmtLocked(api *abi.API, stmtHandle uintptr) bool {
	if api.PreparedStatementType == nil {
		return false
	}
	switch api.PreparedStatementType(stmtHandle) {
	case abi.StatementTypeSelect,
		abi.StatementTypeExplain,
		abi.StatementTypePragma,
		abi.StatementTypeCall,
		abi.StatementTypeRelation,
		abi.StatementTypeLogicalPlan:
		return true
	default:
		return false
	}
}

// ExecScript extracts and executes all statements in sql sequentially under ctx
// cancellation. Positional args are distributed across statements in declaration
// order (if statements 0..N-2 have no parameters, all args bind to statement
// N-1). RowsAffected reports the total rows changed across statements in the
// script. Empty or comment-only scripts return zero ExecResult and nil (unless
// args are supplied, which returns ErrInvalidInput).
func (c *Conn) ExecScript(ctx context.Context, sql string, args ...Value) (ExecResult, error) {
	return c.execScriptWithArgs(ctx, nil, sql, newScriptArgsPos(args))
}

// ExecScriptNamed extracts and executes all statements in sql sequentially
// under ctx cancellation, routing named parameters ($name) to the statements
// that reference them.
func (c *Conn) ExecScriptNamed(ctx context.Context, sql string, args ...NamedArg) (ExecResult, error) {
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return ExecResult{}, err
	}
	return c.execScriptWithArgs(ctx, nil, sql, sa)
}

func (c *Conn) execScriptWithArgs(ctx context.Context, tx *Tx, sql string, args *scriptArgs) (ExecResult, error) {
	if c == nil {
		return ExecResult{}, ErrClosed
	}
	if ctx == nil {
		return ExecResult{}, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return ExecResult{}, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return ExecResult{}, err
	}
	if err := c.usableLocked(); err != nil {
		return ExecResult{}, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return ExecResult{}, err
	}
	defer c.endScopeLocked(scope)

	api := c.api
	var extracted uintptr
	count := api.ExtractStatements(c.handle, &csql[0], &extracted)
	runtime.KeepAlive(csql)
	defer func() {
		if extracted != 0 {
			api.DestroyExtracted(&extracted)
		}
	}()
	if count == 0 {
		msg := abi.GoString(api.ExtractStatementsError(extracted))
		if cerr := scope.err(); cerr != nil {
			return ExecResult{}, cerr
		}
		if msg != "" {
			return ExecResult{}, nativeError("extract_statements", abi.ErrorInvalid, msg)
		}
		if args.hasAny() {
			return ExecResult{}, fmt.Errorf("%w: script contains no statements to bind parameters to", ErrInvalidInput)
		}
		if !scope.tryFinish() {
			return ExecResult{}, scope.err()
		}
		return ExecResult{}, nil
	}

	var (
		totalAffected  uint64
		lastStmtType   StatementType
		lastReturnType ResultType
	)
	for i := range count {
		if cerr := scope.err(); cerr != nil {
			return ExecResult{}, cerr
		}
		s, err := c.prepareExtractedLocked(extracted, i, scope)
		if err != nil {
			return ExecResult{}, err
		}
		isLast := i == count-1
		binder := func() error { return args.bindStmtLocked(s, isLast) }
		res, execErr := s.execNoFinishLocked(binder, scope)
		s.releaseLocked()
		if execErr != nil {
			return ExecResult{}, execErr
		}
		totalAffected += res.RowsAffected
		lastStmtType = res.StatementType
		lastReturnType = res.ReturnType
	}
	if !scope.tryFinish() {
		return ExecResult{}, scope.err()
	}
	return ExecResult{
		RowsAffected:  totalAffected,
		StatementType: lastStmtType,
		ReturnType:    lastReturnType,
	}, nil
}

// QueryScript extracts all statements in sql, auto-executes leading non-row
// statements (DDL/SET/plain DML), and streams the first row-returning statement
// (or the final statement if none return rows). Additional row-returning
// statements in the script can be consumed via Rows.NextResultSet. Positional
// args are distributed across statements in declaration order.
func (c *Conn) QueryScript(ctx context.Context, sql string, args ...Value) (*Rows, error) {
	return c.queryScriptWithArgs(ctx, nil, sql, newScriptArgsPos(args))
}

// QueryScriptNamed extracts all statements in sql, auto-executes leading
// non-row statements, and streams row-returning statements with named
// parameters ($name). Additional result sets can be consumed via
// Rows.NextResultSet.
func (c *Conn) QueryScriptNamed(ctx context.Context, sql string, args ...NamedArg) (*Rows, error) {
	sa, err := newScriptArgsNamed(args)
	if err != nil {
		return nil, err
	}
	return c.queryScriptWithArgs(ctx, nil, sql, sa)
}

func (c *Conn) queryScriptWithArgs(ctx context.Context, tx *Tx, sql string, args *scriptArgs) (*Rows, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	csql, err := abi.CString(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: sql: %v", ErrInvalidInput, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	scope, err := c.beginScopeLocked(ctx)
	if err != nil {
		return nil, err
	}

	api := c.api
	var extracted uintptr
	count := api.ExtractStatements(c.handle, &csql[0], &extracted)
	runtime.KeepAlive(csql)
	if count == 0 {
		msg := abi.GoString(api.ExtractStatementsError(extracted))
		if extracted != 0 {
			api.DestroyExtracted(&extracted)
		}
		cerr := scope.err()
		c.endScopeLocked(scope)
		if cerr != nil {
			return nil, cerr
		}
		if msg != "" {
			return nil, nativeError("extract_statements", abi.ErrorInvalid, msg)
		}
		return nil, fmt.Errorf("%w: script contains no statements", ErrInvalidInput)
	}

	var preAffected uint64
	for i := range count {
		if cerr := scope.err(); cerr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, cerr
		}
		s, err := c.prepareExtractedLocked(extracted, i, scope)
		if err != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, err
		}
		isLast := i == count-1
		if isLast || isRowReturningStmtLocked(api, s.handle) {
			rows, err := s.startLocked(func() error { return args.bindStmtLocked(s, isLast) }, scope)
			if err != nil {
				s.releaseLocked()
				api.DestroyExtracted(&extracted)
				c.endScopeLocked(scope)
				return nil, err
			}
			rows.ownsStmt = true
			rows.rowsAffected += preAffected
			if isLast {
				api.DestroyExtracted(&extracted)
			} else {
				rows.script = &scriptCursor{
					extracted: extracted,
					nextIdx:   i + 1,
					total:     count,
					args:      args,
				}
			}
			return rows, nil
		}
		res, execErr := s.execNoFinishLocked(func() error { return args.bindStmtLocked(s, false) }, scope)
		s.releaseLocked()
		if execErr != nil {
			api.DestroyExtracted(&extracted)
			c.endScopeLocked(scope)
			return nil, execErr
		}
		preAffected += res.RowsAffected
	}

	// Unreachable because count > 0 and isLast always enters the return branch.
	api.DestroyExtracted(&extracted)
	c.endScopeLocked(scope)
	return nil, fmt.Errorf("%w: script contains no statements", ErrInvalidInput)
}

func (r *Rows) advanceScriptResultLocked() bool {
	c := r.conn
	api := c.api
	sc := r.script

	// Release current result set resources while retaining Conn lease and scope.
	r.destroyCurrentResultLocked()

	for sc.nextIdx < sc.total {
		if cerr := r.scope.err(); cerr != nil {
			sc.destroyLocked(api)
			r.err = cerr
			return false
		}
		idx := sc.nextIdx
		isLast := idx == sc.total-1
		s, err := c.prepareExtractedLocked(sc.extracted, idx, r.scope)
		if err != nil {
			sc.destroyLocked(api)
			r.err = err
			return false
		}
		if isRowReturningStmtLocked(api, s.handle) {
			result, err := s.runPendingLocked(func() error { return sc.args.bindStmtLocked(s, isLast) }, r.scope)
			if err != nil {
				s.releaseLocked()
				sc.destroyLocked(api)
				r.err = err
				return false
			}
			if cerr := r.scope.err(); cerr != nil {
				api.DestroyResult(&result)
				s.releaseLocked()
				sc.destroyLocked(api)
				r.err = cerr
				return false
			}
			if !api.ResultIsStreaming(result) {
				api.DestroyResult(&result)
				s.releaseLocked()
				sc.destroyLocked(api)
				r.err = ErrNotStreaming
				return false
			}
			n := api.ColumnCount(&result)
			if n > math.MaxInt {
				api.DestroyResult(&result)
				s.releaseLocked()
				sc.destroyLocked(api)
				r.err = fmt.Errorf("%w: column count %d overflows int", ErrInvalidInput, n)
				return false
			}
			columns := make([]Column, int(n))
			plans := make([]columnPlan, int(n))
			for j := range n {
				name := abi.GoString(api.ColumnName(&result, j))
				lt := api.ColumnLogicalType(&result, j)
				plan, planErr := buildColumnPlan(api, lt, 0, r.limits)
				api.DestroyLogicalType(&lt)
				if planErr != nil {
					api.DestroyResult(&result)
					s.releaseLocked()
					sc.destroyLocked(api)
					r.err = fmt.Errorf("column %d (%q): %w", j, name, planErr)
					return false
				}
				columns[j] = Column{Name: name, Type: plan.lt}
				plans[j] = plan
			}
			if cerr := r.scope.err(); cerr != nil {
				api.DestroyResult(&result)
				s.releaseLocked()
				sc.destroyLocked(api)
				r.err = cerr
				return false
			}
			r.stmt = s
			s.rows = r
			r.ownsStmt = true
			r.result = result
			r.columns = columns
			r.plans = plans
			if api.ResultStatementType != nil {
				r.stmtType = StatementType(api.ResultStatementType(result))
			} else if api.PreparedStatementType != nil {
				r.stmtType = StatementType(api.PreparedStatementType(s.handle))
			} else {
				r.stmtType = StatementTypeInvalid
			}
			if api.ResultReturnType != nil {
				r.returnType = ResultType(api.ResultReturnType(result))
			} else {
				r.returnType = ResultTypeInvalid
			}
			r.chunk = 0
			r.chunkSize = 0
			r.rowIdx = 0
			r.hasRow = false
			r.done = false
			r.rowsAffected += api.RowsChanged(&result)
			sc.nextIdx = idx + 1
			if sc.nextIdx >= sc.total {
				sc.destroyLocked(api)
			}
			return true
		}

		// Intervening or trailing non-row-returning statement (e.g., DML/DDL cleanup).
		res, execErr := s.execNoFinishLocked(func() error { return sc.args.bindStmtLocked(s, isLast) }, r.scope)
		s.releaseLocked()
		if execErr != nil {
			sc.destroyLocked(api)
			r.err = execErr
			return false
		}
		r.rowsAffected += res.RowsAffected
		sc.nextIdx = idx + 1
	}

	sc.destroyLocked(api)
	if !r.scope.tryFinish() {
		r.err = r.scope.err()
		return false
	}
	r.done = true
	return false
}

func (c *Conn) prepareExtractedLocked(extracted uintptr, index uint64, scope *cancelScope) (*Stmt, error) {
	if cerr := scope.err(); cerr != nil {
		return nil, cerr
	}
	var handle uintptr
	state := c.api.PrepareExtractedStatement(c.handle, extracted, index, &handle)
	if state != abi.StateSuccess {
		message := abi.GoString(c.api.PrepareError(handle))
		if handle != 0 {
			c.api.DestroyPrepare(&handle)
		}
		if cerr := scope.err(); cerr != nil {
			return nil, cerr
		}
		return nil, nativeError(fmt.Sprintf("prepare_extracted[%d]", index), abi.ErrorInvalid, message)
	}
	if cerr := scope.err(); cerr != nil {
		c.api.DestroyPrepare(&handle)
		return nil, cerr
	}
	return &Stmt{conn: c, handle: handle}, nil
}
