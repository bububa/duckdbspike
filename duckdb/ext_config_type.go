package duckdb

import (
	"fmt"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// ConfigOptionSpec defines a custom DuckDB runtime configuration option
// registered via Conn.RegisterConfigOption or Tx.RegisterConfigOption.
type ConfigOptionSpec struct {
	Name         string
	Type         LogicalType
	DefaultValue *Value
	DefaultScope ConfigOptionScope
	Description  string
}

// RegisterConfigOption registers a custom typed runtime configuration option on
// c. Once registered, the option can be modified via SQL SET and read via
// Conn.ConfigOption or ConfigOptionAs.
func (c *Conn) RegisterConfigOption(spec ConfigOptionSpec) error {
	return c.registerConfigOptionWithTx(nil, spec)
}

// RegisterConfigOption registers a custom typed runtime configuration option
// within tx.
func (tx *Tx) RegisterConfigOption(spec ConfigOptionSpec) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerConfigOptionWithTx(tx, spec)
}

func (c *Conn) registerConfigOptionWithTx(tx *Tx, spec ConfigOptionSpec) error {
	if c == nil {
		return ErrClosed
	}
	if spec.Name == "" {
		return fmt.Errorf("%w: config option name must not be empty", ErrInvalidInput)
	}
	if spec.Type.Kind == TypeInvalid && spec.DefaultValue == nil {
		return fmt.Errorf("%w: config option %q requires Type or DefaultValue", ErrInvalidInput, spec.Name)
	}
	if spec.Type.Kind == TypeAny {
		return fmt.Errorf("%w: config option %q cannot use ANY type", ErrInvalidInput, spec.Name)
	}
	if spec.DefaultValue != nil && spec.DefaultValue.IsDefault() {
		return fmt.Errorf("%w: config option %q cannot use DefaultValue sentinel", ErrInvalidInput, spec.Name)
	}
	switch spec.DefaultScope {
	case ConfigOptionScopeInvalid, ConfigOptionScopeLocal, ConfigOptionScopeSession, ConfigOptionScopeGlobal:
	default:
		return fmt.Errorf("%w: invalid config option scope %d", ErrInvalidInput, spec.DefaultScope)
	}

	cname, err := abi.CString(spec.Name)
	if err != nil {
		return fmt.Errorf("%w: config option name %q: %v", ErrInvalidInput, spec.Name, err)
	}
	var cdesc []byte
	if spec.Description != "" {
		cdesc, err = abi.CString(spec.Description)
		if err != nil {
			return fmt.Errorf("%w: config option description: %v", ErrInvalidInput, err)
		}
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
	opt := api.CreateConfigOption()
	if opt == 0 {
		return nativeError("create_config_option", abi.ErrorOutOfMemory, "")
	}
	defer api.DestroyConfigOption(&opt)

	api.ConfigOptionSetName(opt, &cname[0])
	runtime.KeepAlive(cname)

	if spec.Type.Kind != TypeInvalid {
		lt, err := createNativeLogicalType(api, spec.Type, 0, c.limits)
		if err != nil {
			return fmt.Errorf("config option %q type: %w", spec.Name, err)
		}
		api.ConfigOptionSetType(opt, lt)
		api.DestroyLogicalType(&lt)
	}

	if spec.DefaultValue != nil {
		dv, err := createNativeValue(api, *spec.DefaultValue, 0, c.limits)
		if err != nil {
			return fmt.Errorf("config option %q default value: %w", spec.Name, err)
		}
		api.ConfigOptionSetDefaultValue(opt, dv)
		api.DestroyValue(&dv)
	}

	if spec.DefaultScope != ConfigOptionScopeInvalid {
		api.ConfigOptionSetDefaultScope(opt, int32(spec.DefaultScope))
	}

	if len(cdesc) > 0 {
		api.ConfigOptionSetDescription(opt, &cdesc[0])
		runtime.KeepAlive(cdesc)
	}

	if api.RegisterConfigOption(c.handle, opt) != abi.StateSuccess {
		return nativeError("register_config_option", abi.ErrorInvalid, fmt.Sprintf("failed to register config option %q", spec.Name))
	}
	return nil
}

// RegisterLogicalType registers a named aliased logical type on c so SQL DDL
// and casts can reference it by name (for example `CREATE TABLE t (id my_id)`).
func (c *Conn) RegisterLogicalType(name string, lt LogicalType) error {
	return c.registerLogicalTypeWithTx(nil, name, lt)
}

// RegisterLogicalType registers a named aliased logical type within tx.
func (tx *Tx) RegisterLogicalType(name string, lt LogicalType) error {
	if tx == nil || tx.conn == nil {
		return ErrTxDone
	}
	return tx.conn.registerLogicalTypeWithTx(tx, name, lt)
}

func (c *Conn) registerLogicalTypeWithTx(tx *Tx, name string, lt LogicalType) error {
	if c == nil {
		return ErrClosed
	}
	if name == "" {
		return fmt.Errorf("%w: logical type name must not be empty", ErrInvalidInput)
	}
	if lt.Kind == TypeInvalid || lt.Kind == TypeAny {
		return fmt.Errorf("%w: logical type %q has unsupported base kind %s", ErrInvalidInput, name, lt.Kind)
	}
	if lt.Alias != "" && lt.Alias != name {
		return fmt.Errorf("%w: logical type alias %q conflicts with registered name %q", ErrInvalidInput, lt.Alias, name)
	}
	if _, err := abi.CString(name); err != nil {
		return fmt.Errorf("%w: logical type name %q: %v", ErrInvalidInput, name, err)
	}

	aliased := lt
	aliased.Alias = name

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return err
	}
	if err := c.usableLocked(); err != nil {
		return err
	}

	api := c.api
	h, err := createNativeLogicalType(api, aliased, 0, c.limits)
	if err != nil {
		return fmt.Errorf("register logical type %q: %w", name, err)
	}
	defer api.DestroyLogicalType(&h)

	if api.RegisterLogicalType(c.handle, h, 0) != abi.StateSuccess {
		return nativeError("register_logical_type", abi.ErrorInvalid, fmt.Sprintf("failed to register logical type %q", name))
	}
	return nil
}
