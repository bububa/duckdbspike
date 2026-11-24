package duckdbsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/bububa/duckdbspike/duckdb"
)

// Composite is a generic sql.Scanner that decodes DuckDB STRUCT columns into
// Go structs (matching `db:"name"` struct tags or case-insensitive exported
// field names), recursively handling nested structs, slices, pointers, and
// sql.Scanner fields.
type Composite[T any] struct {
	val   T
	valid bool
}

// Get returns the decoded Go value (or the zero value of T when SQL NULL).
func (c Composite[T]) Get() T { return c.val }

// Valid reports whether the scanned column was non-NULL.
func (c Composite[T]) Valid() bool { return c.valid }

// Scan implements database/sql.Scanner.
func (c *Composite[T]) Scan(src any) error {
	var zero T
	if src == nil {
		c.val = zero
		c.valid = false
		return nil
	}
	var st duckdb.StructValue
	switch x := src.(type) {
	case duckdb.StructValue:
		st = x
	case *duckdb.StructValue:
		if x == nil {
			c.val = zero
			c.valid = false
			return nil
		}
		st = *x
	case duckdb.Value:
		if x.IsNull() {
			c.val = zero
			c.valid = false
			return nil
		}
		var err error
		st, err = x.Struct()
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: Composite.Scan expected duckdb.StructValue, got %T", duckdb.ErrTypeMismatch, src)
	}

	var out T
	rv := reflect.ValueOf(&out).Elem()
	if err := decodeStructInto(st, rv, 0); err != nil {
		return err
	}
	c.val = out
	c.valid = true
	return nil
}

func decodeStructInto(st duckdb.StructValue, dst reflect.Value, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%w: Composite struct nesting exceeds depth limit", duckdb.ErrLimitExceeded)
	}
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	if dst.Kind() != reflect.Struct {
		return fmt.Errorf("%w: Composite target must be a struct, got %s", duckdb.ErrTypeMismatch, dst.Type())
	}

	rt := dst.Type()
	byTag := make(map[string]int, rt.NumField())
	byExact := make(map[string]int, rt.NumField())
	byLower := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		if tag := f.Tag.Get("db"); tag != "" && tag != "-" {
			name, _, _ := strings.Cut(tag, ",")
			if name != "" {
				byTag[name] = i
			}
		}
		byExact[f.Name] = i
		byLower[strings.ToLower(f.Name)] = i
	}

	for _, sf := range st.Fields {
		idx, ok := byTag[sf.Name]
		if !ok {
			idx, ok = byExact[sf.Name]
		}
		if !ok {
			idx, ok = byLower[strings.ToLower(sf.Name)]
		}
		if !ok {
			continue
		}
		if err := assignDuckDBValue(sf.Value, dst.Field(idx), depth+1); err != nil {
			return fmt.Errorf("field %q: %w", sf.Name, err)
		}
	}
	return nil
}

func assignDuckDBValue(v duckdb.Value, dst reflect.Value, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%w: Composite nesting exceeds depth limit", duckdb.ErrLimitExceeded)
	}
	if v.IsNull() {
		// Delegate NULL to sql.Scanner implementations (such as sql.Null* or Composite).
		if dst.CanAddr() {
			if sc, ok := dst.Addr().Interface().(sql.Scanner); ok {
				return sc.Scan(nil)
			}
		}
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	// Unwrap pointers for non-null values.
	for dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		// Check if the pointer itself implements sql.Scanner before dereferencing.
		if sc, ok := dst.Interface().(sql.Scanner); ok {
			dv, err := fromDuckDBValue(v, false)
			if err != nil {
				return err
			}
			return sc.Scan(dv)
		}
		dst = dst.Elem()
	}

	// Check sql.Scanner on addressable value (e.g., duckdb.Decimal, duckdb.UUID, sql.NullString, Composite[U]).
	if dst.CanAddr() {
		if sc, ok := dst.Addr().Interface().(sql.Scanner); ok {
			dv, err := fromDuckDBValue(v, false)
			if err != nil {
				return err
			}
			return sc.Scan(dv)
		}
	}

	// Nested STRUCT -> Go struct.
	if v.Kind() == duckdb.TypeStruct && dst.Kind() == reflect.Struct && dst.Type() != reflect.TypeFor[duckdb.StructValue]() {
		st, err := v.Struct()
		if err != nil {
			return err
		}
		return decodeStructInto(st, dst, depth+1)
	}

	// LIST / ARRAY -> Go slice.
	if (v.Kind() == duckdb.TypeList || v.Kind() == duckdb.TypeArray) && dst.Kind() == reflect.Slice && dst.Type() != reflect.TypeFor[[]byte]() {
		var elems []duckdb.Value
		if v.Kind() == duckdb.TypeList {
			lv, err := v.List()
			if err != nil {
				return err
			}
			elems = lv.Elements
		} else {
			av, err := v.Array()
			if err != nil {
				return err
			}
			elems = av.Elements
		}
		slice := reflect.MakeSlice(dst.Type(), len(elems), len(elems))
		for i, elem := range elems {
			if err := assignDuckDBValue(elem, slice.Index(i), depth+1); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		dst.Set(slice)
		return nil
	}

	// Direct duckdb.Value target.
	if dst.Type() == reflect.TypeFor[duckdb.Value]() {
		dst.Set(reflect.ValueOf(v))
		return nil
	}

	dv, err := fromDuckDBValue(v, false)
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(dv)
	if rv.Type().AssignableTo(dst.Type()) {
		dst.Set(rv)
		return nil
	}
	// Exact integer widening / narrowing with overflow check.
	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := v.AsInt64()
		if err != nil {
			return err
		}
		if dst.OverflowInt(n) {
			return fmt.Errorf("%w: integer %d overflows %s", duckdb.ErrOutOfRange, n, dst.Type())
		}
		dst.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := v.AsUint64()
		if err != nil {
			return err
		}
		if dst.OverflowUint(u) {
			return fmt.Errorf("%w: integer %d overflows %s", duckdb.ErrOutOfRange, u, dst.Type())
		}
		dst.SetUint(u)
		return nil
	case reflect.Float64:
		if v.Kind() == duckdb.TypeFloat {
			f, err := v.Float32()
			if err != nil {
				return err
			}
			dst.SetFloat(float64(f))
			return nil
		}
	}
	return fmt.Errorf("%w: cannot assign %s (%T) to %s", duckdb.ErrTypeMismatch, v.Kind(), dv, dst.Type())
}
