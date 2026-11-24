package duckdbsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/bububa/duckdbspike/duckdb"
)

// Composite is a generic sql.Scanner and driver.Valuer that decodes DuckDB
// STRUCT columns into Go structs and encodes Go structs into DuckDB STRUCT
// parameters (matching `db:"name"` struct tags or exported field names),
// recursively handling nested structs, slices, pointers, and sql.Scanner /
// driver.Valuer fields.
type Composite[T any] struct {
	val   T
	valid bool
}

// NewComposite wraps v as a non-NULL Composite[T] suitable for binding as a
// DuckDB STRUCT SQL parameter.
func NewComposite[T any](v T) Composite[T] {
	return Composite[T]{val: v, valid: true}
}

// NullComposite returns a SQL NULL Composite[T].
func NullComposite[T any]() Composite[T] {
	return Composite[T]{}
}

// Get returns the decoded Go value (or the zero value of T when SQL NULL).
func (c Composite[T]) Get() T { return c.val }

// Valid reports whether the scanned column was non-NULL.
func (c Composite[T]) Valid() bool { return c.valid }

// Value implements database/sql/driver.Valuer, encoding a valid Composite[T]
// into a duckdb.StructValue (or nil when SQL NULL).
func (c Composite[T]) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return encodeStructFromGo(reflect.ValueOf(c.val), 0)
}

func encodeStructFromGo(rv reflect.Value, depth int) (duckdb.StructValue, error) {
	if depth > 64 {
		return duckdb.StructValue{}, fmt.Errorf("%w: Composite struct nesting exceeds depth limit", duckdb.ErrLimitExceeded)
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return duckdb.StructValue{}, fmt.Errorf("%w: nil pointer struct", duckdb.ErrInvalidInput)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return duckdb.StructValue{}, fmt.Errorf("%w: Composite.Value target must be a struct, got %s", duckdb.ErrTypeMismatch, rv.Type())
	}
	rt := rv.Type()
	fields := make([]duckdb.StructFieldValue, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name := sf.Name
		if tag != "" {
			if n, _, _ := strings.Cut(tag, ","); n != "" {
				name = n
			}
		}
		fv := rv.Field(i)
		dv, lt, err := encodeStructField(fv, depth+1)
		if err != nil {
			return duckdb.StructValue{}, fmt.Errorf("field %q: %w", name, err)
		}
		fields = append(fields, duckdb.StructFieldValue{
			Name:  name,
			Type:  lt,
			Value: dv,
		})
	}
	if len(fields) == 0 {
		return duckdb.StructValue{}, fmt.Errorf("%w: struct %s has no exported fields", duckdb.ErrInvalidInput, rt)
	}
	return duckdb.StructValue{Fields: fields}, nil
}

func encodeStructField(fv reflect.Value, depth int) (duckdb.Value, duckdb.LogicalType, error) {
	if depth > 64 {
		return duckdb.Value{}, duckdb.LogicalType{}, fmt.Errorf("%w: Composite nesting exceeds depth limit", duckdb.ErrLimitExceeded)
	}
	v, err := toDuckDBValueDepth(fv.Interface(), depth)
	if err != nil {
		// If fv is a nested Go struct (not a known duckdb scalar/driver.Valuer),
		// encode it recursively as a DuckDB STRUCT.
		t := fv.Type()
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct && !isDirectScalarStructType(t) {
			if fv.Kind() == reflect.Ptr && fv.IsNil() {
				lt, ltErr := inferLogicalTypeFromGoType(t, depth)
				if ltErr != nil {
					return duckdb.Value{}, duckdb.LogicalType{}, ltErr
				}
				return duckdb.TypedNullValue(lt), lt, nil
			}
			st, stErr := encodeStructFromGo(fv, depth)
			if stErr != nil {
				return duckdb.Value{}, duckdb.LogicalType{}, stErr
			}
			sv, svErr := duckdb.NewStructValue(st.Fields)
			if svErr != nil {
				return duckdb.Value{}, duckdb.LogicalType{}, svErr
			}
			return sv, sv.Type(), nil
		}
		return duckdb.Value{}, duckdb.LogicalType{}, err
	}
	if v.IsNull() {
		lt, ltErr := inferLogicalTypeFromGoType(fv.Type(), depth)
		if ltErr != nil {
			return duckdb.TypedNullValue(duckdb.ScalarType(duckdb.TypeSQLNull)), duckdb.ScalarType(duckdb.TypeSQLNull), nil
		}
		return duckdb.TypedNullValue(lt), lt, nil
	}
	return v, v.Type(), nil
}

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

	// ARRAY / LIST -> Go fixed-size array [N]T (excluding duckdb.UUID handled by sql.Scanner above).
	if (v.Kind() == duckdb.TypeArray || v.Kind() == duckdb.TypeList) && dst.Kind() == reflect.Array && dst.Type() != reflect.TypeFor[duckdb.UUID]() {
		var elems []duckdb.Value
		if v.Kind() == duckdb.TypeArray {
			av, err := v.Array()
			if err != nil {
				return err
			}
			elems = av.Elements
		} else {
			lv, err := v.List()
			if err != nil {
				return err
			}
			elems = lv.Elements
		}
		if len(elems) != dst.Len() {
			return fmt.Errorf("%w: cannot assign %d elements into %s", duckdb.ErrTypeMismatch, len(elems), dst.Type())
		}
		for i, elem := range elems {
			if err := assignDuckDBValue(elem, dst.Index(i), depth+1); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return nil
	}

	// MAP -> Go map[K]V.
	if v.Kind() == duckdb.TypeMap && dst.Kind() == reflect.Map {
		mv, err := v.Map()
		if err != nil {
			return err
		}
		m := reflect.MakeMapWithSize(dst.Type(), len(mv.Entries))
		keyType := dst.Type().Key()
		valType := dst.Type().Elem()
		for i, entry := range mv.Entries {
			kp := reflect.New(keyType)
			if err := assignDuckDBValue(entry.Key, kp.Elem(), depth+1); err != nil {
				return fmt.Errorf("map key[%d]: %w", i, err)
			}
			vp := reflect.New(valType)
			if err := assignDuckDBValue(entry.Value, vp.Elem(), depth+1); err != nil {
				return fmt.Errorf("map value[%d]: %w", i, err)
			}
			m.SetMapIndex(kp.Elem(), vp.Elem())
		}
		dst.Set(m)
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
