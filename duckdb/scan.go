package duckdb

import (
	"database/sql/driver"
	"fmt"
)

func scanExact[T any](dst *T, src any, name string) error {
	if src == nil {
		return fmt.Errorf("%w: cannot scan SQL NULL into %s", ErrNullValue, name)
	}
	if v, ok := src.(T); ok {
		*dst = v
		return nil
	}
	if vp, ok := src.(*T); ok && vp != nil {
		*dst = *vp
		return nil
	}
	return fmt.Errorf("%w: cannot scan %T into %s", ErrTypeMismatch, src, name)
}

func (h *Hugeint) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Hugeint()
		if err != nil {
			return err
		}
		*h = x
		return nil
	}
	return scanExact(h, src, "duckdb.Hugeint")
}

func (h Hugeint) Value() (driver.Value, error) { return h, nil }

func (u *Uhugeint) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Uhugeint()
		if err != nil {
			return err
		}
		*u = x
		return nil
	}
	return scanExact(u, src, "duckdb.Uhugeint")
}

func (u Uhugeint) Value() (driver.Value, error) { return u, nil }

func (d *Decimal) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Decimal()
		if err != nil {
			return err
		}
		*d = x
		return nil
	}
	return scanExact(d, src, "duckdb.Decimal")
}

func (u *UUID) Scan(src any) error {
	if src == nil {
		return fmt.Errorf("%w: cannot scan SQL NULL into duckdb.UUID", ErrNullValue)
	}
	switch x := src.(type) {
	case UUID:
		*u = x
		return nil
	case *UUID:
		if x == nil {
			return fmt.Errorf("%w: cannot scan nil *UUID", ErrNullValue)
		}
		*u = *x
		return nil
	case Value:
		v, err := x.UUID()
		if err != nil {
			return err
		}
		*u = v
		return nil
	case string:
		parsed, err := ParseUUID(x)
		if err != nil {
			return err
		}
		*u = parsed
		return nil
	case []byte:
		if len(x) == 16 {
			copy(u[:], x)
			return nil
		}
		parsed, err := ParseUUID(string(x))
		if err != nil {
			return err
		}
		*u = parsed
		return nil
	default:
		return fmt.Errorf("%w: cannot scan %T into duckdb.UUID", ErrTypeMismatch, src)
	}
}

func (u UUID) Value() (driver.Value, error) { return u, nil }

func (d *Date) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Date()
		if err != nil {
			return err
		}
		*d = x
		return nil
	}
	return scanExact(d, src, "duckdb.Date")
}

func (d Date) Value() (driver.Value, error) { return d, nil }

func (t *Time) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Time()
		if err != nil {
			return err
		}
		*t = x
		return nil
	}
	return scanExact(t, src, "duckdb.Time")
}

func (t Time) Value() (driver.Value, error) { return t, nil }

func (t *TimeNS) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.TimeNS()
		if err != nil {
			return err
		}
		*t = x
		return nil
	}
	return scanExact(t, src, "duckdb.TimeNS")
}

func (t TimeNS) Value() (driver.Value, error) { return t, nil }

func (t *TimeTZ) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.TimeTZ()
		if err != nil {
			return err
		}
		*t = x
		return nil
	}
	return scanExact(t, src, "duckdb.TimeTZ")
}

func (t TimeTZ) Value() (driver.Value, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Timestamp) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Timestamp()
		if err != nil {
			return err
		}
		*t = x
		return nil
	}
	return scanExact(t, src, "duckdb.Timestamp")
}

func (iv *Interval) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Interval()
		if err != nil {
			return err
		}
		*iv = x
		return nil
	}
	return scanExact(iv, src, "duckdb.Interval")
}

func (iv Interval) Value() (driver.Value, error) { return iv, nil }

func (e *Enum) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Enum()
		if err != nil {
			return err
		}
		*e = x
		return nil
	}
	return scanExact(e, src, "duckdb.Enum")
}

func (b *Bit) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Bit()
		if err != nil {
			return err
		}
		*b = x
		return nil
	}
	return scanExact(b, src, "duckdb.Bit")
}

func (b Bit) Value() (driver.Value, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Bignum) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Bignum()
		if err != nil {
			return err
		}
		*b = x
		return nil
	}
	return scanExact(b, src, "duckdb.Bignum")
}

func (b Bignum) Value() (driver.Value, error) { return b, nil }

func (g *Geometry) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Geometry()
		if err != nil {
			return err
		}
		*g = x
		return nil
	}
	return scanExact(g, src, "duckdb.Geometry")
}

func (u *UnionValue) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Union()
		if err != nil {
			return err
		}
		*u = x
		return nil
	}
	return scanExact(u, src, "duckdb.UnionValue")
}

func (l *ListValue) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.List()
		if err != nil {
			return err
		}
		*l = x
		return nil
	}
	return scanExact(l, src, "duckdb.ListValue")
}

func (l ListValue) Value() (driver.Value, error) { return l, nil }

func (a *ArrayValue) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Array()
		if err != nil {
			return err
		}
		*a = x
		return nil
	}
	return scanExact(a, src, "duckdb.ArrayValue")
}

func (a ArrayValue) Value() (driver.Value, error) { return a, nil }

func (s *StructValue) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Struct()
		if err != nil {
			return err
		}
		*s = x
		return nil
	}
	return scanExact(s, src, "duckdb.StructValue")
}

func (s StructValue) Value() (driver.Value, error) { return s, nil }

func (m *MapValue) Scan(src any) error {
	if v, ok := src.(Value); ok {
		x, err := v.Map()
		if err != nil {
			return err
		}
		*m = x
		return nil
	}
	return scanExact(m, src, "duckdb.MapValue")
}

func (m MapValue) Value() (driver.Value, error) { return m, nil }
