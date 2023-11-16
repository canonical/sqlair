package typeinfo

import (
	"database/sql"
	"fmt"
	"reflect"
)

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Member describes a type that is a child
// of some other encapsulating type.
type Member interface {
	// OuterType returns the outer type in which this member is present.
	OuterType() reflect.Type

	// MemberName returns this member's name.
	MemberName() string

	// ValueForOuter returns the value represented by this
	// type member within the input outer value.
	ValueForOuter(reflect.Value) (reflect.Value, error)

	AddScanTarget(reflect.Value, []any, []ScanProxy) ([]any, []ScanProxy, error)
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

func (mk *mapKey) OuterType() reflect.Type {
	return mk.mapType
}

func (mk *mapKey) MemberName() string {
	return mk.name
}

func (mk *mapKey) ValueForOuter(v reflect.Value) (reflect.Value, error) {
	val := v.MapIndex(reflect.ValueOf(mk.name))
	if val.Kind() == reflect.Invalid {
		return val, fmt.Errorf("map %q does not contain key %q", mk.OuterType().Name(), mk.name)
	}
	return val, nil
}

func (mk *mapKey) AddScanTarget(outVal reflect.Value, ptrs []any, proxies []ScanProxy) ([]any, []ScanProxy, error) {
	scanVal := reflect.New(mk.mapType.Elem()).Elem()
	return append(ptrs, scanVal.Addr().Interface()),
		append(proxies, ScanProxy{original: outVal, scan: scanVal, key: reflect.ValueOf(mk.name)}), nil
}

// structField represents reflection information about a field from some struct type.
type structField struct {
	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index for Type.Field.
	index int

	// The tag associated with this field
	tag string

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

func (f *structField) OuterType() reflect.Type {
	return f.structType
}

func (f *structField) MemberName() string {
	return f.tag
}

func (f *structField) ValueForOuter(v reflect.Value) (reflect.Value, error) {
	return v.Field(f.index), nil
}

func (f *structField) AddScanTarget(outVal reflect.Value, ptrs []any, proxies []ScanProxy) ([]any, []ScanProxy, error) {
	val := outVal.Field(f.index)
	if !val.CanSet() {
		return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", f.name, f.structType.Name())
	}

	pt := reflect.PointerTo(val.Type())
	if val.Type().Kind() != reflect.Pointer && !pt.Implements(scannerInterface) {
		// Rows.Scan will return an error if it tries to scan NULL into a type that cannot be set to nil.
		// For types that are not a pointer and do not implement sql.Scanner a pointer to them is generated
		// and passed to Rows.Scan. If Scan has set this pointer to nil the value is zeroed.
		scanVal := reflect.New(pt).Elem()
		return append(ptrs, scanVal.Addr().Interface()), append(proxies, ScanProxy{original: val, scan: scanVal}), nil
	}
	return append(ptrs, val.Addr().Interface()), proxies, nil
}
