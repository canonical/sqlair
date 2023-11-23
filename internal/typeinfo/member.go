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

	// AccessorString returns the string used to access this member in an
	// expression.
	AccessorString() string

	// ValueFromOuter returns the value represented by this
	// type member within the input outer value.
	ValueFromOuter(reflect.Value) (reflect.Value, error)

	// GetScanTarget returns a pointer for the target of rows.Scan, and a
	// ScanProxy reference in the event that we need to coerce that pointer
	// into a struct field or map key.
	GetScanTarget(reflect.Value) (any, *ScanProxy, error)
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

// OuterType returns the reflected type of the map
// for which this Member implementation is a key.
func (mk *mapKey) OuterType() reflect.Type {
	return mk.mapType
}

// MemberName returns the map key.
func (mk *mapKey) MemberName() string {
	return mk.name
}

// AccessorString returns the string "mapName.keyName"
// used to access this map key in an expression.
func (mk *mapKey) AccessorString() string {
	return mk.mapType.Name() + "." + mk.name
}

// ValueFromOuter returns the value for this map key in the input reflected map.
// An error is returned if the map does not contain this key.
func (mk *mapKey) ValueFromOuter(v reflect.Value) (reflect.Value, error) {
	val := v.MapIndex(reflect.ValueOf(mk.name))
	if val.Kind() == reflect.Invalid {
		return val, fmt.Errorf("map %q does not contain key %q", mk.OuterType().Name(), mk.name)
	}
	return val, nil
}

// GetScanTarget returns a pointer for the target of rows.Scan, and a ScanProxy
// reference for setting that target as the value for this map key.
func (mk *mapKey) GetScanTarget(outVal reflect.Value) (any, *ScanProxy, error) {
	scanVal := reflect.New(mk.mapType.Elem()).Elem()
	return scanVal.Addr().Interface(), &ScanProxy{original: outVal, scan: scanVal, key: reflect.ValueOf(mk.name)}, nil
}

// structField represents reflection information about a field from some struct type.
type structField struct {
	// name is the member name within the struct.
	name string

	// structType is the reflected type of the struct containing this field.
	structType reflect.Type

	// index for Type.Field.
	index int

	// tag is the struct tag associated with this field.
	tag string

	// omitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

// OuterType returns the reflected type of struct in
// which this Member implementation is a field.
func (f *structField) OuterType() reflect.Type {
	return f.structType
}

// MemberName returns the name of this struct field.
func (f *structField) MemberName() string {
	return f.tag
}

// AccessorString returns the string "structName.tag"
// used to access this struct field in an expression.
func (f *structField) AccessorString() string {
	return f.structType.Name() + "." + f.name
}

// ValueFromOuter returns the value of this field in the input reflected struct.
func (f *structField) ValueFromOuter(v reflect.Value) (reflect.Value, error) {
	return v.Field(f.index), nil
}

// GetScanTarget returns a pointer for the target of rows.Scan, and a ScanProxy
// reference in the event that we need to coerce that pointer into a struct
// field.
// Rows.Scan will return an error if it tries to scan NULL into a type that
// cannot be set to nil, so for types that are not a pointer and do not
// implement sql.Scanner, a pointer to them is generated and passed to
// Rows.Scan. If Scan has set this pointer to nil the value is zeroed by
// ScanProxy.OnSuccess.
func (f *structField) GetScanTarget(outVal reflect.Value) (any, *ScanProxy, error) {
	val := outVal.Field(f.index)
	if !val.CanSet() {
		return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", f.name, f.structType.Name())
	}

	pt := reflect.PointerTo(val.Type())
	if val.Type().Kind() != reflect.Pointer && !pt.Implements(scannerInterface) {
		scanVal := reflect.New(pt).Elem()
		return scanVal.Addr().Interface(), &ScanProxy{original: val, scan: scanVal}, nil
	}
	return val.Addr().Interface(), nil, nil
}
