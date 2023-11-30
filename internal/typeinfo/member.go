package typeinfo

import (
	"database/sql"
	"fmt"
	"reflect"
)

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Input is a locator for a Go value from SQLair input arguments to be used in
// a SQL query parameter.
type Input interface {
	// LocateParams locates the input argument associated with this Input and
	// then the Go value within it that is to be used in a query parameter. An
	// error is returned if the map does not contain the input argument.
	LocateParams(map[reflect.Type]reflect.Value) ([]reflect.Value, error)
	ValueLocator
}

// Output is a locator for a target to scan results to in the SQLair output
// arguments.
type Output interface {
	// GetScanTarget returns a pointer for the target of rows.Scan, and a
	// ScanProxy reference in the event that we need to coerce that pointer
	// into a struct field or map key.
	LocateScanTarget(map[reflect.Type]reflect.Value) (any, *ScanProxy, error)
	ValueLocator
}

// ValueLocator specifies how to locate a value in a SQLair argument type.
type ValueLocator interface {
	ArgType() reflect.Type
	String() string
}

// mapKey stores information about where to find a key of a particular map.
type mapKey struct {
	name    string
	mapType reflect.Type
}

// ArgType returns the type of the map the key is located in.
func (mk *mapKey) ArgType() reflect.Type {
	return mk.mapType
}

// LocateParams locates the map and then the value of the key specified in
// mapKey from the provided typeToValue map. An error is returned if the map
// does not contain this key. A slice with a single entry is returned to fit
// the Input interface.
func (mk *mapKey) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	m, err := locateValue(typeToValue, mk.mapType)
	if err != nil {
		return nil, err
	}
	v := m.MapIndex(reflect.ValueOf(mk.name))
	if v.Kind() == reflect.Invalid {
		return nil, fmt.Errorf("map %q does not contain key %q", mk.mapType.Name(), mk.name)
	}
	return []reflect.Value{v}, nil
}

// String returns a natural language description of the mapKey for use in error
// messages.
func (mk *mapKey) String() string {
	return "key \"" + mk.name + "\" of map \"" + mk.mapType.Name() + "\""
}

// LocateScanTarget locates the map specified in mapKey from the provided
// typeToValue map. It returns a pointer for to pass to rows.Scan, and a
// ScanProxy reference for setting the key value in the map once the pointer
// has been scanned into.
func (mk *mapKey) LocateScanTarget(typeToValue map[reflect.Type]reflect.Value) (any, *ScanProxy, error) {
	m, err := locateValue(typeToValue, mk.mapType)
	if err != nil {
		return nil, nil, err
	}
	scanVal := reflect.New(mk.mapType.Elem()).Elem()
	return scanVal.Addr().Interface(), &ScanProxy{original: m, scan: scanVal, key: reflect.ValueOf(mk.name)}, nil
}

// structField represents reflection information about a field of a particular
// struct type.
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

// ArgType returns the type of struct in this field is located in.
func (f *structField) ArgType() reflect.Type {
	return f.structType
}

// LocateParams locates the struct the field is located in from the typeToValue
// map. It returns the value of this field. A slice with a single entry is
// returned to fit the Input interface.
func (f *structField) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	s, err := locateValue(typeToValue, f.structType)
	if err != nil {
		return nil, err
	}
	return []reflect.Value{s.Field(f.index)}, nil
}

// String returns a natural language description of the struct field for use in
// error messages.
func (f *structField) String() string {
	return "tag \"" + f.tag + "\" of struct \"" + f.structType.Name() + "\""
}

// LocateScanTarget locates the struct specified in structField from the
// provided typeToValue map. It returns a pointer for the target of rows.Scan,
// and a ScanProxy reference in the event that we need to coerce that pointer
// into a struct field.
// rows.Scan will return an error if it tries to scan NULL into a type that
// cannot be set to nil, so for types that are not a pointer and do not
// implement sql.Scanner, a pointer to them is generated and passed to
// Rows.Scan. If Scan has set this pointer to nil the value is zeroed by
// ScanProxy.OnSuccess.
func (f *structField) LocateScanTarget(typeToValue map[reflect.Type]reflect.Value) (any, *ScanProxy, error) {
	s, err := locateValue(typeToValue, f.structType)
	if err != nil {
		return nil, nil, err
	}
	val := s.Field(f.index)
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

// locateValue locates the value corresponding to the given type in the
// typeToValueMap and returns an error message if it cannot be found.
func locateValue(typeToValue map[reflect.Type]reflect.Value, typ reflect.Type) (reflect.Value, error) {
	v, ok := typeToValue[typ]
	if !ok {
		argNames := []string{}
		for argType := range typeToValue {
			argNames = append(argNames, argType.Name())
		}
		return reflect.Value{}, typeMissingError(typ.Name(), argNames)
	}
	return v, nil
}
