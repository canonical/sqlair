package typeinfo

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

type Input interface {
	// GetParams returns the query parameters represented
	// by this Input from the assosiated input argument.
	LocateParams(map[reflect.Type]reflect.Value) ([]reflect.Value, error)
	ValueLocator
}

type Output interface {
	// GetScanTarget returns a pointer for the target of rows.Scan, and a
	// ScanProxy reference in the event that we need to coerce that pointer
	// into a struct field or map key.
	LocateScanTarget(map[reflect.Type]reflect.Value) (any, *ScanProxy, error)
	ValueLocator
}

type ValueLocator interface {
	ArgType() reflect.Type
	String() string
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

// OuterType returns the reflected type of the map
// for which this Member implementation is a key.
func (mk *mapKey) ArgType() reflect.Type {
	return mk.mapType
}

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

// ValueFromOuter returns the value for this map key in the input reflected map.
// An error is returned if the map does not contain this key.
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

// MemberName returns the map key.
func (mk *mapKey) String() string {
	return "key \"" + mk.name + "\" of map \"" + mk.mapType.Name() + "\""
}

// GetScanTarget returns a pointer for the target of rows.Scan, and a ScanProxy
// reference for setting that target as the value for this map key.
func (mk *mapKey) LocateScanTarget(typeToValue map[reflect.Type]reflect.Value) (any, *ScanProxy, error) {
	m, err := locateValue(typeToValue, mk.mapType)
	if err != nil {
		return nil, nil, err
	}
	scanVal := reflect.New(mk.mapType.Elem()).Elem()
	return scanVal.Addr().Interface(), &ScanProxy{original: m, scan: scanVal, key: reflect.ValueOf(mk.name)}, nil
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
func (f *structField) ArgType() reflect.Type {
	return f.structType
}

// ValueFromOuter returns the value of this field in the input reflected struct.
func (f *structField) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	s, err := locateValue(typeToValue, f.structType)
	if err != nil {
		return nil, err
	}
	return []reflect.Value{s.Field(f.index)}, nil
}

func (f *structField) String() string {
	return "tag \"" + f.tag + "\" of struct \"" + f.structType.Name() + "\""
}

// GetScanTarget returns a pointer for the target of rows.Scan, and a ScanProxy
// reference in the event that we need to coerce that pointer into a struct
// field.
// Rows.Scan will return an error if it tries to scan NULL into a type that
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

type sliceRange struct {
	sliceType reflect.Type
	low       *uint64
	high      *uint64
}

func (sr *sliceRange) ArgType() reflect.Type {
	return sr.sliceType
}

func (sr *sliceRange) String() string {
	low := ""
	if sr.low != nil {
		low = strconv.Itoa(int(*sr.low))
	}
	high := ""
	if sr.high != nil {
		high = strconv.Itoa(int(*sr.high))
	}
	return sr.sliceType.Name() + "[" + low + ":" + high + "]"
}

func (sr *sliceRange) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	s, err := locateValue(typeToValue, sr.sliceType)
	if err != nil {
		return nil, err
	}

	high := s.Len()
	if sr.high != nil {
		if int(*sr.high) > s.Len() {
			return nil, fmt.Errorf("slice range out of bounds")
		}
		high = int(*sr.high)
	}

	low := 0
	if sr.low != nil {
		if low > high {
			return nil, fmt.Errorf("slice range out of bounds")
		}
		low = int(*sr.low)
	}

	params := []reflect.Value{}
	for i := low; i < high; i++ {
		params = append(params, s.Index(i))
	}
	return params, nil
}

type sliceIndex struct {
	sliceType reflect.Type
	index     uint64
}

func (si *sliceIndex) ArgType() reflect.Type {
	return si.sliceType
}

func (si *sliceIndex) String() string {
	return si.sliceType.Name() + "[" + strconv.Itoa(int(si.index)) + "]"
}

func (si *sliceIndex) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	s, err := locateValue(typeToValue, si.sliceType)
	if err != nil {
		return nil, err
	}
	if int(si.index) >= s.Len() {
		return nil, fmt.Errorf("index out of range")
	}
	return []reflect.Value{s.Index(int(si.index))}, nil
}
