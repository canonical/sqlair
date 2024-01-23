// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
)

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// ValueLocator specifies how to locate a value in a SQLair argument type.
type ValueLocator interface {
	// ArgType is the type of the input/output argument that the specified
	// value is located in.
	ArgType() reflect.Type
	// Desc returns a written description of the ValueLocator for error messages.
	Desc() string
	// Identifier returns a string that uniquely identifies the ValueLocator in
	// the query.
	Identifier() string
}

// Input is a locator for a Go value from SQLair input arguments to be used in
// a SQL query parameter.
type Input interface {
	ValueLocator
	// LocateParams locates the input argument associated with this Input in
	// the typeToValue map and then returns the Go values within the input
	// argument that are to be used in query parameters. An error is returned
	// if typeToValue does not contain the input argument.
	LocateParams(typeToValue TypeToValue) ([]reflect.Value, error)
}

// Output is a locator for a target to scan results to in the SQLair output
// arguments.
type Output interface {
	ValueLocator
	// LocateScanTarget locates the output argument associated this Output in
	// typeToValue and returns a pointer to the Go value within the output
	// argument for rows.Scan, along with a ScanProxy for the cases where the
	// output argument cannot be scanned into directly.
	//
	// rows.Scan will return an error if it tries to scan NULL into a type that
	// cannot be set to nil, so for types that are not a pointer and do not
	// implement sql.Scanner, a pointer to them is generated and passed to
	// Rows.Scan. If Scan has set this pointer to nil the value is zeroed by
	// ScanProxy.OnSuccess.
	LocateScanTarget(typeToValue TypeToValue) (any, *ScanProxy, error)
}

// mapKey specifies at which key to find a value in a particular map.
type mapKey struct {
	name    string
	mapType reflect.Type
}

// ArgType returns the type of the map the key is located in.
func (mk *mapKey) ArgType() reflect.Type {
	return mk.mapType
}

// LocateParams locates the map in typeToValue and then gets value assosiated
// with the key specified in mapKey. An error is returned if the map does not
// contain this key. A slice with a single entry is returned to fit the Input
// interface.
func (mk *mapKey) LocateParams(typeToValue TypeToValue) ([]reflect.Value, error) {
	m, ok := typeToValue[mk.mapType]
	if !ok {
		return nil, valueNotFoundError(typeToValue, mk.mapType)
	}
	v := m.MapIndex(reflect.ValueOf(mk.name))
	if v.Kind() == reflect.Invalid {
		return nil, fmt.Errorf("map %q does not contain key %q", mk.mapType.Name(), mk.name)
	}
	return []reflect.Value{v}, nil
}

// Desc returns a natural language description of the mapKey for use in error
// messages.
func (mk *mapKey) Desc() string {
	return fmt.Sprintf("key %q of map %q", mk.name, mk.mapType.Name())
}

// Identifier returns a string that uniquely identifies the map key in the
// context of the query.
func (mk *mapKey) Identifier() string {
	return mk.mapType.Name() + "." + mk.name
}

// LocateScanTarget locates the map specified in mapKey from the provided
// typeToValue map. It returns a pointer to pass to rows.Scan, and a ScanProxy
// reference for setting the key value in the map once the pointer has been
// scanned into.
func (mk *mapKey) LocateScanTarget(typeToValue TypeToValue) (any, *ScanProxy, error) {
	m, ok := typeToValue[mk.mapType]
	if !ok {
		return nil, nil, valueNotFoundError(typeToValue, mk.mapType)
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

// ArgType returns the type of the struct this field is located in.
func (f *structField) ArgType() reflect.Type {
	return f.structType
}

// LocateParams locates the struct that contains the field in the typeToValue
// map. It returns the value of this field. A slice with a single entry is
// returned to fit the Input interface.
func (f *structField) LocateParams(typeToValue TypeToValue) ([]reflect.Value, error) {
	s, ok := typeToValue[f.structType]
	if !ok {
		return nil, valueNotFoundError(typeToValue, f.structType)
	}
	return []reflect.Value{s.Field(f.index)}, nil
}

// Desc returns a natural language description of the struct field for use in
// error messages.
func (f *structField) Desc() string {
	return fmt.Sprintf("tag %q of struct %q", f.tag, f.structType.Name())
}

// Identifier returns a string that uniquely identifies the struct field in the
// context of the query.
func (f *structField) Identifier() string {
	return f.structType.Name() + "." + f.tag
}

// LocateScanTarget locates the struct specified in structField from the
// provided typeToValue map. It returns a pointer for the target of rows.Scan,
// and a ScanProxy reference in the event that we need to coerce that pointer
// into a struct field.
func (f *structField) LocateScanTarget(typeToValue TypeToValue) (any, *ScanProxy, error) {
	s, ok := typeToValue[f.structType]
	if !ok {
		return nil, nil, valueNotFoundError(typeToValue, f.structType)
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

// slice represents a slice input.
type slice struct {
	sliceType reflect.Type
}

// Desc returns a natural language description of the slice for use in error
// messages.
func (s *slice) Desc() string {
	return fmt.Sprintf("slice %q", s.sliceType.Name())
}

// Identifier returns a string that uniquely identifies the slice type in the
// context of the query.
func (s *slice) Identifier() string {
	return s.sliceType.Name() + "[:]"
}

// ArgType is the type of the slice input to extract query parameters from.
func (s *slice) ArgType() reflect.Type {
	return s.sliceType
}

// LocateParams locates the slice argument assosiated with the slice
// ValueLocator in typeToValue and returns the reflect.Value objects generated
// by reflecting on the elements of the slice.
func (s *slice) LocateParams(typeToValue TypeToValue) ([]reflect.Value, error) {
	sv, ok := typeToValue[s.sliceType]
	if !ok {
		return nil, valueNotFoundError(typeToValue, s.sliceType)
	}

	params := []reflect.Value{}
	for i := 0; i < sv.Len(); i++ {
		params = append(params, sv.Index(i))
	}
	return params, nil
}

// valueNotFoundError generates the arguments present and returns a typeMissingError
func valueNotFoundError(typeToValue TypeToValue, missingType reflect.Type) error {
	// Get the argument names from typeToValue map.
	argNames := []string{}
	for argType := range typeToValue {
		if argType.Name() == missingType.Name() {
			return fmt.Errorf("parameter with type %q missing, have type with same name: %q", missingType.String(), argType.String())
		}
		argNames = append(argNames, argType.Name())
	}
	// Sort for consistant error messages.
	sort.Strings(argNames)
	return typeMissingError(missingType.Name(), argNames)
}
