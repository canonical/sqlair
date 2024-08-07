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
	LocateParams(typeToValue TypeToValue) (params *Params, err error)
}

// Params contains query parameters and metadata generated from an input value.
type Params struct {
	// Vals contains the query parameters.
	Vals []any
	// Omit indicates if the params have an omitempty flag set and should be
	// omitted from input.
	Omit bool
	// Bulk is true if the list of values should be inserted in a bulk insert
	// expression.
	Bulk bool
	// ArgTypeUsed is the type of the argument that was used to generate the
	// params.
	ArgTypeUsed reflect.Type
}

// newParams generates a new Params struct.
func newParams(vals []any, omit bool, bulk bool, argType reflect.Type) *Params {
	return &Params{
		Vals:        vals,
		Omit:        omit,
		Bulk:        bulk,
		ArgTypeUsed: argType,
	}
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

// LocateParams locates the map (or slice of maps for a bulk insert) in
// typeToValue and then gets value associated with the key specified in mapKey.
// An error is returned if any map does not contain this key.
func (mk *mapKey) LocateParams(typeToValue TypeToValue) (*Params, error) {
	var argType reflect.Type
	var vals []any
	if m, ok := typeToValue[mk.mapType]; ok {
		v := m.MapIndex(reflect.ValueOf(mk.name))
		if v.Kind() == reflect.Invalid {
			return nil, fmt.Errorf("map %q does not contain key %q", mk.mapType.Name(), mk.name)
		}
		argType = m.Type()
		vals = append(vals, v.Interface())
		return newParams(vals, false, false, argType), nil
	}
	if ms, ok := locateBulkType(typeToValue, mk.mapType); ok {
		if ms.Len() == 0 {
			return nil, fmt.Errorf("got slice of %q with length 0", mk.mapType.Name())
		}
		for i := 0; i < ms.Len(); i++ {
			m := ms.Index(i)
			if m.Kind() == reflect.Pointer {
				if m.IsNil() {
					return nil, fmt.Errorf("got nil pointer in slice of %q at index %d", mk.mapType.Name(), i)
				}
				m = m.Elem()
			}
			// The slice has the correct type so there is no need to check the
			// type of each element.
			if m.IsNil() {
				return nil, fmt.Errorf("got nil map in slice of %q at index %d", m.Type().Name(), i)
			}
			v := m.MapIndex(reflect.ValueOf(mk.name))
			if v.Kind() == reflect.Invalid {
				return nil, fmt.Errorf("map %q does not contain key %q", mk.mapType.Name(), mk.name)
			}
			vals = append(vals, v.Interface())
		}
		argType = ms.Type()
		return newParams(vals, false, true, argType), nil
	}
	return nil, valueNotFoundError(typeToValue, mk.mapType)
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

	// index for Type.FieldByIndex.
	index []int

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

// LocateParams locates the struct (or slice of structs for a bulk insert) that
// contains the field in the TypeToValue map. It returns Params containing the
// value of this field.
func (f *structField) LocateParams(typeToValue TypeToValue) (*Params, error) {
	omit := false
	var argType reflect.Type
	var vals []any
	if s, ok := typeToValue[f.structType]; ok {
		val := s.FieldByIndex(f.index)
		if val.IsZero() && f.omitEmpty {
			omit = true
		}
		argType = s.Type()
		vals = append(vals, val.Interface())
		return newParams(vals, omit, false, argType), nil
	}
	if ss, ok := locateBulkType(typeToValue, f.structType); ok {
		if ss.Len() == 0 {
			return nil, fmt.Errorf("got slice of %q with length 0", f.structType.Name())
		}

		for i := 0; i < ss.Len(); i++ {
			s := ss.Index(i)
			if s.Kind() == reflect.Pointer {
				if s.IsNil() {
					return nil, fmt.Errorf("got nil pointer in slice of %q at index %d", f.structType.Name(), i)
				}
				s = s.Elem()
			}
			// The slice has the correct type so there is no need to check the
			// type of each element.
			val := s.FieldByIndex(f.index)
			if f.omitEmpty {
				// If the omitemtpy flag is present, we expect either all rows to
				// have a zero value, or all have a none zero value. If we have a
				// mix then throw an error.
				if i == 0 && val.IsZero() {
					omit = true
				} else if val.IsZero() != omit {
					return nil, fmt.Errorf("got mix of zero and none zero values in %s which has the omitempty flag set, in a bulk insert, values must be all zero or all none zero", f.Desc())
				}
			}
			argType = ss.Type()
			vals = append(vals, val.Interface())
		}
		return newParams(vals, omit, true, argType), nil
	}
	return nil, valueNotFoundError(typeToValue, f.structType)
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
	val := s.FieldByIndex(f.index)
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

// locateBulkType type looks for a slice of t in typeToValue.
func locateBulkType(typeToValue TypeToValue, t reflect.Type) (reflect.Value, bool) {
	if bt, ok := typeToValue[reflect.SliceOf(t)]; ok {
		return bt, true
	}
	bt, ok := typeToValue[reflect.SliceOf(reflect.PointerTo(t))]
	return bt, ok
}

// LocateParams locates the slice argument associated with the slice
// ValueLocator in typeToValue and returns the values objects generated
// by reflecting on the elements of the slice.
func (s *slice) LocateParams(typeToValue TypeToValue) (*Params, error) {
	sv, ok := typeToValue[s.sliceType]
	if !ok {
		return nil, valueNotFoundError(typeToValue, s.sliceType)
	}

	var vals []any
	for i := 0; i < sv.Len(); i++ {
		vals = append(vals, sv.Index(i).Interface())
	}
	return newParams(vals, false, false, s.sliceType), nil
}

// PrettyTypeName returns a human readable name for slices and pointers.
func PrettyTypeName(t reflect.Type) string {
	if t.Name() == "" {
		switch t.Kind() {
		case reflect.Slice:
			return "[]" + PrettyTypeName(t.Elem())
		case reflect.Pointer:
			return "*" + PrettyTypeName(t.Elem())
		}
	}
	return t.Name()
}

// valueNotFoundError generates the arguments present and returns a TypeMissingError
func valueNotFoundError(typeToValue TypeToValue, missingType reflect.Type) error {
	// Get the argument names from typeToValue map.
	argNames := []string{}
	for argType := range typeToValue {
		if argType.Name() == missingType.Name() {
			return fmt.Errorf("parameter with type %q missing, have type with same name: %q", missingType.String(), argType.String())
		}
		argNames = append(argNames, PrettyTypeName(argType))
	}
	// Sort for consistent error messages.
	sort.Strings(argNames)
	return TypeMissingError(PrettyTypeName(missingType), argNames)
}
