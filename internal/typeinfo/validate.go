// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import (
	"fmt"
	"reflect"
)

type TypeToValue = map[reflect.Type]reflect.Value

// ValidateInputs takes the raw SQLair input arguments from the user and uses
// reflection to check that they are valid. It returns a TypeToValue containing
// the reflect.Value of the input arguments.
func ValidateInputs(args []any) (TypeToValue, error) {
	typeToValue := TypeToValue{}
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if err := validateValue(v); err != nil {
			return nil, err
		}
		v = reflect.Indirect(v)
		t := v.Type()
		switch k := v.Kind(); k {
		case reflect.Map, reflect.Struct:
			if t.Name() == "" {
				return nil, fmt.Errorf("cannot use anonymous %s", k)
			}
			if _, ok := typeToValue[reflect.SliceOf(t)]; ok {
				return nil, typeAndSliceProvidedError(
					reflect.SliceOf(t), t)
			} else if _, ok := typeToValue[reflect.SliceOf(reflect.PointerTo(t))]; ok {
				return nil, typeAndSliceProvidedError(
					reflect.SliceOf(reflect.PointerTo(t)), t)
			}
		case reflect.Slice:
			// If the slice has no name and its element type is map, struct or
			// pointer then we assume it is for a bulk insert.
			switch t.Elem().Kind() {
			case reflect.Map, reflect.Struct:
				if _, ok := typeToValue[t.Elem()]; t.Name() == "" && ok {
					return nil, typeAndSliceProvidedError(t, t.Elem())
				}
			case reflect.Pointer:
				if _, ok := typeToValue[t.Elem().Elem()]; t.Name() == "" && ok {
					return nil, typeAndSliceProvidedError(t, t.Elem().Elem())
				}
			default:
				// We only care if a slice has no name it is not going to be
				// used for a bulk insert.
				if t.Name() == "" {
					return nil, fmt.Errorf("cannot use anonymous slice outside bulk insert")
				}
			}
		default:
			return nil, fmt.Errorf("need supported value, got %s", k)
		}
		if _, ok := typeToValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeToValue[t] = v
	}
	return typeToValue, nil
}

func checkDuplicate(tv TypeToValue, t reflect.Type) error {
	switch t.Kind() {
	case reflect.Slice:
		// If the slice has no name and its element type is map, struct or
		// pointer then we assume it is for a bulk insert.
		switch t.Elem().Kind() {
		case reflect.Map, reflect.Struct:
			if _, ok := tv[t.Elem()]; t.Name() == "" && ok {
				return typeAndSliceProvidedError(t, t.Elem())
			}
		case reflect.Pointer:
			if _, ok := tv[t.Elem().Elem()]; t.Name() == "" && ok {
				return typeAndSliceProvidedError(t, t.Elem().Elem())
			}
		}
	case reflect.Map, reflect.Struct:

	}
	return nil
}

// ValidateOutputs takes the raw SQLair output arguments from the user and uses
// reflection to check that they are valid. It returns a TypeToValue containing
// the reflect.Value of the output arguments.
func ValidateOutputs(args []any) (TypeToValue, error) {
	typeToValue := TypeToValue{}
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if err := validateValue(v); err != nil {
			return nil, err
		}
		k := v.Kind()
		if k != reflect.Map && k != reflect.Pointer {
			return nil, fmt.Errorf("need map or pointer to struct, got %s", k)
		}
		if k == reflect.Pointer {
			v = v.Elem()
			k = v.Kind()
			if k != reflect.Struct && k != reflect.Map {
				return nil, fmt.Errorf("need map or pointer to struct, got pointer to %s", k)
			}
		}
		t := v.Type()
		if _, ok := typeToValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeToValue[t] = v
	}
	return typeToValue, nil
}

func validateValue(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Invalid:
		return fmt.Errorf("got nil argument")
	case reflect.Pointer:
		if v.IsNil() {
			return fmt.Errorf("got nil pointer to %s", v.Type().Elem().Name())
		}
	case reflect.Map:
		if v.IsNil() {
			return fmt.Errorf("got nil %s", v.Type().Name())
		}
	}
	return nil
}

func typeAndSliceProvidedError(slice reflect.Type, elem reflect.Type) error {
	return fmt.Errorf("type %q and its slice type %q provided, unclear if bulk insert intended",
		PrettyTypeName(slice), PrettyTypeName(elem))
}
