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
		case reflect.Map, reflect.Slice, reflect.Struct:
			if t.Name() == "" {
				return nil, fmt.Errorf("cannot use anonymous %s", k)
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
