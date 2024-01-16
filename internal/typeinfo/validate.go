package typeinfo

import (
	"fmt"
	"reflect"
)

func ValidateInputs(args []any) (map[reflect.Type]reflect.Value, error) {
	typeToValue := map[reflect.Type]reflect.Value{}
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if isNil(v) {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		v = reflect.Indirect(v)
		k := v.Kind()
		if k != reflect.Struct && k != reflect.Map {
			return nil, fmt.Errorf("need struct or map, got %s", k)
		}
		t := v.Type()
		if _, ok := typeToValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeToValue[t] = v
	}
	return typeToValue, nil
}

func ValidateOutputs(args []any) (map[reflect.Type]reflect.Value, error) {
	typeToValue := map[reflect.Type]reflect.Value{}
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if isNil(v) {
			return nil, fmt.Errorf("need map or pointer to struct, got nil")
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

func isNil(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Pointer, reflect.Map:
		return v.IsNil()
	}
	return false
}
