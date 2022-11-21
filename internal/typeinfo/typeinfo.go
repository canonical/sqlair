package typeinfo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var cacheMutex sync.RWMutex
var cache = make(map[string]Info)

// Reflect will return the Info of a given type,
// generating and caching as required.
func GetTypeInfo(value any) (Info, error) {
	if value == (any)(nil) {
		return Info{}, fmt.Errorf("Can not reflect nil value")
	}

	v := reflect.ValueOf(value)
	v = reflect.Indirect(v)

	tname := v.Type().Name()

	cacheMutex.RLock()
	info, found := cache[tname]
	cacheMutex.RUnlock()
	if found {
		return info, nil
	}

	info, err := generate(v)
	if err != nil {
		return Info{}, err
	}

	cacheMutex.Lock()
	cache[tname] = info
	cacheMutex.Unlock()

	return info, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLAir operation.
func generate(value reflect.Value) (Info, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)

	// Reflection information is generated for structs, typeinfo.M
	// and plain types only.
	if value.Kind() != reflect.Struct {
		if value.Kind() == reflect.Map && value.Type().Name() != "M" {
			return Info{}, fmt.Errorf("Can't reflect map type")
		} else {
			return Info{Type: value.Type()}, nil
		}
	}

	info := Info{
		TagToField: make(map[string]Field),
		FieldToTag: make(map[string]string),
		Type:       value.Type(),
	}

	// If we reach this point, this is a reflect.Struct:
	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		// Fields without a "db" tag are outside of SQLAir's remit.
		tag := field.Tag.Get("db")
		if tag == "" {
			continue
		}
		tag, omitEmpty, err := parseTag(tag)
		if err != nil {
			return Info{}, err
		}
		info.TagToField[tag] = Field{
			Name:      field.Name,
			Index:     i,
			OmitEmpty: omitEmpty,
			Type:      reflect.TypeOf(value.Field(i).Interface()),
		}
		info.FieldToTag[field.Name] = tag
	}

	return info, nil

	return info, nil
}

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		if strings.ToLower(options[1]) != "omitempty" {
			return "", false, fmt.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	return options[0], omitEmpty, nil
}
