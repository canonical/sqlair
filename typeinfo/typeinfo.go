package typeinfo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var cmutex sync.RWMutex
var cache = make(map[string]Info)

// Return the Info of a type name passed as parameter
// Returns error if the type has not been reflected yet.
func GetInfoFromName(typeName string) (Info, error) {
	cmutex.RLock()
	info, found := cache[typeName]
	cmutex.RUnlock()
	if found {
		return info, nil
	} else {
		return info, fmt.Errorf("unknown type")
	}
}

// Reflect will return the Info of a given type,
// generating and caching as required.
func GetTypeInfo(value any) (Info, error) {
	if value == (any)(nil) {
		return Info{}, fmt.Errorf("Can not reflect nil value")
	}

	v := reflect.ValueOf(value)
	v = reflect.Indirect(v)

	tname := v.Type().Name()

	cmutex.RLock()
	info, found := cache[tname]
	cmutex.RUnlock()
	if found {
		return info, nil
	}

	info, err := generate(v)
	if err != nil {
		return Info{}, err
	}

	// Do not cache for "M" types
	if !(info.Type.Kind() == reflect.Map && info.Type.Name() == "M") {
		cmutex.Lock()
		cache[tname] = info
		cmutex.Unlock()
	}

	return info, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for Sqlair operation.
func generate(value reflect.Value) (Info, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)

	// Reflection information is generated for structs, typeinfo.M
	// and plain types only.
	if value.Kind() != reflect.Struct {
		if value.Kind() == reflect.Map {
			if value.Type().Name() != "M" {
				return Info{}, fmt.Errorf("Can't reflect map type")
			}
		} else {
			return Info{Type: value.Type()}, nil
		}
	}

	info := Info{
		TagsToFields: make(map[string]Field),
		FieldsToTags: make(map[string]string),
		Type:         value.Type(),
	}

	switch value.Kind() {
	case reflect.Map:
		for _, key := range value.MapKeys() {
			info.TagsToFields[key.String()] = Field{
				Name:      key.String(),
				OmitEmpty: false,
				Type:      reflect.TypeOf(value.MapIndex(key).Interface()),
			}
		}

		return info, nil
	case reflect.Struct:
		typ := value.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			// Fields without a "db" tag are outside of Sqlair's remit.
			tag := field.Tag.Get("db")
			if tag == "" {
				continue
			}
			tag, omitEmpty, err := parseTag(tag)
			if err != nil {
				return Info{}, err
			}
			info.TagsToFields[tag] = Field{
				Name:      field.Name,
				Index:     i,
				OmitEmpty: omitEmpty,
				Type:      reflect.TypeOf(value.Field(i).Interface()),
			}
			info.FieldsToTags[field.Name] = tag
		}

		return info, nil
	}

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
