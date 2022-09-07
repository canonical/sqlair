package reflect

import (
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// Cache is responsible for generating, caching and retrieving reflection
// information for use in parsing and executing Sqlair DSL statements.
type cache struct {
	mutex sync.RWMutex
	cache map[reflect.Type]Info
}

// Reflect will return the Info of a given type,
// generating and caching as required.
func (r *cache) Reflect(value any) (Info, error) {
	v := reflect.ValueOf(value)
	v = reflect.Indirect(v)

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if rs, ok := r.cache[v.Type()]; ok {
		return rs, nil
	}

	ri, err := generate(v)
	if err != nil {
		return Struct{}, err
	}
	r.cache[v.Type()] = ri
	return ri, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for Sqlair operation.
func generate(value reflect.Value) (Info, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)

	// If this is a not a struct, we can not provide
	// any further reflection information.
	if value.Kind() != reflect.Struct {
		return Value{value: value}, nil
	}

	info := Struct{
		Fields: make(map[string]Field),
		value:  value,
	}

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
			return Value{}, err
		}

		info.Fields[tag] = Field{
			Name:      field.Name,
			OmitEmpty: omitEmpty,
			value:     value.Field(i),
		}
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
			return "", false, errors.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	return options[0], omitEmpty, nil
}
