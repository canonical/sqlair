package typeinfo

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]*Info)

// Reflect will return the Info of a given type,
// generating and caching as required.
func GetTypeInfo(value any) (*Info, error) {
	if value == (any)(nil) {
		return &Info{}, fmt.Errorf("cannot reflect nil value")
	}

	v := reflect.ValueOf(value)
	v = reflect.Indirect(v)

	cacheMutex.RLock()
	info, found := cache[v.Type()]
	cacheMutex.RUnlock()
	if found {
		return info, nil
	}

	info, err := generate(v)
	if err != nil {
		return &Info{}, err
	}

	cacheMutex.Lock()
	cache[v.Type()] = info
	cacheMutex.Unlock()

	return info, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLAir operation.
func generate(value reflect.Value) (*Info, error) {
	// Dereference the value if it is a pointer.
	value = reflect.Indirect(value)

	// Reflection information is only generated for structs.
	if value.Kind() != reflect.Struct {
		return &Info{}, fmt.Errorf("can only reflect struct type")
	}

	info := Info{
		TagToField: make(map[string]Field),
		FieldToTag: make(map[string]string),
		Type:       value.Type(),
	}

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
			return &Info{}, err
		}
		info.TagToField[tag] = Field{
			Name:      field.Name,
			Index:     i,
			OmitEmpty: omitEmpty,
			Type:      reflect.TypeOf(value.Field(i).Interface()),
		}
		info.FieldToTag[field.Name] = tag
	}

	return &info, nil
}

// This expression should be aligned with the bytes we allow in isNameByte in
// the parser.
var validColNameRx = regexp.MustCompile(`^([a-zA-Z_])+([a-zA-Z_0-9])*$`)

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	// Refuse to parse if there are more than 2 items.
	if len(options) > 2 {
		return "", false, fmt.Errorf("too many options in 'db' tag")
	}
	if len(options) == 2 {
		if strings.ToLower(options[1]) != "omitempty" {
			return "", false, fmt.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	name := options[0]
	if len(name) == 0 {
		return "", false, fmt.Errorf("empty db tag")
	}

	if !validColNameRx.MatchString(name) {
		return "", false, fmt.Errorf("invalid column name in 'db' tag")
	}

	return name, omitEmpty, nil
}
