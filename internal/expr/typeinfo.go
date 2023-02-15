package expr

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]*info)

func typeInfoFromCache(t reflect.Type) (*info, error) {
	cacheMutex.RLock()
	info, found := cache[t]
	cacheMutex.RUnlock()
	if found {
		return info, nil
	}
	return nil, fmt.Errorf("type %s not seen before", t.Name())
}

// Reflect will return the info of a given type,
// generating and caching as required.
func typeInfo(value any) (*info, error) {
	if value == (any)(nil) {
		return nil, fmt.Errorf("cannot reflect nil value")
	}

	t := reflect.TypeOf(value)

	cacheMutex.RLock()
	info, found := cache[t]
	cacheMutex.RUnlock()
	if found {
		return info, nil
	}

	info, err := generate(t)
	if err != nil {
		return nil, err
	}

	cacheMutex.Lock()
	cache[t] = info
	cacheMutex.Unlock()

	return info, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLAir operation.
func generate(t reflect.Type) (*info, error) {
	// Reflection information is only generated for structs.
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("internal error: attempted to obtain struct information for something that is not a struct: %s.", t)
	}

	info := info{
		tagToField: make(map[string]field),
		typ:        t,
	}
	tags := []string{}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		// Fields without a "db" tag are outside of SQLAir's remit.
		tag := f.Tag.Get("db")
		if tag == "" {
			continue
		}
		if !f.IsExported() {
			return nil, fmt.Errorf("field %q of struct %s not exported", f.Name, t.Name())
		}

		tag, omitEmpty, err := parseTag(tag)
		if err != nil {
			return nil, fmt.Errorf("cannot parse tag for field %s.%s: %s", t.Name(), f.Name, err)
		}
		tags = append(tags, tag)
		info.tagToField[tag] = field{
			name:      f.Name,
			index:     f.Index,
			omitEmpty: omitEmpty,
			typ:       f.Type,
		}
	}

	sort.Strings(tags)
	info.tags = tags

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
	if len(options) > 1 {
		for _, flag := range options[1:] {
			if flag == "omitempty" {
				omitEmpty = true
			} else {
				return "", omitEmpty, fmt.Errorf("unsupported flag %q in tag %q", flag, tag)
			}
		}
	}

	name := options[0]
	if len(name) == 0 {
		return "", false, fmt.Errorf("empty db tag")
	}

	if !validColNameRx.MatchString(name) {
		return "", false, fmt.Errorf("invalid column name in 'db' tag: %q", name)
	}

	return name, omitEmpty, nil
}
