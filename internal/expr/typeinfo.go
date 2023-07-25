package expr

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// typeMember represents either a struct and db tag or a map and key.
type typeMember interface {

	// The struct or map type.
	outerType() reflect.Type

	// The db tag or key.
	memberName() string
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

func (mk *mapKey) outerType() reflect.Type {
	return mk.mapType
}

func (mk mapKey) memberName() string {
	return mk.name
}

// structField represents reflection information about a struct field.
type structField struct {
	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index of the field in the type. This is used with reflect.Type.Field().
	index int

	// The tag assosiated with this struct field.
	tag string

	// OmitEmpty is true when "omitempty" is a property of the field's "db"
	// tag.
	omitEmpty bool
}

func (f *structField) outerType() reflect.Type {
	return f.structType
}

func (f structField) memberName() string {
	return f.tag
}

// typeInfo can represent reflection information about a struct or a map.
type typeInfo interface {
	typ() reflect.Type
}

type structInfo struct {
	structType reflect.Type

	// tags is an ordered list of all db tags of the struct.
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) typ() reflect.Type {
	return si.structType
}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) typ() reflect.Type {
	return mi.mapType
}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]typeInfo)

// getTypeInfo will return the typeInfo of a given type, generating and caching
// as required.
func getTypeInfo(value any) (typeInfo, error) {
	if value == (any)(nil) {
		return nil, fmt.Errorf("cannot reflect nil value")
	}

	t := reflect.TypeOf(value)

	cacheMutex.RLock()
	typeInfo, found := cache[t]
	cacheMutex.RUnlock()
	if found {
		return typeInfo, nil
	}

	typeInfo, err := generateTypeInfo(t)
	if err != nil {
		return nil, err
	}

	cacheMutex.Lock()
	cache[t] = typeInfo
	cacheMutex.Unlock()

	return typeInfo, nil
}

// generateTypeInfo generates reflection information about a type that is
// required for SQLair.
func generateTypeInfo(t reflect.Type) (typeInfo, error) {
	switch t.Kind() {
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf(`map type %s must have key type string, found type %s`, t.Name(), t.Key().Kind())
		}
		return &mapInfo{mapType: t}, nil
	case reflect.Struct:
		info := structInfo{
			tagToField: make(map[string]*structField),
			structType: t,
		}
		tags := []string{}

		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
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
			info.tagToField[tag] = &structField{
				name:       f.Name,
				index:      i,
				omitEmpty:  omitEmpty,
				tag:        tag,
				structType: t,
			}
		}

		sort.Strings(tags)
		info.tags = tags

		return &info, nil
	default:
		return nil, fmt.Errorf("internal error: cannot obtain type information for type that is not map or struct: %s.", t)
	}
}

// This expression should be aligned with the bytes allowed in isNameByte in
// the parser.
var validColNameRx = regexp.MustCompile(`^([a-zA-Z_])+([a-zA-Z_0-9])*$`)

// parseTag parses the full db tag and returns the column name it specifies,
// and whether it contains the "omitempty" flag.
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
