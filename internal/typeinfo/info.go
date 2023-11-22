package typeinfo

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// This expression should be aligned with the bytes we allow in isNameByte in
// the parser.
var validColNameRx = regexp.MustCompile(`^([a-zA-Z_])+([a-zA-Z_0-9])*$`)

// Info exposes useful information about types used in SQLair queries.
type Info interface {
	Typ() reflect.Type

	// TypeMember returns the type member associated with a given column name.
	TypeMember(member string) (Member, error)

	// GetAllMembers returns all members a type associated with column names.
	GetAllMembers() ([]Member, error)
}

type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) Typ() reflect.Type {
	return si.structType
}

func (si *structInfo) TypeMember(member string) (Member, error) {
	tm, ok := si.tagToField[member]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), member)
	}
	return tm, nil
}

func (si *structInfo) GetAllMembers() ([]Member, error) {
	if len(si.tags) == 0 {
		return nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
	}

	var tms []Member
	for _, tag := range si.tags {
		tms = append(tms, si.tagToField[tag])
	}
	return tms, nil
}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) Typ() reflect.Type {
	return mi.mapType
}

func (mi *mapInfo) TypeMember(member string) (Member, error) {
	return &mapKey{name: member, mapType: mi.mapType}, nil
}

func (mi *mapInfo) GetAllMembers() ([]Member, error) {
	return nil, fmt.Errorf(`columns must be specified for map with star`)
}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]Info)

// Reflect will return the typeInfo of a given type,
// generating and caching as required.
func GetTypeInfo(value any) (Info, error) {
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

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLair operation.
func generateTypeInfo(t reflect.Type) (Info, error) {
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
		return nil, fmt.Errorf("internal error: cannot obtain type information for type that is not map or struct: %s", t)
	}
}

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
