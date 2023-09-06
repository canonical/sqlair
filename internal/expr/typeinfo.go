package expr

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type typeMember interface {
	outerType() reflect.Type
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

// structField represents reflection information about a field from some struct type.
type structField struct {
	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index for Type.Field.
	index int

	// The tag assosiated with this field
	tag string

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

func (f *structField) outerType() reflect.Type {
	return f.structType
}

func (f structField) memberName() string {
	return f.tag
}

type typeInfo interface {
	typ() reflect.Type
	typeMember(string) (typeMember, error)
	getAllMembers() ([]typeMember, error)
}

type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) typ() reflect.Type {
	return si.structType
}

func (si *structInfo) typeMember(member string) (typeMember, error) {
	tm, ok := si.tagToField[member]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), member)
	}
	return tm, nil
}

func (si *structInfo) getAllMembers() ([]typeMember, error) {
	if len(si.tags) == 0 {
		return nil, fmt.Errorf("type %q does not have any db tags", si.structType.Name())
	}

	tms := []typeMember{}
	for _, tag := range si.tags {
		tms = append(tms, si.tagToField[tag])
	}
	return tms, nil
}

var _ typeInfo = &structInfo{}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) typ() reflect.Type {
	return mi.mapType
}

func (mi *mapInfo) typeMember(member string) (typeMember, error) {
	return &mapKey{name: member, mapType: mi.mapType}, nil
}

func (mi *mapInfo) getAllMembers() ([]typeMember, error) {
	return nil, fmt.Errorf(`map type %q cannot be used with asterisk`, mi.mapType.Name())
}

var _ typeInfo = &mapInfo{}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]typeInfo)

// Reflect will return the typeInfo of a given type,
// generating and caching as required.
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

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLair operation.
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
		return nil, fmt.Errorf("internal error: cannot obtain type information for type that is not map or struct: %s.", t)
	}
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
