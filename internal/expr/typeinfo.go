package expr

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// typeMember should be implemented without pointer receivers as it is used as a
// key in maps in some places.
type typeMember interface {
	outerType() reflect.Type
	memberName() string
}

type basicType struct {
	basicType reflect.Type
}

func (st basicType) outerType() reflect.Type {
	return st.basicType
}

func (st basicType) memberName() string {
	panic("internal error: memberName called on basicType")
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

func (mk mapKey) outerType() reflect.Type {
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

func (f structField) outerType() reflect.Type {
	return f.structType
}

func (f structField) memberName() string {
	return f.tag
}

// typeInfo exposes useful information about types used in SQLair queries.
type typeInfo interface {
	typ() reflect.Type

	// typeMember returns the type member associated with a given column name.
	typeMember(member string) (typeMember, error)

	// getAllMembers returns all members a type associated with column names.
	getAllMembers() ([]typeMember, error)
}

type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]structField
}

func (si *structInfo) typ() reflect.Type {
	return si.structType
}

func (si *structInfo) typeMember(member string) (typeMember, error) {
	if member == "" {
		return nil, fmt.Errorf(`unqualified type %q must be a basic type in`, si.structType.Name())
	}
	tm, ok := si.tagToField[member]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), member)
	}
	return tm, nil
}

func (si *structInfo) getAllMembers() ([]typeMember, error) {
	if len(si.tags) == 0 {
		return nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
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
	if member == "" {
		return nil, fmt.Errorf(`type %q missing map key`, mi.mapType.Name())
	}
	return mapKey{name: member, mapType: mi.mapType}, nil
}

func (mi *mapInfo) getAllMembers() ([]typeMember, error) {
	return nil, fmt.Errorf(`columns must be specified for map with star`)
}

var _ typeInfo = &mapInfo{}

type basicTypeInfo struct {
	basicType reflect.Type
}

func (pti *basicTypeInfo) typ() reflect.Type {
	return pti.basicType
}

func (pti *basicTypeInfo) typeMember(member string) (typeMember, error) {
	if member != "" {
		return nil, fmt.Errorf(`cannot specify member of basic type %q`, pti.basicType.Name())
	}
	return basicType{basicType: pti.basicType}, nil
}

func (pti *basicTypeInfo) getAllMembers() ([]typeMember, error) {
	return nil, fmt.Errorf(`internal error: basic type used in invalid context`)
}

var _ typeInfo = &basicTypeInfo{}

var basicKinds = map[reflect.Kind]bool{
	reflect.String:  true,
	reflect.Bool:    true,
	reflect.Int:     true,
	reflect.Int8:    true,
	reflect.Int16:   true,
	reflect.Int32:   true,
	reflect.Int64:   true,
	reflect.Uint:    true,
	reflect.Uint8:   true,
	reflect.Uint16:  true,
	reflect.Uint32:  true,
	reflect.Uint64:  true,
	reflect.Float32: true,
	reflect.Float64: true,
}

func IsBasicKind(k reflect.Kind) bool {
	return basicKinds[k]
}

var basicTypes = map[string]any{
	"string":  "",
	"bool":    false,
	"uint":    uint(0),
	"uint8":   uint8(0),
	"uint16":  uint16(0),
	"uint32":  uint32(0),
	"uint64":  uint64(0),
	"int":     int(0),
	"int8":    int8(0),
	"int16":   int16(0),
	"int32":   int32(0),
	"int64":   int64(0),
	"float32": float32(0),
	"float64": float64(0),
}

func lookupType(ti typeNameToInfo, typeName string) (typeInfo, bool) {
	if v, ok := basicTypes[typeName]; ok {
		return &basicTypeInfo{basicType: reflect.TypeOf(v)}, true
	}
	v, ok := ti[typeName]
	return v, ok
}

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
	switch k := t.Kind(); {
	case IsBasicKind(k):
		return &basicTypeInfo{basicType: t}, nil
	case k == reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf(`map type %s must have key type string, found type %s`, t.Name(), t.Key().Kind())
		}
		return &mapInfo{mapType: t}, nil
	case k == reflect.Struct:
		info := structInfo{
			tagToField: make(map[string]structField),
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
			info.tagToField[tag] = structField{
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
