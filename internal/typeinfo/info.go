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

type ArgInfos map[string]Arg

func (ais ArgInfos) GetArgWithMembers(name string) (ArgWithMembers, error) {
	argInfo, ok := ais[name]
	if !ok {
		argNames := []string{}
		for argName := range ais {
			argNames = append(argNames, argName)
		}
		// Sort for consistant error messages.
		sort.Strings(argNames)
		return nil, typeMissingError(name, argNames)
	}
	argWMInfo, ok := argInfo.(ArgWithMembers)
	if !ok {
		return nil, fmt.Errorf("internal error: type %s does not have members", argInfo.Typ().Name())
	}
	return argWMInfo, nil
}

// Arg exposes useful information about SQLair input/output argument types.
type Arg interface {
	Typ() reflect.Type
}

// ArgWithMembers represents a struct or a map argument.
type ArgWithMembers interface {
	// OutputMember returns the member of the arg associated with a given
	// column name.
	OutputMember(string) (Output, error)

	// InputMember returns the member of the arg associated with a given
	// column name.
	InputMember(string) (Input, error)

	// AllOutputMembers returns all members a type associated with column names.
	AllOutputMembers() ([]Output, error)
	AllMemberNames() ([]string, error)
	Arg
}

type SliceArg interface {
	GetRange()
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

func (si *structInfo) InputMember(name string) (Input, error) {
	return si.member(name)
}

func (si *structInfo) OutputMember(name string) (Output, error) {
	return si.member(name)
}

func (si *structInfo) member(member string) (*structField, error) {
	tm, ok := si.tagToField[member]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), member)
	}
	return tm, nil
}

func (si *structInfo) AllOutputMembers() ([]Output, error) {
	if len(si.tags) == 0 {
		return nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
	}

	var os []Output
	for _, tag := range si.tags {
		os = append(os, si.tagToField[tag])
	}
	return os, nil
}

func (si *structInfo) AllMemberNames() ([]string, error) {
	return si.tags, nil
}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) Typ() reflect.Type {
	return mi.mapType
}

func (mi *mapInfo) InputMember(name string) (Input, error) {
	return mi.member(name)
}

func (mi *mapInfo) OutputMember(name string) (Output, error) {
	return mi.member(name)
}

func (mi *mapInfo) member(name string) (*mapKey, error) {
	return &mapKey{name: name, mapType: mi.mapType}, nil
}

func (mi *mapInfo) AllOutputMembers() ([]Output, error) {
	return nil, fmt.Errorf(`columns must be specified for map with star`)
}

func (mi *mapInfo) AllMemberNames() ([]string, error) {
	return nil, fmt.Errorf(`columns must be specified for map with star`)
}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]Arg)

// GetArgInfo will return information useful for SQLair from a sample
// instantiation of an argument type.
func GetArgInfo(value any) (Arg, error) {
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
func generateTypeInfo(t reflect.Type) (Arg, error) {
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

func typeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}
