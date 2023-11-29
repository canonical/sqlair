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

// ArgInfo is used to access type information useful for SQLair. It should only
// be accessed using it methods, not used directly as a map.
type ArgInfo map[string]arg

// GenerateArgInfo returns type information useful for SQLair from sample
// instantiations of an argument type.
func GenerateArgInfo(typeSamples ...any) (ArgInfo, error) {
	argInfo := ArgInfo{}
	// Generate and save reflection info.
	for _, typeSample := range typeSamples {
		if typeSample == nil {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		t := reflect.TypeOf(typeSample)
		switch t.Kind() {
		case reflect.Struct, reflect.Map:
			if t.Name() == "" {
				return nil, fmt.Errorf("cannot use anonymous %s", t.Kind())
			}
			info, err := getArgInfo(t)
			if err != nil {
				return nil, err
			}
			if dupeArg, ok := argInfo[t.Name()]; ok {
				if dupeArg.typ() == t {
					return nil, fmt.Errorf("found multiple instances of type %q", t.Name())
				}
				return nil, fmt.Errorf("two types found with name %q: %q and %q", t.Name(), dupeArg.typ().String(), t.String())
			}
			argInfo[t.Name()] = info
		case reflect.Pointer:
			return nil, fmt.Errorf("need struct or map, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
	}
	return argInfo, nil
}

// InputMember returns an input locator for a member of a struct or map.
func (argInfo ArgInfo) InputMember(typeName string, member string) (Input, error) {
	vl, err := argInfo.getMember(typeName, member)
	if err != nil {
		return nil, err
	}
	input, ok := vl.(Input)
	if !ok {
		return nil, fmt.Errorf("internal error: %s cannot be used as input", vl.ArgType().Kind())
	}
	return input, nil
}

// OutputMember returns an output locator for a member of a struct or map.
func (argInfo ArgInfo) OutputMember(typeName string, member string) (Output, error) {
	vl, err := argInfo.getMember(typeName, member)
	if err != nil {
		return nil, err
	}
	output, ok := vl.(Output)
	if !ok {
		return nil, fmt.Errorf("internal error: %s cannot be used as output", vl.ArgType().Kind())
	}
	return output, nil
}

// AllOutputMembers returns a list of output locators that locate every member
// of the named type.
func (argInfo ArgInfo) AllOutputMembers(typeName string) ([]Output, []string, error) {
	arg, err := argInfo.getArg(typeName)
	if err != nil {
		return nil, nil, err
	}
	si, ok := arg.(*structInfo)
	if !ok {
		return nil, nil, fmt.Errorf("columns must be specified for map with star")
	}
	return si.allOutputMembers()
}

func (argInfo ArgInfo) getMember(typeName string, member string) (ValueLocator, error) {
	arg, err := argInfo.getArg(typeName)
	if err != nil {
		return nil, err
	}
	var vl ValueLocator
	switch arg := arg.(type) {
	case *structInfo:
		vl, err = arg.member(member)
	case *mapInfo:
		vl, err = arg.member(member)
	default:
		return nil, fmt.Errorf("")
	}
	if err != nil {
		return nil, err
	}
	return vl, nil
}

func (argInfo ArgInfo) getArg(typeName string) (arg, error) {
	arg, ok := argInfo[typeName]
	if !ok {
		argNames := []string{}
		for argName := range argInfo {
			argNames = append(argNames, argName)
		}
		// Sort for consistant error messages.
		sort.Strings(argNames)
		return nil, typeMissingError(typeName, argNames)
	}
	return arg, nil
}

// arg exposes useful information about SQLair input/output argument types.
type arg interface {
	typ() reflect.Type
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

func (si *structInfo) member(name string) (*structField, error) {
	tm, ok := si.tagToField[name]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), name)
	}
	return tm, nil
}

func (si *structInfo) allOutputMembers() ([]Output, []string, error) {
	if len(si.tags) == 0 {
		return nil, nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
	}

	var os []Output
	for _, tag := range si.tags {
		os = append(os, si.tagToField[tag])
	}
	return os, si.tags, nil
}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) typ() reflect.Type {
	return mi.mapType
}

func (mi *mapInfo) member(name string) (*mapKey, error) {
	return &mapKey{name: name, mapType: mi.mapType}, nil
}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]arg)

// getArgInfo will return information useful for SQLair from a sample
// instantiation of an argument type.
func getArgInfo(t reflect.Type) (arg, error) {
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
func generateTypeInfo(t reflect.Type) (arg, error) {
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
