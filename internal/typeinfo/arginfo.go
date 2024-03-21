// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// ArgInfo is used to access type information about SQLair input and output
// arguments. Methods on ArgInfo can be used to generate input and output value
// locators.
//
// ArgInfo should only be accessed using it methods, not used directly as a
// map.
type ArgInfo map[string]arg

// GenerateArgInfo takes sample instantiations of argument types and uses
// reflection to generate an ArgInfo containing the types.
func GenerateArgInfo(typeSamples []any) (ArgInfo, error) {
	argInfo := ArgInfo{}
	for _, typeSample := range typeSamples {
		if typeSample == nil {
			return nil, fmt.Errorf("need supported value, got nil")
		}
		t := reflect.TypeOf(typeSample)
		switch t.Kind() {
		case reflect.Struct, reflect.Map, reflect.Slice:
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
			return nil, fmt.Errorf("need non-pointer type, got pointer to %s", t.Elem().Kind())
		default:
			return nil, fmt.Errorf("need supported type, got %s", t.Kind())
		}
	}
	return argInfo, nil
}

// Kind looks up the type name and returns its kind.
func (argInfo ArgInfo) Kind(typeName string) (reflect.Kind, error) {
	arg, ok := argInfo[typeName]
	if !ok {
		return 0, nameNotFoundError(argInfo, typeName)
	}
	return arg.typ().Kind(), nil
}

// InputMember returns an input locator for a member of a struct or map.
func (argInfo ArgInfo) InputMember(typeName string, memberName string) (Input, error) {
	vl, err := argInfo.getMember(typeName, memberName)
	if err != nil {
		return nil, err
	}
	input, ok := vl.(Input)
	if !ok {
		return nil, fmt.Errorf("internal error: %s cannot be used as input", vl.ArgType().Kind())
	}
	return input, nil
}

// AllStructInputs returns a list of inputs locators that locate every member
// of the named type along with the names of the members. If the type is not a
// struct an error is returned.
func (argInfo ArgInfo) AllStructInputs(typeName string) ([]Input, []string, error) {
	si, err := argInfo.getAllStructMembers(typeName)
	if err != nil {
		return nil, nil, err
	}

	var inputs []Input
	for _, tag := range si.tags {
		inputs = append(inputs, si.tagToField[tag])
	}
	return inputs, si.tags, nil
}

// OutputMember returns an output locator for a member of a struct or map.
func (argInfo ArgInfo) OutputMember(typeName string, memberName string) (Output, error) {
	vl, err := argInfo.getMember(typeName, memberName)
	if err != nil {
		return nil, err
	}
	output, ok := vl.(Output)
	if !ok {
		return nil, fmt.Errorf("internal error: %s cannot be used as output", vl.ArgType().Kind())
	}
	return output, nil
}

// AllStructOutputs returns a list of output locators that locate every member
// of the named type along with the names of the members. If the type is not a
// struct an error is returned.
func (argInfo ArgInfo) AllStructOutputs(typeName string) ([]Output, []string, error) {
	si, err := argInfo.getAllStructMembers(typeName)
	if err != nil {
		return nil, nil, err
	}

	var outputs []Output
	for _, tag := range si.tags {
		outputs = append(outputs, si.tagToField[tag])
	}
	return outputs, si.tags, nil
}

// getMember finds a type and a member of it and returns a locator for the
// member. If the type does not have members it returns an error.
func (argInfo ArgInfo) getMember(typeName string, memberName string) (ValueLocator, error) {
	arg, ok := argInfo[typeName]
	if !ok {
		return nil, nameNotFoundError(argInfo, typeName)
	}
	switch arg := arg.(type) {
	case *structInfo:
		structField, ok := arg.tagToField[memberName]
		if !ok {
			return nil, fmt.Errorf(`type %q has no %q db tag`, arg.structType.Name(), memberName)
		}
		return structField, nil
	case *mapInfo:
		return &mapKey{name: memberName, mapType: arg.mapType}, nil
	default:
		return nil, fmt.Errorf("cannot get named member of %s", arg.typ().Kind())
	}
}

// getAllStructMembers returns a information about every member of the named type
// along with their names.
func (argInfo ArgInfo) getAllStructMembers(typeName string) (*structInfo, error) {
	arg, ok := argInfo[typeName]
	if !ok {
		return nil, nameNotFoundError(argInfo, typeName)
	}
	si, ok := arg.(*structInfo)
	if !ok {
		switch k := arg.typ().Kind(); k {
		case reflect.Map:
			return nil, fmt.Errorf("cannot use %s with asterisk unless columns are specified", k)
		case reflect.Slice:
			return nil, fmt.Errorf("cannot use %s with asterisk", k)
		default:
			return nil, fmt.Errorf("internal error: invalid arg type %s", k)
		}
	}
	if len(si.tags) == 0 {
		return nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
	}
	return si, nil
}

// InputSlice returns an input locator for a slice.
func (argInfo ArgInfo) InputSlice(typeName string) (Input, error) {
	arg, ok := argInfo[typeName]
	if !ok {
		return nil, nameNotFoundError(argInfo, typeName)
	}
	si, ok := arg.(*sliceInfo)
	if !ok {
		return nil, fmt.Errorf("cannot use slice syntax with %s", arg.typ().Kind())
	}
	return &slice{sliceType: si.sliceType}, nil
}

// arg exposes useful information about SQLair input/output argument types.
type arg interface {
	typ() reflect.Type
}

// structInfo stores information useful for SQLair about struct types.
type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) typ() reflect.Type {
	return si.structType
}

// mapInfo stores a map type.
type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) typ() reflect.Type {
	return mi.mapType
}

// sliceInfo stores a slice type
type sliceInfo struct {
	sliceType reflect.Type
}

func (si *sliceInfo) typ() reflect.Type {
	return si.sliceType
}

// argInfoCache caches type reflection information across queries.
var argInfoCacheMutex sync.RWMutex
var argInfoCache = make(map[reflect.Type]arg)

// getArgInfo returns type information useful for SQLair from a sample
// instantiation of an argument type.
func getArgInfo(t reflect.Type) (arg, error) {
	// Check cache for type
	argInfoCacheMutex.RLock()
	typeInfo, found := argInfoCache[t]
	argInfoCacheMutex.RUnlock()
	if found {
		return typeInfo, nil
	}

	switch t.Kind() {
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf(`map type %s must have key type string, found type %s`, t.Name(), t.Key().Kind())
		}
		typeInfo = &mapInfo{mapType: t}
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

		typeInfo = &info
	case reflect.Slice:
		return &sliceInfo{sliceType: t}, nil
	default:
		return nil, fmt.Errorf("internal error: cannot obtain type information for unsupported type: %s", t)
	}

	// Put type in cache.
	argInfoCacheMutex.Lock()
	argInfoCache[t] = typeInfo
	argInfoCacheMutex.Unlock()

	return typeInfo, nil
}

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		for _, flag := range options[1:] {
			if strings.TrimSpace(flag) == "omitempty" {
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

	// Check the tag is a valid column name.

	if name[0] == '"' || name[0] == '\'' {
		if name[len(name)-1] != name[0] {
			return "", false, fmt.Errorf("missing quotes at end of 'db' tag: %q", name)
		}
		// No need to validate chars in quotes.
		return name, omitEmpty, nil
	}

	char, size := utf8.DecodeRuneInString(name)
	nextPos := size
	var checker func(rune) bool
	switch {
	case unicode.IsDigit(char):
		// If it starts with a digit, check the tag is a number.
		checker = unicode.IsDigit
	case unicode.IsLetter(char) || char == '_':
		// Otherwise make sure it is made up of letters, digits and underscore.
		checker = func(char rune) bool {
			return unicode.IsLetter(char) || unicode.IsDigit(char) || char == '_'
		}
	default:
		return "", false, fmt.Errorf("invalid column name in 'db' tag: %q", name)
	}
	for nextPos < len(name) {
		char, size = utf8.DecodeRuneInString(name[nextPos:])
		nextPos += size
		if !(checker(char)) {
			return "", false, fmt.Errorf("invalid column name in 'db' tag: %q", name)
		}
	}

	return name, omitEmpty, nil
}

// nameNotFoundError generates the arguments present and returns a typeMissingError
func nameNotFoundError(argInfo ArgInfo, missingTypeName string) error {
	// Get names of the arguments we have from the ArgInfo keys.
	argNames := []string{}
	for argName := range argInfo {
		argNames = append(argNames, argName)
	}
	// Sort for consistent error messages.
	sort.Strings(argNames)
	return typeMissingError(missingTypeName, argNames)
}

// typeMissingError returns an error specifying the missing type and types
// that are present.
func typeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}
