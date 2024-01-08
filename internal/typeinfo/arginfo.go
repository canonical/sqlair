// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
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
			return nil, fmt.Errorf("need valid value, got nil")
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
	arg, ok := argInfo[typeName]
	if !ok {
		return nil, nil, nameNotFoundError(argInfo, typeName)
	}
	si, ok := arg.(*structInfo)
	if !ok {
		errStr := ""
		switch arg.typ().Kind() {
		case reflect.Map:
			errStr = "cannot use %s with asterisk unless columns are specified"
		case reflect.Slice:
			errStr = "cannot use %s with asterisk"
		default:
			errStr = "internal error: unknown invalid arg type %s"
		}
		return nil, nil, fmt.Errorf(errStr, arg.typ().Kind())
	}
	if len(si.tags) == 0 {
		return nil, nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
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
		return nil, fmt.Errorf("cannot get named member of type %q", arg.typ().Name())
	}
}

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

// nameNotFoundError generates the arguments present and returns a typeMissingError
func nameNotFoundError(argInfo ArgInfo, missingTypeName string) error {
	// Get names of the arguments we have from the ArgInfo keys.
	argNames := []string{}
	for argName := range argInfo {
		argNames = append(argNames, argName)
	}
	// Sort for consistant error messages.
	sort.Strings(argNames)
	return typeMissingError(missingTypeName, argNames)
}

// typeMissingError returns an error specificing the missing type and types
// that are present.
func typeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}
