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

// ArgInfo exposes useful information about SQLair input/output argument types.
type ArgInfo interface {
	Typ() reflect.Type
	// GetMember finds a type and a member of it and returns a locator for the
	// member. If the type does not have members it returns an error.
	GetMember(memberName string) (ValueLocator, error)
	// GetAllStructMembers returns information about every struct member of the
	// arg along with their names. If the arg is not a struct an error is
	// returned.
	GetAllStructMembers() ([]ValueLocator, []string, error)
	GetSlice() (ValueLocator, error)
}

// GenerateArgInfo takes sample instantiations of argument types and uses
// reflection to generate an ArgInfo for each. These ArgInfo objects are
// returned in a map keyed by the type names.
func GenerateArgInfo(typeSamples []any) (map[string]ArgInfo, error) {
	argInfo := map[string]ArgInfo{}
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
				if dupeArg.Typ() == t {
					return nil, fmt.Errorf("found multiple instances of type %q", t.Name())
				}
				return nil, fmt.Errorf("two types found with name %q: %q and %q", t.Name(), dupeArg.Typ().String(), t.String())
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

// structInfo stores information useful for SQLair about struct types.
type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) Typ() reflect.Type {
	return si.structType
}

// GetMember returns a value locator for the specified field of the struct.
func (si *structInfo) GetMember(memberName string) (ValueLocator, error) {
	structField, ok := si.tagToField[memberName]
	if !ok {
		return nil, fmt.Errorf(`type %q has no %q db tag`, si.structType.Name(), memberName)
	}
	return structField, nil
}

// GetAllStructMembers returns information about every member of the struct type
// along with their names.
func (si *structInfo) GetAllStructMembers() ([]ValueLocator, []string, error) {
	if len(si.tags) == 0 {
		return nil, nil, fmt.Errorf(`no "db" tags found in struct %q`, si.structType.Name())
	}

	var vls []ValueLocator
	for _, tag := range si.tags {
		vls = append(vls, si.tagToField[tag])
	}
	return vls, si.tags, nil
}

// GetSlice returns a an error.
func (si *structInfo) GetSlice() (ValueLocator, error) {
	return nil, fmt.Errorf("cannot use slice syntax with a struct")
}

// mapInfo stores a map type.
type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) Typ() reflect.Type {
	return mi.mapType
}

// GetMember returns a value locator for the specified key of the map
func (mi *mapInfo) GetMember(memberName string) (ValueLocator, error) {
	return &mapKey{name: memberName, mapType: mi.mapType}, nil
}

// GetAllStructMembers returns an error since maps do not have struct members
func (mi *mapInfo) GetAllStructMembers() ([]ValueLocator, []string, error) {
	return nil, nil, fmt.Errorf("cannot use map with asterisk unless columns are specified")
}

// GetSlice returns a an error.
func (mi *mapInfo) GetSlice() (ValueLocator, error) {
	return nil, fmt.Errorf("cannot use slice syntax with a map")
}

// sliceInfo stores a slice type
type sliceInfo struct {
	sliceType reflect.Type
}

func (si *sliceInfo) Typ() reflect.Type {
	return si.sliceType
}

// GetMember returns an error since slices do not have named members.
func (si *sliceInfo) GetMember(_ string) (ValueLocator, error) {
	return nil, fmt.Errorf("cannot get named member of slice")
}

// GetAllStructMembers returns an error since slices do not have struct members
func (si *sliceInfo) GetAllStructMembers() ([]ValueLocator, []string, error) {
	return nil, nil, fmt.Errorf("cannot use slice with asterisk")
}

// GetSlice returns a locator for a slice.
func (si *sliceInfo) GetSlice() (ValueLocator, error) {
	return &slice{sliceType: si.sliceType}, nil
}

// argInfoCache caches type reflection information across queries.
var argInfoCacheMutex sync.RWMutex
var argInfoCache = make(map[reflect.Type]ArgInfo)

// getArgInfo returns type information useful for SQLair from a sample
// instantiation of an argument type.
func getArgInfo(t reflect.Type) (ArgInfo, error) {
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

		fields, err := getStructFields(t)
		if err != nil {
			return nil, err
		}

		// Check for duplicate tags.
		for _, field := range fields {
			tags = append(tags, field.tag)
			if dup, ok := info.tagToField[field.tag]; ok {
				return nil, fmt.Errorf("db tag %q appears in both field %q and field %q of struct %q",
					field.tag, field.name, dup.name, t.Name())
			}
			info.tagToField[field.tag] = field
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

// getStructFields returns relevant reflection information about all struct
// fields included embedded fields. The caller must check that structType is a
// struct.
func getStructFields(structType reflect.Type) ([]*structField, error) {
	var fields []*structField
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("db")

		// If Anonymous is true, the field is embedded.
		if field.Anonymous && tag == "" {
			// If the embedded struct is tagged then we do not look inside it and
			// we pass it straight to the driver. This means it must implement the
			// Valuer or Scanner interface (for inputs/outputs respectively) or the
			// driver will reject it with a panic.
			if !field.IsExported() {
				continue
			}

			fieldType := field.Type
			if fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() != reflect.Struct {
				continue
			}
			// Promote the embedded struct fields into the current parent struct
			// scope, making sure to update the Index list for navigation back
			// to the original nested location.
			nestedFields, err := getStructFields(fieldType)
			if err != nil {
				return nil, err
			}
			for _, nestedField := range nestedFields {
				nestedField.index = append([]int{i}, nestedField.index...)
				nestedField.structType = structType
			}
			fields = append(fields, nestedFields...)
		} else {
			// Fields without a "db" tag are outside of SQLair's remit.
			if tag == "" {
				continue
			}
			if !field.IsExported() {
				return nil, fmt.Errorf("field %q of struct %s not exported", field.Name, structType.Name())
			}
			tag, omitEmpty, err := parseTag(tag)
			if err != nil {
				return nil, fmt.Errorf("cannot parse tag for field %s.%s: %s", structType.Name(), field.Name, err)
			}
			fields = append(fields, &structField{
				name:       field.Name,
				index:      field.Index,
				omitEmpty:  omitEmpty,
				tag:        tag,
				structType: structType,
			})
		}
	}
	return fields, nil
}

// TypeMissingError returns an error specifying the missing type and types
// that are present.
func TypeMissingError(missingType string, existingTypes []string) error {
	if len(existingTypes) == 0 {
		return fmt.Errorf(`parameter with type %q missing`, missingType)
	}
	// "%s" is used instead of %q to correctly print double quotes within the joined string.
	return fmt.Errorf(`parameter with type %q missing (have "%s")`, missingType, strings.Join(existingTypes, `", "`))
}
