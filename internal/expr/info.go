package expr

import (
	"reflect"
)

type typeMember interface {
	outerType() reflect.Type
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

func (mk mapKey) outerType() reflect.Type {
	return mk.mapType
}

// field represents reflection information about a field from some struct type.
type field struct {
	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index sequence for Type.FieldByIndex.
	index []int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

func (f field) outerType() reflect.Type {
	return f.structType
}

type info interface {
	typ() reflect.Type
}

type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]field
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
