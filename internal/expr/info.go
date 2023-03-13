package expr

import (
	"reflect"
)

type Outputs = []field

// Field represents reflection information about a field from some struct type.
type field struct {
	typ reflect.Type

	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index sequence for Type.FieldByIndex.
	index []int

	// OmitEmpty is true when "omitempty" is
	// a property of the Field's "db" tag.
	omitEmpty bool
}

// Info represents reflected information about a struct type.
type info struct {
	typ reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]field
}
