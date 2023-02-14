package expr

import (
	"reflect"
)

// Field represents a single field from a struct type.
type field struct {
	fieldType reflect.Type

	// Name is the name of the struct field.
	name string

	// Index of this field in the structure.
	index int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

// Info represents reflected information about a struct type.
type info struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	// Relate tag names to fields.
	tagToField map[string]field
}
