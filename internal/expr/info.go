package expr

import (
	"reflect"
)

// field represents reflection information about a field from some struct type.
type field struct {
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

// Info represents reflected information about a struct type.
type structInfo struct {
	typ reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]field
}
