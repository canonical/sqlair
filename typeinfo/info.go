package typeinfo

import (
	"reflect"
)

// Field represents a single field from a struct type.
type Field struct {
	Type reflect.Type

	// Name is the name of the struct field.
	Name string

	// Index of this field in the structure
	Index int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	OmitEmpty bool
}

// Info represents reflected information about a struct type.
type Info struct {
	Type reflect.Type

	// TagsToFields  maps "db" tags to struct fields.
	// SQLAir does not care about fields without a "db" tag.
	TagsToFields map[string]Field

	// FieldsToTags maps field names to tags
	FieldsToTags map[string]string
}

type M map[string]any
