package typeinfo

import (
	"reflect"
)

// Field represents a single field from a struct type.
type Field struct {
	Type reflect.Type

	// Name is the name of the struct field.
	Name string

	// Index of this field in the structure.
	Index int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	OmitEmpty bool
}

// Info represents reflected information about a struct type.
type Info struct {
	Type reflect.Type

	// Relate tag names to fields.
	TagToField map[string]Field

	// Relate field names to tags.
	FieldToTag map[string]string
}
