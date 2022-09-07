package reflect

import (
	"reflect"
)

// Info describes the ability to return reflection information.
type Info interface {
	Name() string
	Kind() reflect.Kind
}

// Value represents reflection information for a simple type.
// It wraps a reflect.Value in order to implement Info.
type Value struct {
	value reflect.Value
}

// Kind returns the Value's reflect.Kind.
func (r Value) Kind() reflect.Kind {
	return r.value.Kind()
}

// Name returns the name of the Value's type.
func (r Value) Name() string {
	return r.value.Type().Name()
}

// Field represents a single field from a struct type.
type Field struct {
	value reflect.Value

	// Name is the name of the struct field.
	Name string

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	OmitEmpty bool
}

// Struct represents reflected information about a struct type.
type Struct struct {
	value reflect.Value

	// Fields maps "db" tags to struct fields.
	// Sqlair does not care about fields without a "db" tag.
	Fields map[string]Field
}

// Kind returns the Struct's reflect.Kind.
func (r Struct) Kind() reflect.Kind {
	return r.value.Kind()
}

// Name returns the name of the Struct's type.
func (r Struct) Name() string {
	return r.value.Type().Name()
}
