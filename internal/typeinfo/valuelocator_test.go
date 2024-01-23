// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import (
	"reflect"

	. "gopkg.in/check.v1"
)

func (s *typeInfoSuite) TestLocateScanTargetMap(c *C) {
	type M map[string]any
	argInfo, err := GenerateArgInfo([]any{M{}})
	c.Assert(err, IsNil)

	output, err := argInfo.OutputMember("M", "foo")
	c.Assert(err, IsNil)

	m := M{}
	valOfM := reflect.ValueOf(m)
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(m): valOfM,
	}
	// Values in maps cannot be set directly. A proxy is set by rows.Scan then
	// we set it with the OnSuccess function in our map.
	ptr, scanProxy, err := output.LocateScanTarget(typeToValue)
	c.Assert(err, IsNil)

	// Check scanProxy has the expected values.
	c.Assert(scanProxy.original, Equals, valOfM)
	c.Assert(scanProxy.key.Interface(), Equals, "foo")
	c.Assert(scanProxy.scan, Not(IsNil))

	// Simulate rows.Scan
	ptrVal := reflect.ValueOf(&ptr).Elem()
	ptrVal.Set(reflect.ValueOf("bar"))
	scanProxy.scan = ptrVal

	// Check that the value in the proxy was successfully set in the map.
	scanProxy.OnSuccess()
	c.Assert(m["foo"], Equals, "bar")
}

func (s *typeInfoSuite) TestLocateScanTargetStruct(c *C) {
	type T struct {
		Foo string  `db:"foo"`
		Bar *string `db:"bar"`
	}

	argInfo, err := GenerateArgInfo([]any{T{}})
	c.Assert(err, IsNil)

	t := T{}
	valOfT := reflect.ValueOf(&t).Elem()
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(t): valOfT,
	}

	// Fields containing non-pointer values need a scan proxy allow scanning of
	// NULL. Foo is one such field.
	output, err := argInfo.OutputMember("T", "foo")
	c.Assert(err, IsNil)

	ptr, scanProxy, err := output.LocateScanTarget(typeToValue)
	c.Assert(err, IsNil)

	// Check scanProxy has the expected values.
	c.Assert(scanProxy.original, Equals, valOfT.FieldByName("Foo"))
	c.Assert(scanProxy.key, Equals, reflect.Value{})
	c.Assert(scanProxy.scan.Interface(), Equals, (*string)(nil))

	// Simulate rows.Scan
	ptrVal := reflect.ValueOf(&ptr).Elem()
	ptrVal.Set(reflect.ValueOf("baz"))
	scanProxy.scan = ptrVal

	scanProxy.OnSuccess()
	// Check that the value in the proxy was successfully moved to the field of the struct.
	c.Assert(t.Foo, Equals, "baz")

	// Test field Bar which does not need proxy as it is indirected by a
	// pointer.
	output, err = argInfo.OutputMember("T", "bar")
	c.Assert(err, IsNil)

	ptr, scanProxy, err = output.LocateScanTarget(typeToValue)
	c.Assert(err, IsNil)
	c.Assert(scanProxy, IsNil)
	c.Assert(ptr, FitsTypeOf, (**string)(nil))
}

func (s *typeInfoSuite) TestLocateScanTargetError(c *C) {
	type T struct {
		Foo string `db:"foo"`
	}
	type M map[string]any

	argInfo, err := GenerateArgInfo([]any{T{}, M{}})
	c.Assert(err, IsNil)

	output, err := argInfo.OutputMember("T", "foo")
	c.Assert(err, IsNil)

	// Check missing type error.
	_, _, err = output.LocateScanTarget(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "T" missing`)

	output, err = argInfo.OutputMember("M", "baz")
	c.Assert(err, IsNil)

	// Check missing type error.
	_, _, err = output.LocateScanTarget(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "M" missing`)

	// Check missing type with same name error.
	//
	// This error is designed to catch types from different packages with the
	// same name. Since this requires creating a package just for the test we
	// instead test it with a shadowed type which still hits this error
	// message.
	{
		type M map[string]any
		typeToValue := map[reflect.Type]reflect.Value{reflect.TypeOf(M{}): reflect.ValueOf(M{})}
		_, _, err = output.LocateScanTarget(typeToValue)
		c.Assert(err, ErrorMatches, `parameter with type "typeinfo.M" missing, have type with same name: "typeinfo.M"`)
	}
}

func (s *typeInfoSuite) TestLocateParamsMap(c *C) {
	type M map[string]any

	argInfo, err := GenerateArgInfo([]any{M{}})
	c.Assert(err, IsNil)

	m := M{"foo": "bar"}
	valOfM := reflect.ValueOf(m)
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(m): valOfM,
	}

	input, err := argInfo.InputMember("M", "foo")
	c.Assert(err, IsNil)

	vals, err := input.LocateParams(typeToValue)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 1)

	c.Assert(vals[0].Interface(), Equals, "bar")
}

func (s *typeInfoSuite) TestLocateParamsStruct(c *C) {
	type T struct {
		Foo string `db:"foo"`
	}

	argInfo, err := GenerateArgInfo([]any{T{}})
	c.Assert(err, IsNil)

	t := T{Foo: "bar"}
	valOfT := reflect.ValueOf(&t).Elem()
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(t): valOfT,
	}

	input, err := argInfo.InputMember("T", "foo")
	c.Assert(err, IsNil)

	vals, err := input.LocateParams(typeToValue)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 1)

	c.Assert(vals[0].Interface(), Equals, "bar")
}

func (s *typeInfoSuite) TestLocateParamsStructError(c *C) {
	type T struct {
		Foo string `db:"foo"`
	}

	argInfo, err := GenerateArgInfo([]any{T{}})
	c.Assert(err, IsNil)

	input, err := argInfo.InputMember("T", "foo")
	c.Assert(err, IsNil)

	// Check missing type error.
	_, err = input.LocateParams(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "T" missing`)
}

func (s *typeInfoSuite) TestLocateParamsMapError(c *C) {
	type M map[string]any

	argInfo, err := GenerateArgInfo([]any{M{}})
	c.Assert(err, IsNil)

	m := M{"foo": "bar"}
	valOfM := reflect.ValueOf(m)
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(m): valOfM,
	}

	input, err := argInfo.InputMember("M", "baz")
	c.Assert(err, IsNil)

	// Check missing key error.
	_, err = input.LocateParams(typeToValue)
	c.Assert(err, ErrorMatches, `map "M" does not contain key "baz"`)

	// Check missing type error.
	_, err = input.LocateParams(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "M" missing`)
}

func (*typeInfoSuite) TestLocateParamsSlice(c *C) {
	type S []any
	type T []int

	argInfo, err := GenerateArgInfo([]any{S{}, T{}})
	c.Assert(err, IsNil)

	tests := []struct {
		slice          any
		expectedValues []any
	}{{
		slice:          T{1, 2},
		expectedValues: []any{1, 2},
	}, {
		slice:          S{1, "two", 3.0},
		expectedValues: []any{1, "two", 3.0},
	}, {
		slice:          S{},
		expectedValues: []any{},
	}}

	for _, test := range tests {
		valOfSlice := reflect.ValueOf(test.slice)
		typeToValue := map[reflect.Type]reflect.Value{
			reflect.TypeOf(test.slice): valOfSlice,
		}

		input, err := argInfo.InputSlice(valOfSlice.Type().Name())
		c.Assert(err, IsNil)

		vals, err := input.LocateParams(typeToValue)
		c.Assert(err, IsNil)
		c.Assert(vals, HasLen, len(test.expectedValues))

		for i := 0; i < len(test.expectedValues); i++ {
			c.Assert(vals[i].Interface(), Equals, test.expectedValues[i])
		}
	}
}

func (*typeInfoSuite) TestLocateParamsSliceError(c *C) {
	type S []any
	type T []int

	argInfo, err := GenerateArgInfo([]any{S{}})
	c.Assert(err, IsNil)

	input, err := argInfo.InputSlice("S")
	c.Assert(err, IsNil)

	// Check missing type error.
	_, err = input.LocateParams(map[reflect.Type]reflect.Value{})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `parameter with type "S" missing`)

	// Check missing type error with one type present.
	_, err = input.LocateParams(map[reflect.Type]reflect.Value{reflect.TypeOf(T{}): reflect.ValueOf(T{})})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `parameter with type "S" missing (have "T")`)
}
