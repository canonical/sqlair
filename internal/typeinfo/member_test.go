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
	// we set it withe OnSuccess function in our map.
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
	c.Assert(scanProxy.scan, FitsTypeOf, reflect.ValueOf((*int)(nil)))

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

	_, err = input.LocateParams(typeToValue)
	c.Assert(err, ErrorMatches, `map "M" does not contain key "baz"`)
}
