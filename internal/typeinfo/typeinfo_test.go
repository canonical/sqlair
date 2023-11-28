package typeinfo

import (
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

var _ arg = &structInfo{}
var _ arg = &mapInfo{}
var _ arg = &sliceInfo{}

func TestTypeInfo(t *testing.T) { TestingT(t) }

type typeInfoSuite struct{}

var _ = Suite(&typeInfoSuite{})

func (e *typeInfoSuite) TestArgInfoStruct(c *C) {
	type myStruct struct {
		ID        int    `db:"id"`
		Name      string `db:"name,omitempty"`
		ValidTag1 int    `db:"_i_d_55_"`
		ValidTag2 int    `db:"IdENT99"`
		NotInDB   string
	}

	argInfo, err := GenerateArgInfo(myStruct{})
	c.Assert(err, IsNil)

	// The struct fields in this list are ordered according to how sort.Strings
	// orders the tag names. This matches the order of the Output values from
	// AllOutputMembers.
	structFields := []struct {
		fieldName string
		index     int
		omitEmpty bool
		tag       string
	}{{
		fieldName: "ValidTag2",
		index:     3,
		omitEmpty: false,
		tag:       "IdENT99",
	}, {
		fieldName: "ValidTag1",
		index:     2,
		omitEmpty: false,
		tag:       "_i_d_55_",
	}, {
		fieldName: "ID",
		index:     0,
		omitEmpty: false,
		tag:       "id",
	}, {
		fieldName: "Name",
		index:     1,
		omitEmpty: true,
		tag:       "name",
	}}

	allOutputs, names, err := argInfo.AllOutputMembers("myStruct")
	c.Assert(err, IsNil)
	c.Assert(allOutputs, HasLen, len(structFields))

	for i, t := range structFields {
		expectedStructField := &structField{
			name:       t.fieldName,
			structType: reflect.TypeOf(myStruct{}),
			index:      t.index,
			tag:        t.tag,
			omitEmpty:  t.omitEmpty,
		}

		input, err := argInfo.InputMember("myStruct", t.tag)
		c.Assert(err, IsNil)
		c.Assert(input, DeepEquals, expectedStructField)

		output, err := argInfo.OutputMember("myStruct", t.tag)
		c.Assert(err, IsNil)
		c.Assert(output, DeepEquals, expectedStructField)

		c.Assert(allOutputs[i], DeepEquals, expectedStructField)
		c.Assert(names[i], Equals, t.tag)
	}
}

func (e *typeInfoSuite) TestArgInfoMap(c *C) {
	type myMap map[string]any

	argInfo, err := GenerateArgInfo(myMap{})
	c.Assert(err, IsNil)

	expectedMapKey := &mapKey{mapType: reflect.TypeOf(myMap{}), name: "key"}

	input, err := argInfo.InputMember("myMap", expectedMapKey.name)
	c.Assert(err, IsNil)
	c.Assert(input, DeepEquals, expectedMapKey)

	output, err := argInfo.OutputMember("myMap", expectedMapKey.name)
	c.Assert(err, IsNil)
	c.Assert(output, DeepEquals, expectedMapKey)
}

// This struct is used to test shadowed types in TestGenerateArgInfoInvalidTypeErrors
type S struct{ foo int }

var s = S{}

func (e *typeInfoSuite) TestGenerateArgInfoInvalidTypeErrors(c *C) {
	type S struct{ foo int }
	type M map[string]any

	_, err := GenerateArgInfo(nil)
	c.Assert(err, ErrorMatches, "need struct or map, got nil")

	_, err = GenerateArgInfo(struct{ foo int }{})
	c.Assert(err, ErrorMatches, "cannot use anonymous struct")

	_, err = GenerateArgInfo(map[string]any{})
	c.Assert(err, ErrorMatches, "cannot use anonymous map")

	_, err = GenerateArgInfo(S{}, S{})
	c.Assert(err, ErrorMatches, `found multiple instances of type "S"`)

	_, err = GenerateArgInfo((*S)(nil))
	c.Assert(err, ErrorMatches, "need struct or map, got pointer to struct")

	_, err = GenerateArgInfo((*M)(nil))
	c.Assert(err, ErrorMatches, "need struct or map, got pointer to map")

	_, err = GenerateArgInfo("")
	c.Assert(err, ErrorMatches, "need struct or map, got string")

	_, err = GenerateArgInfo(0)
	c.Assert(err, ErrorMatches, "need struct or map, got int")

	_, err = GenerateArgInfo([10]int{})
	c.Assert(err, ErrorMatches, "need struct or map, got array")

	_, err = GenerateArgInfo(s, S{})
	c.Assert(err, ErrorMatches, `two types found with name "S": "typeinfo.S" and "typeinfo.S"`)
}

func (s *typeInfoSuite) TestGenerateArgInfoStructError(c *C) {
	type S1 struct {
		unexp int `db:"unexp"`
	}
	_, err := GenerateArgInfo(S1{})
	c.Assert(err, ErrorMatches, `field "unexp" of struct S1 not exported`)

	type S2 struct {
		Foo int `db:"id,bad-juju"`
	}
	_, err = GenerateArgInfo(S2{})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S2.Foo: unsupported flag "bad-juju" in tag "id,bad-juju"`)

	type S3 struct {
		Foo int `db:",omitempty"`
	}
	_, err = GenerateArgInfo(S3{})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S3.Foo: empty db tag`)

	type S4 struct {
		Foo int `db:"5id"`
	}
	_, err = GenerateArgInfo(S4{})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S4.Foo: invalid column name in 'db' tag: "5id"`)

	type S5 struct {
		Foo int `db:"+id"`
	}
	_, err = GenerateArgInfo(S5{})
	c.Assert(err.Error(), Equals, `cannot parse tag for field S5.Foo: invalid column name in 'db' tag: "+id"`)

	type S6 struct {
		Foo int `db:"id$$"`
	}
	_, err = GenerateArgInfo(S6{})
	c.Assert(err.Error(), Equals, `cannot parse tag for field S6.Foo: invalid column name in 'db' tag: "id$$"`)
}

func (s *typeInfoSuite) TestArgInfoStructError(c *C) {
	argInfo, err := GenerateArgInfo()
	c.Assert(err, IsNil)

	_, err = argInfo.OutputMember("wrongStruct", "foo")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing`)
	_, err = argInfo.InputMember("wrongStruct", "foo")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing`)
	_, _, err = argInfo.AllOutputMembers("wrongStruct")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing`)

	type myStruct struct {
		Foo int `db:"foo"`
	}
	type myOtherStruct struct {
		Bar int `db:"bar"`
	}
	argInfo, err = GenerateArgInfo(myStruct{}, myOtherStruct{})
	c.Assert(err, IsNil)

	_, err = argInfo.OutputMember("wrongStruct", "foo")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing (have "myOtherStruct", "myStruct")`)
	_, err = argInfo.InputMember("wrongStruct", "foo")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing (have "myOtherStruct", "myStruct")`)
	_, _, err = argInfo.AllOutputMembers("wrongStruct")
	c.Assert(err.Error(), Equals, `parameter with type "wrongStruct" missing (have "myOtherStruct", "myStruct")`)

	_, err = argInfo.OutputMember("myStruct", "bar")
	c.Assert(err.Error(), Equals, `type "myStruct" has no "bar" db tag`)
	_, err = argInfo.InputMember("myStruct", "bar")
	c.Assert(err.Error(), Equals, `type "myStruct" has no "bar" db tag`)
}

func (s *typeInfoSuite) TestGenerateArgInfoMapError(c *C) {
	type badMap map[int]any
	argInfo, err := GenerateArgInfo(badMap{})
	c.Assert(err, ErrorMatches, "map type badMap must have key type string, found type int")

	type myMap map[string]any
	argInfo, err = GenerateArgInfo(myMap{})
	c.Assert(err, IsNil)

	_, _, err = argInfo.AllOutputMembers("myMap")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "columns must be specified for map with star")
}

func (s *typeInfoSuite) TestLocateScanTargetMap(c *C) {
	type M map[string]any
	argInfo, err := GenerateArgInfo(M{})
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

	argInfo, err := GenerateArgInfo(T{})
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

	argInfo, err := GenerateArgInfo(M{})
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

	argInfo, err := GenerateArgInfo(T{})
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
