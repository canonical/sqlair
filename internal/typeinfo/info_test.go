package typeinfo

import (
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

var _ arg = &structInfo{}
var _ arg = &mapInfo{}

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
