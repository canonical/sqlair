// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

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

func (s *typeInfoSuite) TestArgInfoStruct(c *C) {
	type myStruct struct {
		ID        int    `db:"id"`
		Name      string `db:"name,omitempty"`
		ValidTag1 int    `db:"_i_d_55_"`
		ValidTag2 int    `db:"IdENT99"`
		NotInDB   string
	}

	argInfo, err := GenerateArgInfo([]any{myStruct{}})
	c.Assert(err, IsNil)

	// The struct fields in this list are ordered according to how sort.Strings
	// orders the tag names. This matches the order of the Output values from
	// AllStructOutputs.
	structFields := []struct {
		fieldName string
		index     []int
		omitEmpty bool
		tag       string
	}{{
		fieldName: "ValidTag2",
		index:     []int{3},
		omitEmpty: false,
		tag:       "IdENT99",
	}, {
		fieldName: "ValidTag1",
		index:     []int{2},
		omitEmpty: false,
		tag:       "_i_d_55_",
	}, {
		fieldName: "ID",
		index:     []int{0},
		omitEmpty: false,
		tag:       "id",
	}, {
		fieldName: "Name",
		index:     []int{1},
		omitEmpty: true,
		tag:       "name",
	}}

	allOutputs, names, err := argInfo.AllStructOutputs("myStruct")
	c.Assert(err, IsNil)
	c.Assert(allOutputs, HasLen, len(structFields))

	structType := reflect.TypeOf(myStruct{})
	for i, t := range structFields {
		expectedStructField := &structField{
			name:       t.fieldName,
			structType: structType,
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

func (s *typeInfoSuite) TestArgInfoMap(c *C) {
	type myMap map[string]any

	argInfo, err := GenerateArgInfo([]any{myMap{}})
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
type T struct{ foo int }

var t = T{}

func (s *typeInfoSuite) TestGenerateArgInfoInvalidTypeErrors(c *C) {
	type T struct{ foo int }
	type M map[string]any

	tests := []struct {
		args []any
		err  string
	}{{

		args: []any{nil},
		err:  "need supported value, got nil",
	}, {

		args: []any{struct{ foo int }{}},
		err:  "cannot use anonymous struct",
	}, {

		args: []any{map[string]any{}},
		err:  "cannot use anonymous map",
	}, {
		args: []any{T{}, T{}},
		err:  `found multiple instances of type "T"`,
	}, {

		args: []any{(*T)(nil)},
		err:  "need non-pointer type, got pointer to struct",
	}, {

		args: []any{(*M)(nil)},
		err:  "need non-pointer type, got pointer to map",
	}, {

		args: []any{""},
		err:  "need supported type, got string",
	}, {

		args: []any{0},
		err:  "need supported type, got int",
	}, {
		args: []any{[10]int{}},
		err:  "need supported type, got array",
	}, {
		args: []any{t, T{}},
		err:  `two types found with name "T": "typeinfo.T" and "typeinfo.T"`,
	}}

	for _, t := range tests {
		_, err := GenerateArgInfo(t.args)
		c.Assert(err, ErrorMatches, t.err)
	}
}

func (s *typeInfoSuite) TestGenerateArgInfoStructError(c *C) {
	type S1 struct {
		unexp int `db:"unexp"`
	}
	_, err := GenerateArgInfo([]any{S1{}})
	c.Assert(err, ErrorMatches, `field "unexp" of struct S1 not exported`)

	type S2 struct {
		Foo int `db:"id,bad-juju"`
	}
	_, err = GenerateArgInfo([]any{S2{}})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S2.Foo: unsupported flag "bad-juju" in tag "id,bad-juju"`)

	type S3 struct {
		Foo int `db:",omitempty"`
	}
	_, err = GenerateArgInfo([]any{S3{}})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S3.Foo: empty db tag`)

	type S4 struct {
		Foo int `db:"5id"`
	}
	_, err = GenerateArgInfo([]any{S4{}})
	c.Assert(err, ErrorMatches, `cannot parse tag for field S4.Foo: invalid column name in 'db' tag: "5id"`)

	type S5 struct {
		Foo int `db:"+id"`
	}
	_, err = GenerateArgInfo([]any{S5{}})
	c.Assert(err.Error(), Equals, `cannot parse tag for field S5.Foo: invalid column name in 'db' tag: "+id"`)

	type S6 struct {
		Foo int `db:"id$$"`
	}
	_, err = GenerateArgInfo([]any{S6{}})
	c.Assert(err.Error(), Equals, `cannot parse tag for field S6.Foo: invalid column name in 'db' tag: "id$$"`)

	type badMap map[int]any
	_, err = GenerateArgInfo([]any{badMap{}})
	c.Assert(err, ErrorMatches, "map type badMap must have key type string, found type int")

	_, err = GenerateArgInfo([]any{[]int{}})
	c.Assert(err, ErrorMatches, "cannot use anonymous slice")
}

func (*typeInfoSuite) TestInputAndOutputMemberError(c *C) {
	type mySlice []any
	type myMap map[string]any
	type myStruct struct {
		Foo int `db:"foo"`
	}
	argInfo, err := GenerateArgInfo([]any{mySlice{}, myMap{}, myStruct{}})
	c.Assert(err, IsNil)

	tests := []struct {
		typeName   string
		memberName string
		err        string
	}{{
		typeName:   "mySlice",
		memberName: "member1",
		err:        "cannot get named member of slice",
	}, {
		typeName:   "myStruct",
		memberName: "bar",
		err:        `type "myStruct" has no "bar" db tag`,
	}, {
		typeName:   "wrongStruct",
		memberName: "foo",
		err:        `parameter with type "wrongStruct" missing (have "myMap", "mySlice", "myStruct")`,
	}}

	for i, test := range tests {
		_, err = argInfo.InputMember(test.typeName, test.memberName)
		c.Assert(err, NotNil, Commentf("test %d failed", i+1))
		c.Assert(err.Error(), Equals, test.err)

		_, err = argInfo.OutputMember(test.typeName, test.memberName)
		c.Assert(err, NotNil, Commentf("test %d failed", i+1))
		c.Assert(err.Error(), Equals, test.err)
	}
}

func (*typeInfoSuite) TestAllOutputMemberError(c *C) {
	type mySlice []any
	type myMap map[string]any
	argInfo, err := GenerateArgInfo([]any{mySlice{}, myMap{}})
	c.Assert(err, IsNil)

	tests := []struct {
		typeName string
		err      string
	}{{
		typeName: "mySlice",
		err:      "cannot use slice with asterisk",
	}, {
		typeName: "myMap",
		err:      "cannot use map with asterisk unless columns are specified",
	}, {
		typeName: "wrongStruct",
		err:      `parameter with type "wrongStruct" missing (have "myMap", "mySlice")`,
	}}

	for i, test := range tests {
		_, _, err = argInfo.AllStructOutputs(test.typeName)
		c.Assert(err, NotNil, Commentf("test %d failed", i+1))
		c.Assert(err.Error(), Equals, test.err)
	}
}

func (*typeInfoSuite) TestSliceInputError(c *C) {
	type myMap map[string]any
	type myStruct struct {
		Foo int `db:"foo"`
	}
	argInfo, err := GenerateArgInfo([]any{myMap{}, myStruct{}})
	c.Assert(err, IsNil)

	tests := []struct {
		typeName string
		err      string
	}{{
		typeName: "myStruct",
		err:      "cannot use slice syntax with struct",
	}, {
		typeName: "myMap",
		err:      "cannot use slice syntax with map",
	}, {
		typeName: "wrongSlice",
		err:      `parameter with type "wrongSlice" missing (have "myMap", "myStruct")`,
	}}

	for i, test := range tests {
		_, err = argInfo.InputSlice(test.typeName)
		c.Assert(err, NotNil, Commentf("test %d failed", i+1))
		c.Assert(err.Error(), Equals, test.err)
	}
}
