package typeinfo

import (
	"reflect"

	. "gopkg.in/check.v1"
)

func (*typeInfoSuite) TestLocateParamsSlice(c *C) {
	type S []any

	argInfo, err := GenerateArgInfo([]any{S{}})
	c.Assert(err, IsNil)

	s := S{1, 2}
	valOfS := reflect.ValueOf(s)
	typeToValue := map[reflect.Type]reflect.Value{
		reflect.TypeOf(s): valOfS,
	}

	input, err := argInfo.InputSlice("S")
	c.Assert(err, IsNil)

	vals, err := input.LocateParams(typeToValue)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, len(s))

	for i := 0; i < len(s); i++ {
		c.Assert(vals[i].Interface(), Equals, s[i])
	}
}

func (*typeInfoSuite) TestLocateParamsSliceError(c *C) {
	type S []any

	argInfo, err := GenerateArgInfo([]any{S{}})
	c.Assert(err, IsNil)

	input, err := argInfo.InputSlice("S")
	c.Assert(err, IsNil)

	// Check missing type error.
	_, err = input.LocateParams(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "S" missing`)
}
