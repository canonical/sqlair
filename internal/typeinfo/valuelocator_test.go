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

	member, err := argInfo["M"].GetMember("foo")
	c.Assert(err, IsNil)
	output := member.(Output)

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
	member, err := argInfo["T"].GetMember("foo")
	c.Assert(err, IsNil)
	output := member.(Output)

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
	member, err = argInfo["T"].GetMember("bar")
	c.Assert(err, IsNil)
	output = member.(Output)

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

	member, err := argInfo["T"].GetMember("foo")
	c.Assert(err, IsNil)
	output := member.(Output)

	// Check missing type error.
	_, _, err = output.LocateScanTarget(map[reflect.Type]reflect.Value{})
	c.Assert(err, ErrorMatches, `parameter with type "T" missing`)

	member, err = argInfo["M"].GetMember("baz")
	c.Assert(err, IsNil)
	output = member.(Output)

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

type M map[string]any
type N map[string]any
type TS struct {
	Foo string `db:"foo"`
	Bar string `db:"bar, omitempty"`
}
type TT struct{}
type S []any
type Sint []int

func (s *typeInfoSuite) TestLocateParams(c *C) {
	tests := []struct {
		summary      string
		typeSample   any
		arg          any
		input        func(map[string]ArgInfo) (ValueLocator, error)
		expectedBulk bool
		expectedOmit bool
		expectedVals []any
	}{{
		summary:    "map",
		typeSample: M{},
		arg:        M{"foo": "bar"},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		expectedBulk: false,
		expectedOmit: false,
		expectedVals: []any{"bar"},
	}, {
		summary:    "struct",
		typeSample: TS{},
		arg:        TS{Foo: "foo"},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		expectedBulk: false,
		expectedOmit: false,
		expectedVals: []any{"foo"},
	}, {
		summary:    "struct omitempty",
		typeSample: TS{},
		arg:        TS{Foo: "foo", Bar: ""},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("bar")
		},
		expectedBulk: false,
		expectedOmit: true,
		expectedVals: []any{""},
	}, {
		summary:    "int slice",
		typeSample: Sint{},
		arg:        Sint{1, 2},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["Sint"].GetSlice()
		},
		expectedOmit: false,
		expectedBulk: false,
		expectedVals: []any{1, 2},
	}, {
		summary:    "any slice",
		typeSample: S{},
		arg:        S{1, "two", 3.0},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["S"].GetSlice()
		},
		expectedOmit: false,
		expectedBulk: false,
		expectedVals: []any{1, "two", 3.0},
	}, {
		summary:    "empty slice",
		typeSample: S{},
		arg:        S{},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["S"].GetSlice()
		},
		expectedOmit: false,
		expectedBulk: false,
		expectedVals: []any{},
	}, {
		summary:    "map bulk insert",
		typeSample: M{},
		arg:        []M{{"foo": "foo"}, {"foo": "bar"}, {"foo": "baz"}},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		expectedBulk: true,
		expectedOmit: false,
		expectedVals: []any{"foo", "bar", "baz"},
	}, {
		summary:    "map pointer bulk insert",
		typeSample: M{},
		arg:        []*M{{"foo": "foo"}, {"foo": "bar"}, {"foo": "baz"}},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		expectedBulk: true,
		expectedOmit: false,
		expectedVals: []any{"foo", "bar", "baz"},
	}, {
		summary:    "struct bulk insert",
		typeSample: TS{},
		arg:        []TS{{Foo: "foo1"}, {Foo: "foo2"}, {Foo: "foo3"}},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		expectedBulk: true,
		expectedOmit: false,
		expectedVals: []any{"foo1", "foo2", "foo3"},
	}, {
		summary:    "struct pointer bulk insert",
		typeSample: TS{},
		arg:        []*TS{{Foo: "foo1"}, {Foo: "foo2"}, {Foo: "foo3"}},
		input: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		expectedBulk: true,
		expectedOmit: false,
		expectedVals: []any{"foo1", "foo2", "foo3"},
	}}
	for _, t := range tests {
		c.Logf("starting test: %s", t.summary)
		argInfo, err := GenerateArgInfo([]any{t.typeSample})
		c.Assert(err, IsNil)

		typeToValue := map[reflect.Type]reflect.Value{
			reflect.TypeOf(t.arg): reflect.ValueOf(t.arg),
		}
		vl, err := t.input(argInfo)
		c.Assert(err, IsNil)
		input := vl.(Input)

		params, err := input.LocateParams(typeToValue)
		c.Assert(err, IsNil)
		c.Check(params.Omit, Equals, t.expectedOmit)
		c.Check(params.Bulk, Equals, t.expectedBulk)
		c.Assert(params.Vals, HasLen, len(t.expectedVals))
		for i, v := range t.expectedVals {
			c.Check(params.Vals[i], Equals, v)
		}
	}
}

func (s *typeInfoSuite) TestLocateParamsError(c *C) {
	tests := []struct {
		summary    string
		typeSample any
		arg        any
		vl         func(map[string]ArgInfo) (ValueLocator, error)
		err        string
	}{{
		summary:    "invalid map key",
		typeSample: M{},
		arg:        M{"foo": "bar"},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("baz")
		},
		err: `map "M" does not contain key "baz"`,
	}, {
		summary:    "missing map type",
		typeSample: M{},
		arg:        N{"foo": "bar"},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("baz")
		},
		err: `parameter with type "M" missing (have "N")`,
	}, {
		summary:    "missing struct type",
		typeSample: TS{},
		arg:        TT{},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		err: `parameter with type "TS" missing (have "TT")`,
	}, {
		summary:    "slice not found",
		typeSample: Sint{},
		arg:        S{},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["Sint"].GetSlice()
		},
		err: `parameter with type "Sint" missing (have "S")`,
	}, {
		summary:    "map bulk insert invalid key",
		typeSample: M{},
		arg:        []M{{"foo": "bar"}},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("baz")
		},
		err: `map "M" does not contain key "baz"`,
	}, {
		summary:    "map bulk insert nil map in slice",
		typeSample: M{},
		arg:        []M{{"foo": "bar"}, nil},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		err: `got nil map in slice of "M" at index 1`,
	}, {
		summary:    "map bulk insert nil pointer to map in slice",
		typeSample: M{},
		arg:        []*M{{"foo": "bar"}, nil},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		err: `got nil pointer in slice of "M" at index 1`,
	}, {
		summary:    "map bulk insert empty slice",
		typeSample: M{},
		arg:        []M{},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		err: `got slice of "M" with length 0`,
	}, {
		summary:    "map bulk insert nil slice",
		typeSample: M{},
		arg:        ([]M)(nil),
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["M"].GetMember("foo")
		},
		err: `got slice of "M" with length 0`,
	}, {
		summary:    "struct bulk nil pointer in slice",
		typeSample: TS{},
		arg:        []*TS{nil},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		err: `got nil pointer in slice of "TS" at index 0`,
	}, {
		summary:    "struct bulk insert empty slice",
		typeSample: TS{},
		arg:        []TS{},
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		err: `got slice of "TS" with length 0`,
	}, {
		summary:    "struct bulk insert nil slice",
		typeSample: TS{},
		arg:        ([]TS)(nil),
		vl: func(ai map[string]ArgInfo) (ValueLocator, error) {
			return ai["TS"].GetMember("foo")
		},
		err: `got slice of "TS" with length 0`,
	}}

	for _, t := range tests {
		c.Logf("starting test: %s", t.summary)
		argInfo, err := GenerateArgInfo([]any{t.typeSample})
		c.Assert(err, IsNil)

		typeToValue := map[reflect.Type]reflect.Value{
			reflect.TypeOf(t.arg): reflect.ValueOf(t.arg),
		}
		vl, err := t.vl(argInfo)
		c.Assert(err, IsNil)
		input := vl.(Input)

		_, err = input.LocateParams(typeToValue)
		c.Assert(err, NotNil)
		c.Check(err.Error(), Equals, t.err)
	}
}
