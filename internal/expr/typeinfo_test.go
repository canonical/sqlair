package expr

import (
	"reflect"
	"sync"

	. "gopkg.in/check.v1"
)

type ExprInternalSuite struct{}

var _ = Suite(&ExprInternalSuite{})

func (e *ExprInternalSuite) TestReflectStruct(c *C) {
	type something struct {
		ID      int64  `db:"id"`
		Name    string `db:"name,omitempty"`
		NotInDB string
	}

	s := something{
		ID:      99,
		Name:    "Chainheart Machine",
		NotInDB: "doesn't matter",
	}

	info, err := getTypeInfo(s)
	c.Assert(err, IsNil)

	switch info := info.(type) {
	case *structInfo:
		c.Assert(reflect.Struct, Equals, info.structType.Kind())
		c.Assert(reflect.TypeOf(s), Equals, info.structType)

		c.Assert(info.tagToField, HasLen, 2)

		id, ok := info.tagToField["id"]
		c.Assert(ok, Equals, true)
		c.Assert("ID", Equals, id.name)
		c.Assert(id.omitEmpty, Equals, false)

		name, ok := info.tagToField["name"]
		c.Assert(ok, Equals, true)
		c.Assert("Name", Equals, name.name)
		c.Assert(name.omitEmpty, Equals, true)
	}
}

func (s *ExprInternalSuite) TestReflectSimpleConcurrent(c *C) {
	type myStruct struct{}

	// Get the type info of a struct sequentially.
	var seqSt myStruct
	seqInfo, err := getTypeInfo(seqSt)
	c.Assert(err, IsNil)

	// Get some type info concurrently.
	var concSt myStruct
	wg := sync.WaitGroup{}

	// Set up some concurrent access.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			_, _ = getTypeInfo(concSt)
			wg.Done()
		}()
	}

	// Get type info alongside concurrent access.
	concInfo, err := getTypeInfo(concSt)
	c.Assert(err, IsNil)

	c.Assert(seqInfo, Equals, concInfo)

	wg.Wait()
}

func (s *ExprInternalSuite) TestReflectBadTagError(c *C) {

	var unsupportedFlag = []any{
		struct {
			ID int64 `db:"id,bad-juju"`
		}{99},
		struct {
			ID int64 `db:","`
		}{99},
		struct {
			ID int64 `db:"id,omitempty,ddd"`
		}{99},
	}

	var tagEmpty = []any{
		struct {
			ID int64 `db:",omitempty"`
		}{99},
	}

	var invalidColumn = []any{
		struct {
			ID int64 `db:"5id"`
		}{99},
		struct {
			ID int64 `db:"+id"`
		}{99},
		struct {
			ID int64 `db:"-id"`
		}{99},
		struct {
			ID int64 `db:"id/col"`
		}{99},
		struct {
			ID int64 `db:"id$$"`
		}{99},
		struct {
			ID int64 `db:"id|2005"`
		}{99},
		struct {
			ID int64 `db:"id|2005"`
		}{99},
	}

	for _, value := range unsupportedFlag {
		_, err := getTypeInfo(value)
		c.Assert(err, ErrorMatches, "cannot parse tag for field .ID: unsupported flag .*")
	}
	for _, value := range tagEmpty {
		_, err := getTypeInfo(value)
		c.Assert(err, ErrorMatches, "cannot parse tag for field .ID: .*")
	}
	for _, value := range invalidColumn {
		_, err := getTypeInfo(value)
		c.Assert(err, ErrorMatches, "cannot parse tag for field .ID: invalid column name in 'db' tag: .*")
	}
}

func (s *ExprInternalSuite) TestReflectValidTag(c *C) {
	var validTags = []any{
		struct {
			ID int64 `db:"id_"`
		}{99},
		struct {
			ID int64 `db:"id5"`
		}{99},
		struct {
			ID int64 `db:"_i_d_55"`
		}{99},
		struct {
			ID int64 `db:"id_2002"`
		}{99},
		struct {
			ID int64 `db:"IdENT99"`
		}{99},
	}

	for _, value := range validTags {
		_, err := getTypeInfo(value)
		c.Assert(err, IsNil)
	}
}

func (s *ExprInternalSuite) TestUnexportedField(c *C) {
	var unexportedFields = []any{
		struct {
			ID    int64 `db:"id"`
			unexp int64 `db:"unexp"`
		}{99, 100},
		struct {
			unexp int64 `db:"unexp"`
			ID    int64 `db:"id"`
		}{99, 100},
		struct {
			unexp int64 `db:"unexp"`
		}{100},
	}

	for _, value := range unexportedFields {
		_, err := getTypeInfo(value)
		c.Assert(err, ErrorMatches, `field "unexp" of struct  not exported`)
	}
}
