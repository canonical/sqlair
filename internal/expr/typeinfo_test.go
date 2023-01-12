package expr

import (
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestInternal(t *testing.T) { TestingT(t) }

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

	info, err := typeInfo(s)
	c.Assert(err, IsNil)

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
