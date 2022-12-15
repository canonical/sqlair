package assemble_test

import (
	"testing"

	"github.com/canonical/sqlair/internal/assemble"
	"github.com/canonical/sqlair/internal/parse"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ParserSuite struct{}

var _ = Suite(&ParserSuite{})

type Address struct {
	ID       int    `db:"id"`
	District string `db:"district"`
	Street   string `db:"street"`
}

type Person struct {
	ID         int    `db:"id"`
	Fullname   string `db:"name"`
	PostalCode int    `db:"address_id"`
}

type Manager struct {
	Name string `db:"manager_name"`
}

type District struct {
}

func (s *ParserSuite) TestValidAssemble(c *C) {
	testList := []struct {
		input             string
		assembleArgs      []any
		expectedAssembled string
	}{{
		"SELECT street FROM t WHERE x = $Address.street",
		[]any{Address{}},
		"SELECT street FROM t WHERE x = ?",
	}, {
		"SELECT p FROM t WHERE x = $Person.id",
		[]any{Person{}},
		"SELECT p FROM t WHERE x = ?",
	}}
	for _, test := range testList {
		parser := parse.NewParser()
		parsedExpr, _ := parser.Parse(test.input)
		assembledExpr, err := assemble.Assemble(parsedExpr, test.assembleArgs...)
		c.Log(err)
		c.Assert(assembledExpr.SQL, Equals, test.expectedAssembled)
	}
}

func (s *ParserSuite) TestMismatchedInputStructName(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.street"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Person{ID: 1})
	c.Assert(err, ErrorMatches, "cannot assemble expression: unknown type: Address")
}

func (s *ParserSuite) TestMissingTagInput(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.number"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Address{ID: 1})
	c.Assert(err, ErrorMatches, "cannot assemble expression: there is no tag with name number in Address")
}
