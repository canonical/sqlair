package assemble_test

import (
	"fmt"

	"github.com/canonical/sqlair/internal/assemble"
	"github.com/canonical/sqlair/internal/parse"
	"github.com/stretchr/testify/assert"
	. "gopkg.in/check.v1"
)

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
		assembledExpr, _ := assemble.Assemble(parsedExpr, test.assembleArgs)
		c.Assert(assembledExpr, Equals, test.expectedAssembled)
	}
}

func (s *ParserSuite) TestMismatchedInputStructName(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.street"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Person{ID: 1})
	assert.Equal(t, fmt.Errorf("cannot assemble expression: unknown type: Address"), err)
}

func (s *ParserSuite) TestMissingTagInput(c *C) {
	sql := "SELECT street FROM t WHERE x = $Address.number"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Address{ID: 1})
	assert.Equal(t, fmt.Errorf("cannot assemble expression: there is no tag with name number in Address"), err)
}
