package assemble_test

import (
	"fmt"
	"testing"

	"github.com/canonical/sqlair/internal/assemble"
	"github.com/canonical/sqlair/internal/parse"
	"github.com/stretchr/testify/assert"
)

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

func TestMismatchedInputStructName(t *testing.T) {
	sql := "select street from t where x = $Address.street"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Person{ID: 1})
	assert.Equal(t, fmt.Errorf("cannot assemble expression: unknown type: Address"), err)
}

func TestMissingTagInput(t *testing.T) {
	sql := "select street from t where x = $Address.number"
	parser := parse.NewParser()
	parsedExpr, err := parser.Parse(sql)
	_, err = assemble.Assemble(parsedExpr, Address{ID: 1})
	assert.Equal(t, fmt.Errorf("cannot assemble expression: there is no tag with name number in Address"), err)
}
