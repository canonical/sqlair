package parse

import (
	"log"
	"testing"
)

type parseHelperTest struct {
	bytef    func(byte) bool
	stringf  func(string) bool
	stringf0 func() bool
	result   []bool
	input    string
	data     []string
}

var p = NewParser()

var parseTests = []parseHelperTest{
	{bytef: p.peekByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{false}, input: "b", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{true}, input: "a", data: []string{"a"}},

	{bytef: p.skipByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByte, result: []bool{false}, input: "abc", data: []string{"b"}},
	{bytef: p.skipByte, result: []bool{true, true}, input: "abc", data: []string{"a", "b"}},

	{bytef: p.skipByteFind, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByteFind, result: []bool{false, true, true}, input: "abcde", data: []string{"x", "b", "c"}},
	{bytef: p.skipByteFind, result: []bool{true, false}, input: "abcde ", data: []string{" ", " "}},

	{stringf0: p.skipSpaces, result: []bool{false}, input: "", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "abc    d", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "     abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "  \t  abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "\t  abcd", data: []string{}},

	{stringf: p.skipString, result: []bool{false}, input: "", data: []string{"a"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "helloworld", data: []string{"hElLo", "w"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "hello world", data: []string{"hello", " "}},
}

func TestRunTable(t *testing.T) {
	for _, v := range parseTests {
		p.Parse(v.input)
		for i, _ := range v.result {
			var result bool
			if v.bytef != nil {
				result = v.bytef(v.data[i][0])
			}
			if v.stringf != nil {
				result = v.stringf(v.data[i])
			}
			if v.stringf0 != nil {
				result = v.stringf0()
			}
			if v.result[i] != result {
				log.Printf("Test %#v failed. Expected: '%t', got '%t'\n", v, result, v.result[i])
			}
		}
	}
}

// func TestInit(t *testing.T) {
// 	p := NewParser()
// 	expr, err := p.Parse("select foo from bar")
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, (*ParsedExpr)(nil), expr)
// }
//
// func TestOutputParser(t *testing.T) {
// 	var expr *ParsedExpr
//
// 	p := NewParser()
//
// 	expr, err := p.Parse("Select &Person From")
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, "", expr.String()) // Finish this
// }

type Address struct {
	ID int `db:"id"`
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

type M map[string]any

func TestRound(t *testing.T) {
	var tests = []struct {
		input             string
		expectedParsed    string
		prepArgs          []any
		completeArgs      []any
		expectedCompleted string
	}{
		{
			"select p.* as &Person.*",
			"ParsedExpr[stringPart[select] outputPart[tableColumn[p.*] typeField[Person.*]]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* AS&Person.*",
			"ParsedExpr[stringPart[select] outputPart[tableColumn[p.*] typeField[Person.*]]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* as &Person.*, '&notAnOutputExpresion.*' as literal from t",
			"ParsedExpr[stringPart[select] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"stringPart[ '&notAnOutputExpresion.*'] " +
				"stringPart[ as literal from t]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.* ,  '&notAnOutputExpresion.*'  as literal from t",
		},
		{
			"select * as &Person.* from t",
			"ParsedExpr[stringPart[select] " +
				"outputPart[tableColumn[.*] typeField[Person.*]] " +
				"stringPart[ from t]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select *  from t",
		},
		{
			"select foo, bar from table where foo = $Person.ID",
			"ParsedExpr[stringPart[select foo, bar from table where foo =] " +
				"inputPart[Person.ID]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select foo, bar from table where foo = ?",
		},
		{
			"select &Person from table where foo = $Address.ID",
			"ParsedExpr[stringPart[select] outputPart[ typeField[Person.]] " +
				"stringPart[ from table where foo =] " +
				"inputPart[Address.ID]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"select address_id, id, name  from table where foo = ?",
		},
		{
			"select &Person.* from table where foo = $Address.ID",
			"ParsedExpr[stringPart[select] " +
				"outputPart[ typeField[Person.*]] " +
				"stringPart[ from table where foo =] " +
				"inputPart[Address.ID]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"select address_id, id, name  from table where foo = ?",
		},
		{
			"select foo, bar, &Person.ID from table where foo = 'xx'",
			"ParsedExpr[stringPart[select foo, bar,] " +
				"outputPart[ typeField[Person.ID]] " +
				"stringPart[ from table where foo =] " +
				"stringPart[ 'xx']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select foo, bar, id  from table where foo =  'xx'",
		},
		{
			"select foo, &Person.ID, bar, baz, &Manager.Name from table where foo = 'xx'",
			"ParsedExpr[stringPart[select foo,] " +
				"outputPart[ typeField[Person.ID]] " +
				"stringPart[, bar, baz,] " +
				"outputPart[ typeField[Manager.Name]] " +
				"stringPart[ from table where foo =] " +
				"stringPart[ 'xx']]",
			[]any{&Person{}, &Manager{}},
			[]any{&Person{}, &Manager{}},
			"select foo, id , bar, baz, manager_name  from table where foo =  'xx'",
		},
		{
			"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[.*] " +
				"typeField[Person.*]] " +
				"stringPart[ FROM person WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT *  FROM person WHERE name =  'Fred'",
		},
		{
			"SELECT &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[ typeField[Person.*]] " +
				"stringPart[ FROM person WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT address_id, id, name  FROM person WHERE name =  'Fred'",
		},
		{
			"SELECT * AS &Person.*, a.* as &Address.* FROM person, address a WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.*] typeField[Address.*]] " +
				"stringPart[ FROM person, address a WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT * , a.*  FROM person, address a WHERE name =  'Fred'",
		},
		{
			"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM address AS a WHERE p.name =] stringPart[ 'Fred']]",
			[]any{&Address{}},
			[]any{&Address{}},
			"SELECT a.district, a.street  FROM address AS a WHERE p.name =  'Fred'",
		},
		{
			"SELECT 1 FROM person WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT 1 FROM person WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{},
			[]any{},
			"SELECT 1 FROM person WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
				"(5+7), (col1 * col2) as calculated_value FROM person AS p " +
				"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[, (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT p.* , a.district, a.street , (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
				"WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT p.* , a.district, a.street  FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name in (select name from table where table.n = $Person.name)",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[)]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}, &Person{}},
			"SELECT p.* , a.district, a.street  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n = ? )",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in (select name from table " +
				"where table.n = $Person.name) UNION " +
				"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in " +
				"(select name from table where table.n = $Person.name)",
			"ParsedExpr[stringPart[SELECT] outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[) UNION SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[)]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}, &Person{}, &Person{}, &Address{}, &Person{}},
			"SELECT p.* , a.district, a.street  FROM person WHERE p.name in (select name from table where table.n = ? ) UNION SELECT p.* , a.district, a.street  FROM person WHERE p.name in (select name from table where table.n = ? )",
		},
		{
			"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
				"FROM person AS p JOIN person AS m " +
				"ON p.manager_id = m.id WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[m.*] typeField[Manager.*]] " +
				"stringPart[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Manager{}},
			[]any{&Person{}, &Manager{}},
			"SELECT p.* , m.*  FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name =  'Fred'",
		},
		//{
		//	"SELECT (person.*, address.district) AS &M.* " +
		//		"FROM person JOIN address ON person.address_id = address.id " +
		//		"WHERE person.name = 'Fred'",
		//	"ParsedExpr[stringPart[SELECT] " +
		//		"outputPart[tableColumn[person.*] tableColumn[address.district] typeField[M.*]] " +
		//		"stringPart[ FROM person JOIN address ON person.address_id = address.id WHERE person.name =] " +
		//		"stringPart[ 'Fred']]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		//{
		//	"SELECT p.*, a.district " +
		//		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		//		"WHERE p.name = $M.name",
		//	"ParsedExpr[stringPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
		//		"inputPart[M.name]]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		{
			"SELECT person.*, address.district FROM person JOIN address " +
				"ON person.address_id = address.id WHERE person.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name =] " +
				"stringPart[ 'Fred']]",
			[]any{},
			[]any{},
			"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name =  'Fred'",
		},
		{
			"SELECT p FROM person WHERE p.name = $Person.name",
			"ParsedExpr[stringPart[SELECT p FROM person WHERE p.name =] inputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p FROM person WHERE p.name = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.District] typeField[District.]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"inputPart[Person.name] " +
				"stringPart[ AND p.address_id =] " +
				"inputPart[Person.address_id]]",
			[]any{&Person{}, &District{}},
			[]any{&Person{}, &District{}, &Person{}, &Person{}},
			"SELECT p.* , a.District  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?  AND p.address_id = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p INNER JOIN address AS a " +
				"ON p.address_id = $Address.ID " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.District] typeField[District.]] " +
				"stringPart[ FROM person AS p INNER JOIN address AS a ON p.address_id =] " +
				"inputPart[Address.ID] " +
				"stringPart[ WHERE p.name =] " +
				"inputPart[Person.name] " +
				"stringPart[ AND p.address_id =] " +
				"inputPart[Person.address_id]]",
			[]any{&Address{}, &Person{}, &District{}},
			[]any{&Person{}, &District{}, &Address{}, &Person{}, &Person{}},
			"SELECT p.* , a.District  FROM person AS p INNER JOIN address AS a ON p.address_id = ?  WHERE p.name = ?  AND p.address_id = ?",
		},
		{
			"SELECT p.*, a.district " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.*",
			"ParsedExpr[stringPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"inputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?",
		},
		{
			"INSERT INTO person (name) VALUES $Person.name",
			"ParsedExpr[stringPart[INSERT INTO person (name) VALUES] " +
				"inputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person (name) VALUES ?",
		},
		{
			"INSERT INTO person VALUES $Person.*",
			"ParsedExpr[stringPart[INSERT INTO person VALUES] " +
				"inputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person VALUES ?",
		},
		{
			"UPDATE person SET person.address_id = $Address.ID " +
				"WHERE person.id = $Person.ID",
			"ParsedExpr[stringPart[UPDATE person SET person.address_id =] " +
				"inputPart[Address.ID] " +
				"stringPart[ WHERE person.id =] " +
				"inputPart[Person.ID]]",
			[]any{&Address{}, &Person{}},
			[]any{&Address{}, &Person{}},
			"UPDATE person SET person.address_id = ?  WHERE person.id = ?",
		},
	}

	parser := NewParser()
	for i, test := range tests {
		var parsedExpr *ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); parsedExpr.String() !=
			test.expectedParsed {
			t.Errorf("Test %d Failed (Parse): input: %s\nexpected: %s\nactual: %s\n, err: %s\n",
				i, test.input, test.expectedParsed, parsedExpr.String(), err)
		}
	}
}
