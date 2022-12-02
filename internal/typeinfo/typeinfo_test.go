package typeinfo

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReflectSimpleConcurrent(t *testing.T) {
	type mystruct struct{}
	var st mystruct
	wg := sync.WaitGroup{}

	// Set up some concurrent access.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			_, _ = TypeInfo(st)
			wg.Done()
		}()
	}

	info, err := TypeInfo(st)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Type.Kind())
	assert.Equal(t, reflect.TypeOf(st), info.Type)

	wg.Wait()
}

func TestReflectStruct(t *testing.T) {
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

	info, err := TypeInfo(s)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Type.Kind())
	assert.Equal(t, reflect.TypeOf(s), info.Type)

	assert.Len(t, info.TagToField, 2)

	id, ok := info.TagToField["id"]
	assert.True(t, ok)
	assert.Equal(t, "ID", id.Name)
	assert.False(t, id.OmitEmpty)

	name, ok := info.TagToField["name"]
	assert.True(t, ok)
	assert.Equal(t, "Name", name.Name)
	assert.True(t, name.OmitEmpty)
}

func TestReflectNonStructType(t *testing.T) {
	type tagErrorTest struct {
		value any
		err   error
	}

	type mymap map[int]int
	var tagErrorTable = []tagErrorTest{{
		value: mymap{},
		err:   fmt.Errorf(`internal error: attempted to obtain struct information for something that is not a struct: typeinfo.mymap.`),
	}, {
		value: int(0),
		err:   fmt.Errorf(`internal error: attempted to obtain struct information for something that is not a struct: int.`),
	}, {
		value: string(""),
		err:   fmt.Errorf(`internal error: attempted to obtain struct information for something that is not a struct: string.`),
	}, {
		value: map[string]string{},
		err:   fmt.Errorf(`internal error: attempted to obtain struct information for something that is not a struct: map[string]string.`),
	}}

	for _, test := range tagErrorTable {
		info, err := TypeInfo(test.value)
		assert.Equal(t, test.err, err)
		assert.Equal(t, (*Info)(nil), info)
	}
}

func TestReflectBadTagError(t *testing.T) {
	type tagErrorTest struct {
		value any
		err   error
	}

	var tagErrorTable = []tagErrorTest{{
		value: struct {
			ID int64 `db:"id,bad-juju"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: unsupported flag "bad-juju" in tag "id,bad-juju"`),
	}, {
		value: struct {
			ID int64 `db:","`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: unsupported flag "" in tag ","`),
	}, {
		value: struct {
			ID int64 `db:",omitempty"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: empty db tag`),
	}, {
		value: struct {
			ID int64 `db:"id,omitempty,ddd"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: unsupported flag "ddd" in tag "id,omitempty,ddd"`),
	}, {
		value: struct {
			ID int64 `db:"5id"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "5id"`),
	}, {
		value: struct {
			ID int64 `db:"+id"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "+id"`),
	}, {
		value: struct {
			ID int64 `db:"-id"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "-id"`),
	}, {
		value: struct {
			ID int64 `db:"id/col"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "id/col"`),
	}, {
		value: struct {
			ID int64 `db:"id$$"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "id$$"`),
	}, {
		value: struct {
			ID int64 `db:"id|2005"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "id|2005"`),
	}, {
		value: struct {
			ID int64 `db:"id|2005"`
		}{99},
		err: fmt.Errorf(`cannot parse tag for field .ID: invalid column name in 'db' tag: "id|2005"`),
	}, {
		value: struct {
			ID int64 `db:"id_"`
		}{99},
		err: nil,
	}, {
		value: struct {
			ID int64 `db:"id5"`
		}{99},
		err: nil,
	}, {
		value: struct {
			ID int64 `db:"_i_d_55"`
		}{99},
		err: nil,
	}, {
		value: struct {
			ID int64 `db:"id_2002"`
		}{99},
		err: nil,
	}, {
		value: struct {
			ID int64 `db:"IdENT99"`
		}{99},
		err: nil,
	}}

	for _, test := range tagErrorTable {
		_, err := TypeInfo(test.value)
		assert.Equal(t, test.err, err)
	}
}
