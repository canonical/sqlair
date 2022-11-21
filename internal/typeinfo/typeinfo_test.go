package typeinfo

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReflectSimpleConcurrent(t *testing.T) {
	var num int64

	wg := sync.WaitGroup{}

	// Set up some concurrent access.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			_, _ = GetTypeInfo(num)
			wg.Done()
		}()
	}

	info, err := GetTypeInfo(num)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Int64, info.Type.Kind())
	assert.Equal(t, "int64", info.Type.Name())

	wg.Wait()
}

func TestReflectM(t *testing.T) {
	var mymap M
	mymap = make(M)
	mymap["foo"] = 7
	mymap["bar"] = "baz"

	info, err := GetTypeInfo(mymap)
	assert.Nil(t, err)
	assert.Equal(t, "M", info.Type.Name())
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

	info, err := GetTypeInfo(s)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Type.Kind())
	assert.Equal(t, "something", info.Type.Name())

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

func TestReflectSimpleTypes(t *testing.T) {
	var i int
	var s string
	var mymap map[string]string

	{
		info, err := GetTypeInfo(i)
		assert.Equal(t, info.Type.Name(), "int")
		assert.Len(t, info.TagToField, 0)
		assert.Len(t, info.FieldToTag, 0)
		assert.Nil(t, err)
	}
	{
		info, err := GetTypeInfo(s)
		assert.Equal(t, info.Type.Name(), "string")
		assert.Len(t, info.TagToField, 0)
		assert.Len(t, info.FieldToTag, 0)
		assert.Nil(t, err)
	}
	{
		info, err := GetTypeInfo(mymap)
		assert.Equal(t, info, Info{})
		assert.Equal(t, err, fmt.Errorf("Can't reflect map type"))
	}
}

func TestReflectBadTagError(t *testing.T) {
	type something struct {
		ID int64 `db:"id,bad-juju"`
	}

	s := something{ID: 99}

	_, err := GetTypeInfo(s)
	assert.Error(t, fmt.Errorf(`unexpected tag value "bad-juju"`), err)
}
