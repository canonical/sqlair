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
			_, _ = GetTypeInfo(st)
			wg.Done()
		}()
	}

	info, err := GetTypeInfo(st)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Type.Kind())
	assert.Equal(t, "mystruct", info.Type.Name())

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

func TestReflectNonStructType(t *testing.T) {
	var i int
	var s string
	var mymap map[string]string
	var myM M

	{
		info, err := GetTypeInfo(i)
		assert.Equal(t, fmt.Errorf("cannot reflect type"), err)
		assert.Equal(t, &Info{}, info)
	}
	{
		info, err := GetTypeInfo(s)
		assert.Equal(t, fmt.Errorf("cannot reflect type"), err)
		assert.Equal(t, &Info{}, info)
	}
	{
		info, err := GetTypeInfo(mymap)
		assert.Equal(t, fmt.Errorf("cannot reflect type"), err)
		assert.Equal(t, info, &Info{})
	}
	{
		info, err := GetTypeInfo(myM)
		assert.Equal(t, fmt.Errorf("cannot reflect type"), err)
		assert.Equal(t, &Info{}, info)
	}
}

func TestReflectBadTagError(t *testing.T) {
	{
		type s1 struct {
			ID int64 `db:"id,bad-juju"`
		}
		ss := s1{ID: 99}
		_, err := GetTypeInfo(ss)
		assert.Error(t, fmt.Errorf(`unexpected tag value "bad-juju"`), err)
	}

	{
		type s2 struct {
			ID int64 `db:","`
		}
		ss2 := s2{ID: 99}
		_, err := GetTypeInfo(ss2)
		assert.Equal(t, fmt.Errorf(`unexpected tag value ""`), err)
	}
	{
		type s3 struct {
			ID int64 `db:",omitempty"`
		}
		ss3 := s3{ID: 99}
		_, err := GetTypeInfo(ss3)
		assert.Equal(t, fmt.Errorf(`empty db tag`), err)
	}
}
