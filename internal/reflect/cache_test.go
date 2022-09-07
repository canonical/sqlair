package reflect

import (
	"reflect"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReflectSimpleConcurrent(t *testing.T) {
	var num int64

	wg := sync.WaitGroup{}

	// Set up some concurrent access.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			_, _ = Cache().Reflect(num)
			wg.Done()
		}()
	}

	info, err := Cache().Reflect(num)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Int64, info.Kind())
	assert.Equal(t, "int64", info.Name())

	_, ok := info.(Value)
	assert.True(t, ok)

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

	info, err := Cache().Reflect(s)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Kind())
	assert.Equal(t, "something", info.Name())

	st, ok := info.(Struct)
	assert.True(t, ok)

	assert.Len(t, st.Fields, 2)

	id, ok := st.Fields["id"]
	assert.True(t, ok)
	assert.Equal(t, "ID", id.Name)
	assert.False(t, id.OmitEmpty)

	name, ok := st.Fields["name"]
	assert.True(t, ok)
	assert.Equal(t, "Name", name.Name)
	assert.True(t, name.OmitEmpty)
}

func TestReflectBadTagError(t *testing.T) {
	type something struct {
		ID int64 `db:"id,bad-juju"`
	}

	s := something{ID: 99}

	_, err := Cache().Reflect(s)
	assert.Error(t, errors.New(`unexpected tag value "bad-juju"`), err)
}
