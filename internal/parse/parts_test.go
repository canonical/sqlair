package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputPart(t *testing.T) {
	i := InputPart{
		FullName{
			Prefix: "mytype",
			Name:   "mytag",
		},
	}
	assert.NotEqual(t, nil, i)
	assert.Equal(t, "mytype", i.Source.Prefix)
	assert.Equal(t, "mytag", i.Source.Name)
}

func TestOutputPart(t *testing.T) {
	// Fully specified part
	p := OutputPart{FullName{
		Prefix: "mytable",
		Name:   "mycolumn",
	},
		FullName{
			Prefix: "mytype",
			Name:   "mytag",
		},
	}
	assert.NotEqual(t, nil, p)
	assert.Equal(t, "mytable", p.Source.Prefix)
	assert.Equal(t, "mycolumn", p.Source.Name)
	assert.Equal(t, "mytype", p.Target.Prefix)
	assert.Equal(t, "mytag", p.Target.Name)
}
