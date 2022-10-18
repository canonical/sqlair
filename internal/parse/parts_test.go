package parse_test

import (
	"testing"

	"github.com/canonical/sqlair/internal/parse"
	"github.com/stretchr/testify/assert"
)

func TestInputPart(t *testing.T) {
	i := parse.InputPart{
		parse.FullName{
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
	p := parse.OutputPart{
		[]parse.FullName{
			parse.FullName{
				Prefix: "mytable",
				Name:   "mycolumn",
			}},
		parse.FullName{
			Prefix: "mytype",
			Name:   "mytag",
		},
	}
	assert.NotEqual(t, nil, p)
	assert.Equal(t, "mytable", p.Source[0].Prefix)
	assert.Equal(t, "mycolumn", p.Source[0].Name)
	assert.Equal(t, "mytype", p.Target.Prefix)
	assert.Equal(t, "mytag", p.Target.Name)
}
