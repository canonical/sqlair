package parse_test

import (
	"testing"

	"github.com/canonical/sqlair/internal/parse"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type PartsSuite struct{}

var _ = Suite(&PartsSuite{})

func (s *PartsSuite) TestInputPart(c *C) {
	i := parse.InputPart{
		parse.FullName{
			Prefix: "mytype",
			Name:   "mytag",
		},
	}
	c.Assert("mytype", Equals, i.Source.Prefix)
	c.Assert("mytag", Equals, i.Source.Name)
}

func (s *PartsSuite) TestOutputPart(c *C) {
	// Fully specified part
	p := parse.OutputPart{
		[]parse.FullName{{"mytable", "mycolumn"}},
		[]parse.FullName{{"mytype", "mytag"}},
	}
	c.Assert("mytable", Equals, p.Source[0].Prefix)
	c.Assert("mycolumn", Equals, p.Source[0].Name)
	c.Assert("mytype", Equals, p.Target[0].Prefix)
	c.Assert("mytag", Equals, p.Target[0].Name)
}
