package expr

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type PartsSuite struct{}

var _ = Suite(&PartsSuite{})

func (s *PartsSuite) TestInputPart(c *C) {
	i := inputPart{
		fullName{
			prefix: "mytype",
			name:   "mytag",
		},
	}
	c.Assert("mytype", Equals, i.source.prefix)
	c.Assert("mytag", Equals, i.source.name)
}

func (s *PartsSuite) TestOutputPart(c *C) {
	// Fully specified part
	p := outputPart{
		[]fullName{{"mytable", "mycolumn"}},
		[]fullName{{"mytype", "mytag"}},
	}
	c.Assert("mytable", Equals, p.source[0].prefix)
	c.Assert("mycolumn", Equals, p.source[0].name)
	c.Assert("mytype", Equals, p.target[0].prefix)
	c.Assert("mytag", Equals, p.target[0].name)
}
