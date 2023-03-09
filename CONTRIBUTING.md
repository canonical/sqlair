Code formatting
===============

Go provides a tool, `go fmt`, which facilitates a standardised format to Go 
source code. SQLAir has two additional policies:

Imports
-------

Import statements are grouped into three sections: 
- standard library 
- third party libraries 
- SQLAir imports 

The tool "go fmt" can be used to ensure each group is alphabetically sorted, 
for example:

```go
import (
    "reflect"
    "strings"
    "sync"

    "github.com/pkg/errors"
	
	"github.com/canonical/sqlair/internal/parse"
)
```

Comments
--------

Prefer comments above the code to which they apply. Comments should also
include correct punctuation. This means that this:

```go
// Dereference the pointer if it is one.
value = reflect.Indirect(value)
```

is better than:

```go
value = reflect.Indirect(value) // dereference pointer
```

