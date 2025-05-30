# How to contribute

Thanks for your interest in SQLair! Contributions like yours make good projects
great. This contributing guide covers code and documentation contributions,
including information about the project structure.

## Code of conduct

When contributing, you must abide by the [Ubuntu Code of
Conduct](https://ubuntu.com/community/ethos/code-of-conduct).

## Licence and copyright

By default, all contributions to SQLair are made under the Apache 2.0 licence.
See the [licence](LICENSE) in the SQLair GitHub repository for details.

The SQLair documentation uses a separate [CC-BY-SA 3.0
](https://creativecommons.org/licenses/by-sa/3.0/) license.

All contributors must sign the [Canonical contributor licence
agreement](https://ubuntu.com/legal/contributors), which grants Canonical
permission to use the contributions. The author of a change remains the
copyright owner of their code (no copyright assignment occurs).

## Documentation contributions

SQLair's documentation is stored in the (docs)[docs] directory of the
repository. It is based on the [Canonical starter
pack](https://canonical-starter-pack.readthedocs-hosted.com/latest/) and hosted
on [Read the Docs](https://about.readthedocs.com/).

For general guidance, refer to the [starter pack
guide](https://canonical-starter-pack.readthedocs-hosted.com/latest/readme/).

For syntax help and guidelines, refer to the [Canonical style
guides](https://canonical-documentation-with-sphinx-and-readthedocscom.readthedocs-hosted.com/#style-guides).

In structuring, the documentation employs the [Diátaxis](https://diataxis.fr/)
approach.

To run the documentation locally before submitting your changes:

```bash
cd docs
make run
```

## Code contributions

To make a contribution, the first step is figuring out where to make it.

The `sqlair` package contains the public interface of the library. However, this
only handles the surface level logic of SQLair and the statement cache.

The bulk of the SQLair code is located in the `internal/expr`, which handles the
parsing preparing and type mapping for the SQLair queries. See
[internal/expr/doc.go](internal/expr/doc.go) for an explanation of this pipeline.

As the name suggests, the `internal/typeinfo` package handles the type
information. It contains the code for extracting information from the types
passed to SQLair. For more info, see
[internal/typeinfo/doc.go](internal/typeinfo/doc.go).

### Environment setup

SQLair is written in Go version 1.24. Make sure you have this, or a higher
version installed.

### Building and testing

Build SQLair with `go build` and test SQLair with `go test ./...` from the
projects root.

SQLair uses [go check](https://pkg.go.dev/gopkg.in/check.v1?utm_source=godoc).
To run a subset of tests, pass the `-check.f` flag to `go test` with a regular
expression specifying the names of the test(s) you wish to run. For example:
```go
go test ./... -check.f TestExpr
```

### Code formatting

Code should generally be formatted according to the Google Go [style
guide](https://google.github.io/styleguide/go/) or in line with existing code.

Go provides a tool, `go fmt`, which facilitates a standardized format to go
source code. SQLair has one additional policy regarding imports:

#### Imports

Import statements are grouped into 3 sections: standard library, 3rd party
libraries, SQLair imports. The tool "go fmt" can be used to ensure each
group is alphabetically sorted. eg:

```go
import (
    "reflect"
    "strings"
    "sync"

    "github.com/pkg/errors"

    "github.com/canonical/sqlair/internal/parse"
)
```

### Conventional commits

Once you have written some code and have tested the changes, the next step is to
`git commit` it. For commit messages and pull request titles/descriptions SQLair
follows [conventional commits
guidelines](https://www.conventionalcommits.org/en/v1.0.0/). The commits should
be of the following form:
```
<type>(optional <scope>): <description>

[optional body]

[optional footer(s)]
```
- **Type:** The type describes the kind of change (e.g., feat, fix, docs, style,
  refactor, test, chore).
- **Scope:** The scope indicates the part of the codebase affected (e.g., model,
  api, cli).
- **Description:** The description briefly describes the change. It should not
  end in any punctuation.
- **Body:** The body should be a detailed explanation of the change. It should
  specify what has been changed and why and include descriptions of the
  behaviour before and after.
- **Footer:**  The footer includes information about breaking changes, issues
  closed, etc.

The type, scope and description should all begin with a lower case letter. None
of the lines can exceed 100 characters in length.

In the SQLair project, commits are squashed when they are merged. The PR
description is generally copied into the commit message. Make sure that it
includes everything that should be in there.

### Sanity checking PRs and unit tests

All GitHub PRs on the SQLair repository run pre-merge check unit test and
documentation checks. These checks are re-run anytime the PR changes, for
example when a new commit is added.

Passing these checks does not mean your code is ready to be merged, unit tests
should also be added to verify the changes.

### Code review

The SQLair project uses peer review of pull requests prior to merging to
facilitate improvements both in code quality and in design.

Once you have created your pull request, it will be reviewed. Make sure to
address the feedback. Your request might go through several rounds of feedback
before the patch is approved or rejected. Once you get an approval from a
member of the SQLair project, you are ready to have your patch merged.
Congratulations!