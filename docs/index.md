(SQLair documentation root file)=
# SQLair

```{note}
The project is still under development and there may be still be major changes
to the API.
```

SQLair is a Go package that extends SQL to allow mapping between Go types and
SQL databases by allowing references to the Go types within the SQL query
itself. 

SQLair allows you to:

- Map database rows directly into Go structs and maps
- Use Go structs and maps directly in the query parameters

Things SQLair does not do:

- Restrict the SQL that can be used
- Optimise queries

SQLair is designed to be a lightweight convenience layer that smooths the
encoding of database query results into Golang types. It is not an
Object-Relational Mapping tool (ORM).

TODO: Go developers will find this useful for interacting with databases.

```{toctree}
:maxdepth: 2
:hidden:

tutorial/tutorial
howto/index
reference/index
explanation/index
```

````{grid} 1 1 2 2 

```{grid-item-card} [Tutorial](tutorial/tutorial)

**Get started** with a hands-on introduction to SQLair

```

```{grid-item-card} [How-to guides](howto/index)

**Step-by-step guides** covering key operations and common tasks

```

````

````{grid} 1 1 2 2
:reverse:

```{grid-item-card} [Reference](reference/index)

**Technical information** about SQLair

```

```{grid-item-card} [Explanation](explanation/index)

**Discussion and clarification** of key topics

```

````

## Project and community

SQLair is developed by Canonical member of the Canonical family. It's an open
source project that warmly welcomes community projects, contributions,
suggestions, fixes and constructive feedback.

- [SQLair contributing guide](https://github.com/canonical/sqlair/blob/main/CONTRIBUTING.md>)

