(SQLair documentation root file)=
# SQLair

SQLair is a Go package and an extension to SQL that makes it easy to map between
Go types and a SQL database.

With SQLair, once you’ve SQLair-proofed your types and SQLair-wrapped your
database, you can jump straight to writing, parsing, and running queries against
the database.

When interacting with a SQL database in Go, mapping between the database tables
and the Go types is an issue. Convenience layers can be either too little (e.g.,
`database/sql`, where you can write any SQL you want, but you have to manually
iterate over each row) or too much (e.g., Object-Relational Mapping (ORM)
libraries where you get automatic mapping but lose the ability to write the
SQL). With SQLair you get just the right balance – automatic mapping plus the
ability to write any SQL you want.

If you’re looking to streamline your Go game, `go get` SQLair today.

```{note}
The project is pre-release and there may still be major changes to the API.
```

## In this documentation
```{toctree}
:maxdepth: 2
:hidden:

tutorial/tutorial
howto/index
reference/index
explanation/security
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

```{grid-item-card} [Reference](reference/index)

**Technical information** about SQLair

```

```{grid-item-card} [Security](explanation/security)

**Security** information about SQLair

```

````

## Project and community

SQLair is an open source project that warmly welcomes community projects,
contributions, suggestions, fixes and constructive feedback. We welcome any
improvements to the documentation or SQLair itself. Please see our contributing
guide if you would like to get involved!

- [SQLair GitHub page](https://github.com/canonical/sqlair)
- [SQLair contributing guide](https://github.com/canonical/sqlair/blob/main/CONTRIBUTING.md)
- [Ubuntu code of conduct](https://ubuntu.com/community/ethos/code-of-conduct)

The SQLair project is currently under the care of the Juju team at Canonical. If
you have any questions or would just like to say hi you can find us on the
Charmhub Discourse Forum. Just mention SQLair in the subject of your post, and we
will see it.

- [Charmhub Discourse Forum](https://discourse.charmhub.io/)

If you have any issues or bugs to raise, please see our GitHub issues page.

- [GitHub Issues](https://github.com/canonical/sqlair)
