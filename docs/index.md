(SQLair documentation root file)=
# SQLair

```{note}
The project is still under development and there may be still be major changes
to the API.
```

SQLair is a Go package that maps between Go types and SQL queries.

SQLair provides an API for interacting with SQL databases from within Go
programs. It prepares and runs SQL queries containing special SQLair syntax.
The syntax that tells SQLair which values to use as query arguments and which
Go objects map the results into.

When interacting with a database in Go, the tables of the database are often
very similar to the structs in Go program. SQLair allow seamless mapping from
the tables to Go structs without the overhead of an Object-Relational Mapping
library (ORM). SQLair concerns itself only with the mapping from the Go objects
to the database tables. It allows you to run any SQL you wish.

The library is perfect for anyone who is looking for the convenience of
automatic mapping between database tables to Go types and who just wants to
write SQL instead of dealing with an ORM.

```{toctree}
:maxdepth: 2
:hidden:

tutorial/tutorial
howto/index
reference/index
```

````{grid} 1 1 2 2 

```{grid-item-card} [Tutorial](tutorial/tutorial)

**Get started** with a hands-on introduction to SQLair

```

```{grid-item-card} [How-to guides](howto/index)

**Step-by-step guides** covering key operations and common tasks

```

````

````{grid} 1 1 1 1
:reverse:

```{grid-item-card} [Reference](reference/index)

**Technical information** about SQLair

```

````

## Project and community

SQLair is an open source project that warmly welcomes community projects,
contributions, suggestions, fixes and constructive feedback. We welcome any
improvements to the documentation or SQLair itself. Please see our contributing
guide if you would like to get involved!

- [SQLair GitHub page](https://github.com/canonical/sqlair)
- [SQLair contributing guide](https://github.com/canonical/sqlair/blob/main/CONTRIBUTING.md)

The SQLair project is currently under the care of the Juju team at Canonical. If
you have any questions or would just like to say hi you can find us on the
Charmhub Discourse Forum. Just mention SQLair in the subject of your post, and we
will see it.

- [Charmhub Discourse Forum](https://discourse.charmhub.io/)

If you have any issues or bugs to raise, please see our GitHub issues page.

- [GitHub Issues](https://github.com/canonical/sqlair)
